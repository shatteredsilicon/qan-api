/*
   Copyright (c) 2016, Percona LLC and/or its affiliates. All rights reserved.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>
*/

package app

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/revel/revel"
	"github.com/shatteredsilicon/qan-api/app/agent"
	"github.com/shatteredsilicon/qan-api/app/auth"
	"github.com/shatteredsilicon/qan-api/app/controllers"
	agentCtrl "github.com/shatteredsilicon/qan-api/app/controllers/agent"
	"github.com/shatteredsilicon/qan-api/app/db"
	"github.com/shatteredsilicon/qan-api/app/instance"
	"github.com/shatteredsilicon/qan-api/app/query"
	"github.com/shatteredsilicon/qan-api/app/shared"
	"github.com/shatteredsilicon/qan-api/config"
	queryService "github.com/shatteredsilicon/qan-api/service/query"
	"github.com/shatteredsilicon/qan-api/stats"
	"github.com/shatteredsilicon/ssm/proto"
	"golang.org/x/net/websocket"

	// dummy import to keep the package
	_ "github.com/revel/modules/testrunner/app/controllers"
)

// AppVersion is set from linker flags set by revel/cmd:
//
//	https://github.com/revel/cmd/blob/97ec1422620f1a83cd13738708a109fe092364ca/harness/build.go#L132
//
// The value for this variable is taken from os environment variable `APP_VERSION`:
//
//	https://github.com/revel/cmd/blob/97ec1422620f1a83cd13738708a109fe092364ca/harness/build.go#L212
//
// The value of `APP_VERSION` environment variable is set during package build:
//
//	https://github.com/shatteredsilicon/ssm-server-packaging/blob/611b8a2e372c9a399cedf2371157e78c3efa7bc8/rhel/SPECS/percona-qan-api.spec#L50
//
// The variable is used to create `X-Percona-QAN-API-Version` header, and it's important for pmm-client:
//
//	https://github.com/shatteredsilicon/ssm-client/blob/9420a44412f82d2e05f1e263492ca9a23345a55f/pmm/check_network.go#L43
//
// Do not set this var.
var AppVersion = ""

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// package vitess.io/vitess/go/vt/sqlparser imports package github.com/golang/glog,
	// and glog changes the output dest of the standard package 'log', we don't want that
	// so change it back.
	log.SetOutput(os.Stdout)

	// CLOUD_API_HOSTNAME is only used for testing and should override api.hostname.
	hostname := os.Getenv("CLOUD_API_HOSTNAME")
	if hostname == "" {
		hostname = config.Get("api.hostname")
		if hostname == "" {
			panic("Set CLOUD_API_HOSTNAME env var or api.hostname in the config file")
		}
	}

	// Use real stats clients only in stage and prod.
	statsEnv := config.Get("stats.env")
	if statsEnv == "stage" || statsEnv == "prod" {
		for _, service := range []string{"", "route"} {
			statsClient, err := statsd.NewBufferedClient(
				config.Get("statsd.server"),
				"",                   // prefix -- DO NOT SET HERE
				250*time.Millisecond, // buffer time
				8932,                 // MTU for gigabit ethernet
			)
			if err != nil {
				panic(fmt.Sprintf("ERROR: statsd.NewBufferedClient: %s", err))
			}
			s := stats.NewStats(
				statsClient,
				statsEnv,
				config.Get("api.alias"),
				service,
				config.Get("stats.rate"),
			)
			switch service {
			case "": // internal: agent, api
				shared.InternalStats = s
			case "route":
				shared.RouteStats = s
			}
		}
	}

	shared.AgentDirectory = agent.NewLocalDirectory()
	go func() {
		t := time.NewTicker(1 * time.Minute)
		for _ = range t.C {
			shared.AgentDirectory.Refresh(20 * time.Second)
		}
	}()

	// Run the query abstracter, used to consume QAN data.
	shared.QueryAbstracter = queryService.NewMini(config.ApiRootDir + "/service/query") // tables + abstract using Perl
	go shared.QueryAbstracter.Run()

	shared.TableParser = queryService.NewMini("") // only tables
	go shared.TableParser.Run()

	shared.InstanceTasks = instance.NewTasker(10)
	go shared.InstanceTasks.Run()

	revel.Filters = []revel.Filter{
		revel.PanicFilter,             // Recover from panics and display an error page instead.
		revel.RouterFilter,            // Use the routing table to select the right Action
		revel.FilterConfiguringFilter, // A hook for adding or removing per-Action filters.
		revel.ParamsFilter,            // Parse parameters into Controller.Params.
		revel.ValidationFilter,        // Restore kept validation errors and save new ones from cookie.
		revel.InterceptorFilter,       // Run interceptors around the action.
		revel.ActionInvoker,           // Invoke the action.
	}

	// Tasks to be run at the begin and end of every request
	revel.InterceptFunc(beforeController, revel.BEFORE, revel.AllControllers)
	revel.InterceptFunc(afterController, revel.FINALLY, revel.AllControllers)

	// All access to agent resources (/agents/:uuid/*) must specify a valid agent.
	revel.InterceptFunc(authAgent, revel.BEFORE, &agentCtrl.Agent{})

	revel.InterceptFunc(getInstanceId, revel.BEFORE, &controllers.QAN{})
	revel.InterceptFunc(getQueryId, revel.BEFORE, &controllers.Query{})
}

// Copied from github.com/cactus/go-statsd-client/statsd/main.go
func includeStat(rate float32) bool {
	if rate < 1 {
		if rand.Float32() < rate {
			return true
		}
		return false
	}
	return true
}

func beforeController(c *revel.Controller) revel.Result {
	if c.Action == "Home.Options" {
		return nil
	}

	if includeStat(shared.RouteStats.SampleRate) {
		c.Args["t"] = time.Now()
	}

	if c.Action == "Home.Ping" {
		c.Response.Out.Header().Set("X-Percona-QAN-API-Version", AppVersion)
	}

	// Create a MySQL db manager for the controller because most need it, but
	// don't open the connection yet, let the controller do that when it's
	// ready because it might return early (e.g. on invalid input).
	// The controller doesn't need to close it; we do that in afterController.
	c.Args["dbm"] = db.NewMySQLManager()

	// Args for various controllers/routes.
	apiBasePath := os.Getenv("BASE_PATH")
	if apiBasePath == "" {
		apiBasePath = config.Get("api.base.path")
	}
	schema := "http"
	if strings.Contains(strings.ToLower(c.Request.Request.Proto), "https") {
		schema = "https"
	}
	c.Args["wsBase"] = "ws://" + c.Request.Request.Host + apiBasePath
	c.Args["httpBase"] = schema + "://" + c.Request.Request.Host + apiBasePath

	agentVersion := c.Request.Header.Get("X-Percona-QAN-Agent-Version")
	if agentVersion == "" {
		agentVersion = "0.0.9"
	}
	c.Args["agentVersion"] = agentVersion

	// Set common headers before Revel sets the response code and writes
	// the response body. This avoids "multiple calls to WriterHeader" errors.
	c.Response.Out.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")
	c.Response.Out.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
	c.Response.Out.Header().Set("Access-Control-Allow-Origin", "*")

	return nil
}

func afterController(c *revel.Controller) revel.Result {
	if c.Action == "Home.Options" {
		return nil
	}

	dbm := c.Args["dbm"].(db.Manager)
	if err := dbm.Close(); err != nil {
		revel.ERROR.Println(err)
	}

	if c.Args["t"] != nil {
		t := c.Args["t"].(time.Time)
		d := time.Now().Sub(t)
		shared.RouteStats.TimingDuration( // response time
			shared.RouteStats.Metric(c.Action+".t"),
			d,
			1, // sampling handled in beforeController()
		)
		shared.RouteStats.Inc( // call rate
			shared.RouteStats.Metric(c.Action+".call"),
			1,
			1, // sampling handled in beforeController()
		)
	}
	return nil
}

func authAgent(c *revel.Controller) revel.Result {
	// We don't need a valid agent UUID for these routes.
	if c.Action == "Agent.Create" || c.Action == "Agent.List" || c.Action == "Home.Options" {
		return nil
	}

	var agentUuid string
	c.Params.Bind(&agentUuid, "uuid")

	dbm := c.Args["dbm"].(db.Manager)
	dbh := auth.NewMySQLHandler(dbm)
	authHandler := auth.NewAuthDb(dbh)

	agentId, res, err := authHandler.Agent(agentUuid)
	if err != nil {
		switch err {
		case shared.ErrNotFound:
			revel.INFO.Printf("auth agent: not found: %s", agentUuid)
		default:
			revel.ERROR.Printf("auth agent: %s", err)
		}
		c.Response.Status = int(res.Code)
		if c.Request.Method == "WS" {
			if err := websocket.JSON.Send(c.Request.Websocket, proto.Response{
				Code:  res.Code,
				Error: res.Error,
			}); err != nil {
				return c.RenderError(err)
			}
		} else {
			return c.RenderText(res.Error)
		}
	}
	c.Args["agentId"] = agentId

	return nil // success
}

func getInstanceId(c *revel.Controller) revel.Result {
	// Get the internal (auto-inc) instance ID(s) of the UUID.
	var uuid string
	var uuids []string
	c.Params.Bind(&uuid, "uuid")
	c.Params.Bind(&uuids, "uuids")
	if uuid != "" {
		uuids = []string{uuid}
	}
	if len(uuids) == 0 {
		c.Response.Status = http.StatusBadRequest
		return c.RenderText("")
	}

	dbm := c.Args["dbm"].(db.Manager)
	if err := dbm.Open(); err != nil {
		return internalError(c, "init.getInstanceId: dbm.Open", err)
	}

	instanceIds, err := instance.GetInstanceIds(dbm.DB(), uuids)
	if err != nil {
		return internalError(c, "init.getInstanceId: ih.GetInstanceIds", err)
	}
	if len(instanceIds) == 0 {
		c.Response.Status = http.StatusNotFound
		return c.RenderText("")
	}

	c.Args["instanceIds"] = instanceIds
	if uuid != "" {
		c.Args["instanceId"] = instanceIds[0]
	}

	return nil // success
}

func getQueryId(c *revel.Controller) revel.Result {
	// Get the internal (auto-inc) query ID.
	var queryId string
	c.Params.Bind(&queryId, "id")

	dbm := c.Args["dbm"].(db.Manager)
	if err := dbm.Open(); err != nil {
		return internalError(c, "init.getQueryId: dbm.Open", err)
	}

	// 92F3B1B361FB0E5B -> 5
	classId, err := query.GetClassId(dbm.DB(), queryId)
	if err != nil {
		switch err {
		case shared.ErrNotFound:
			c.Response.Status = http.StatusNotFound
			return c.RenderText("")
		default:
			return internalError(c, "init.getQueryId: query.GetClassId", err)
		}
	}
	c.Args["classId"] = classId

	return nil // success
}

func internalError(c *revel.Controller, op string, err error) revel.Result {
	errMsg := fmt.Sprintf("%s: %s", op, err)
	revel.ERROR.Printf(errMsg)
	res := proto.Error{
		Error: errMsg,
	}
	c.Response.Status = http.StatusInternalServerError
	return c.RenderJSON(res)
}
