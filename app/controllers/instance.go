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

package controllers

import (
	"encoding/json"
	"io"
	"os"
	"strings"

	uuid "github.com/nu7hatch/gouuid"
	"github.com/revel/revel"
	"github.com/shatteredsilicon/qan-api/app/db"
	"github.com/shatteredsilicon/qan-api/app/instance"
	"github.com/shatteredsilicon/qan-api/app/shared"
	"github.com/shatteredsilicon/ssm/proto"
)

type Instance struct {
	BackEnd
}

type respInstance struct {
	*proto.Instance
	Disconnected bool
}

func generateRespInstance(ssmUUID string, inst *proto.Instance) respInstance {
	return respInstance{
		Instance:     inst,
		Disconnected: os.Getenv("DISCONNECTED") != "0" && inst.ParentUUID != ssmUUID && inst.UUID != ssmUUID,
	}
}

// GET /instances
func (c *Instance) List() revel.Result {
	dbm := c.Args["dbm"].(db.Manager)
	if err := dbm.Open(); err != nil {
		return c.Error(err, "Instance.List: dbm.Open")
	}
	instanceHandler := instance.NewMySQLHandler(dbm)

	ssmUUID, err := instanceHandler.GetSSMServerOSUUID()
	if err != nil {
		return c.Error(err, "Instance.List: GetSSMServerOSUUID")
	}

	var instanceType, instanceName, parentUUID string
	c.Params.Bind(&instanceType, "type")
	c.Params.Bind(&instanceName, "name")
	c.Params.Bind(&parentUUID, "parent_uuid")
	if instanceType != "" && instanceName != "" {
		_, in, err := instanceHandler.GetByName(instanceType, instanceName, parentUUID)
		if err != nil {
			return c.Error(err, "Instance.List: ih.GetByName")
		}
		if in == nil {
			return c.Error(shared.ErrNotFound, "Instance.List: ih.GetByName")
		}
		return c.RenderJSON(generateRespInstance(ssmUUID, in))
	} else {
		instances, err := instanceHandler.GetAll(true)
		if err != nil {
			return c.Error(err, "Instance.List: ih.GetAll")
		}

		respInstances := make([]respInstance, len(instances))
		for i, instance := range instances {
			respInstances[i] = generateRespInstance(ssmUUID, &instance)
		}
		return c.RenderJSON(respInstances)
	}
}

// POST /instances
func (c *Instance) Create() revel.Result {
	var body []byte
	if len(c.Params.JSON) > 0 {
		body = c.Params.JSON
	} else {
		body, _ = io.ReadAll(c.Request.GetBody())
	}

	if len(body) == 0 {
		return c.BadRequest(nil, "empty body (no data posted)")
	}

	in := proto.Instance{}
	err := json.Unmarshal(body, &in)
	if err != nil {
		return c.BadRequest(err, "cannot decode proto.Instance")
	}

	if in.UUID == "" {
		u4, _ := uuid.NewV4()
		in.UUID = strings.Replace(u4.String(), "-", "", -1)
	}

	dbm := c.Args["dbm"].(db.Manager)
	if err := dbm.Open(); err != nil {
		return c.Error(err, "Instance.Create: dbm.Open")
	}
	ih := instance.NewMySQLHandler(dbm)
	if _, err := ih.Create(in); err != nil {
		if err == shared.ErrDuplicateEntry {
			id, _ := instance.GetInstanceId(dbm.DB(), in.UUID)
			if id == 0 {
				_, in2, err := ih.GetByName(in.Subsystem, in.Name, "")
				if err != nil {
					return c.Error(err, "Instance.Create: ih.GetByName")
				}
				in = *in2
			}
			uri := c.Args["httpBase"].(string) + "/instances/" + in.UUID
			c.Response.Out.Header().Set("Location", uri)
		}
		return c.Error(err, "Instance.Create: ih.Create")
	}

	return c.RenderCreated(c.Args["httpBase"].(string) + "/instances/" + in.UUID)
}

// GET /instances/:uuid
func (c *Instance) Get(uuid string) revel.Result {
	dbm := c.Args["dbm"].(db.Manager)
	if err := dbm.Open(); err != nil {
		return c.Error(err, "Instance.Get: dbm.Open")
	}
	instanceHandler := instance.NewMySQLHandler(dbm)

	ssmUUID, err := instanceHandler.GetSSMServerOSUUID()
	if err != nil {
		return c.Error(err, "Instance.Get: GetSSMServerOSUUID")
	}

	_, instance, err := instanceHandler.Get(uuid)
	if err != nil {
		return c.Error(err, "Instance.Get: ih.Get")
	}
	return c.RenderJSON(generateRespInstance(ssmUUID, instance))
}

// PUT /instances/:uuid
func (c *Instance) Update(uuid string) revel.Result {
	var body []byte
	if len(c.Params.JSON) > 0 {
		body = c.Params.JSON
	} else {
		body, _ = io.ReadAll(c.Request.GetBody())
	}

	if len(body) == 0 {
		return c.BadRequest(nil, "empty body (no data posted)")
	}

	in := proto.Instance{}
	err := json.Unmarshal(body, &in)
	if err != nil {
		return c.BadRequest(err, "cannot decode proto.Instance")
	}

	// I don't want to use a different proto.Instance not having the uuid
	// to avoid having a million of different structs, so, the body can have
	// an uuid but I'm going to rewrite it with the value from the route.
	dbm := c.Args["dbm"].(db.Manager)
	if err := dbm.Open(); err != nil {
		return c.Error(err, "Instance.Update: dbm.Open")
	}
	in.UUID = uuid
	ih := instance.NewMySQLHandler(dbm)
	if err := ih.Update(in); err != nil {
		return c.Error(err, "Instance.Update: ih.Update")
	}

	uri := c.Args["httpBase"].(string) + "/instances/" + in.UUID
	c.Response.Out.Header().Set("Location", uri)

	return c.RenderNoContent()
}

// DELETE /instances/:uuid
func (c *Instance) Delete(uuid string) revel.Result {
	dbm := c.Args["dbm"].(db.Manager)

	if err := dbm.Open(); err != nil {
		return c.Error(err, "Instance.Delete: dbm.Open")
	}

	ih := instance.NewMySQLHandler(dbm)

	_, inst, err := ih.Get(uuid)
	if err != nil {
		return c.Error(err, "Instance.Delete: ih.Get")
	}

	if err := ih.Delete(uuid); err != nil {
		return c.Error(err, "Instance.Delete: ih.Delete")
	}

	if inst.Name != instance.SSMServerName { // Don't remove qan data of internal instance
		shared.InstanceTasks.Add(shared.TypeInstanceTaskDelete, uuid)
	}

	return c.RenderNoContent()
}
