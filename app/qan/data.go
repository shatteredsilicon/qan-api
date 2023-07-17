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

package qan

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/shatteredsilicon/qan-api/app/instance"
	"github.com/shatteredsilicon/qan-api/app/shared"
	"github.com/shatteredsilicon/qan-api/app/ws"
	"github.com/shatteredsilicon/qan-api/stats"
	"github.com/shatteredsilicon/ssm/proto"
	qp "github.com/shatteredsilicon/ssm/proto/qan"
)

const (
	MAX_DATA_MSG  = 100
	THROTTLE_CODE = 299
)

func SaveData(wsConn ws.Connector, agentId uint, dbh *MySQLMetricWriter, stats *stats.Stats) error {
	prefix := fmt.Sprintf("[qan.SaveData] agent_id=%d", agentId)

	// get all existing instances
	instances, err := dbh.ih.GetAll()
	if err != nil {
		return err
	}

	existMap := make(map[string]struct{})
	for i := range instances {
		if instances[i].Subsystem != instance.SubsystemNameMySQL && instances[i].Subsystem != instance.SubsystemNameMongo {
			continue
		}
		existMap[instances[i].UUID] = struct{}{}
	}

	nMsgs := 0
	for {
		// Agent send proto.Data as []byte.
		bytes, err := wsConn.RecvBytes(20)
		if err != nil {
			if err == io.EOF {
				// Agent done sending, closed websocket. Data controller ignores this
				// error so don't change it with fmt.Errorf().
				return err
			}
			return fmt.Errorf("wsConn.RecvBytes: %s", err)
		}
		nBytes := int64(len(bytes))

		// Decode bytes back to proto.Data so we can determine which
		// type of data this is. proto.Data is backwards compatible with proto.Data

		tDecode := time.Now()
		data, report, err := decode(bytes)
		stats.TimingDuration(stats.System("decode"), time.Now().Sub(tDecode), stats.SampleRate)
		if err != nil {
			stats.SetComponent("bad-data.msg")
			stats.Inc(stats.System("bytes"), nBytes, stats.SampleRate)
			stats.Inc(stats.System("in"), 1, stats.SampleRate)

			// Agent removes file from its spool on code >= 400.
			resp := proto.Response{
				Code:  400,
				Error: err.Error(),
			}
			if err := wsConn.Send(resp, 5); err != nil {
				return fmt.Errorf("ww.Send: %s", err)
			}
			continue // next report
		}

		stats.SetComponent(data.Service + ".msg")
		stats.Inc(stats.System("bytes"), nBytes, stats.SampleRate)
		stats.Inc(stats.System("in"), 1, stats.SampleRate)

		if len(data.Data) > proto.MAX_DATA_SIZE {
			stats.Inc(stats.System("too-large"), 1, stats.SampleRate)
			log.Printf("WARN: %s: %s msg too large, dropping: %d > %d\n", prefix, data.Service, len(data.Data), proto.MAX_DATA_SIZE)
			continue // next report
		}

		// check if instance exists first
		if _, ok := existMap[report.UUID]; ok {
			// Queue the data in a per-service queue.
			tDb := time.Now()
			err = dbh.Write(report)
			stats.TimingDuration(stats.System("db"), time.Now().Sub(tDb), stats.SampleRate)
			if err != nil {
				if shared.IsNetworkError(err) {
					// This is usually due to losing connection to MySQL. Return an error
					// so the caller will restart the consumer.
					return fmt.Errorf("dbh.Write: %s", err)
				} else if err == shared.ErrReadOnlyDb {
					return fmt.Errorf("dbh.Write: %s", err)
				} else {
					// This is usually duplicate key errors, stuff we can't recover
					// from, so we just have to drop the data and move on. If it happens
					// a lot for many orgs, then maybe there's a real db problem, but
					// usually it's very random.
					log.Printf("WARN: %s: dbh.Write: %s", prefix, err)
					stats.Inc(stats.System("err-db"), 1, stats.SampleRate)
					stats.Inc(stats.Agent("err-db"), 1, stats.SampleRate)
					return nil
				}
			}
		}

		resp := proto.Response{Code: 200}

		// Don't let agent send too much data at once.
		nMsgs++
		if nMsgs >= MAX_DATA_MSG {
			resp.Code = THROTTLE_CODE
		}

		// Ack the data msg to the agent so it will remove it from its spool.
		if err := wsConn.Send(resp, 5); err != nil {
			return fmt.Errorf("wsConn.Send: %s", err)
		}

		if resp.Code == THROTTLE_CODE {
			log.Printf("%s: WARN: throttling agent because it has sent the max number of messages for one upload: %d."+
				" Check the agent's status to see its data spool size.", prefix, MAX_DATA_MSG)
			return nil
		}
	}

}

func decode(bytes []byte) (proto.Data, qp.Report, error) {
	var data proto.Data
	var report qp.Report

	// Errors in this func are not critical, we can log a warning and move on
	// because there's nothing else we can do about bad data. Usually these
	// errors are random and one-off, but if they become frequent then maybe
	// there's a system bug.

	if err := json.Unmarshal(bytes, &data); err != nil {
		return data, report, fmt.Errorf("json.Unmarshal(data): %s", err)
	}

	reportBytes, err := data.GetData()
	if err != nil {
		return data, report, fmt.Errorf("data.GetData: %s", err)
	}

	// Deserialize QAN report based on ProtocolVersion
	switch data.ProtocolVersion {
	case "1.0":
		if err := json.Unmarshal(reportBytes, &report); err != nil {
			return data, report, fmt.Errorf("json.Unmarshal(report): %s", err)
		}
	default:
		return data, report, fmt.Errorf("protocol version %s not supported", data.ProtocolVersion)
	}

	return data, report, nil
}
