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

package query

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/shatteredsilicon/qan-api/app/db"
	"github.com/shatteredsilicon/qan-api/app/db/mysql"
	"github.com/shatteredsilicon/qan-api/app/shared"
	queryService "github.com/shatteredsilicon/qan-api/service/query"
	"github.com/shatteredsilicon/qan-api/stats"
	queryProto "github.com/shatteredsilicon/ssm/proto/query"
)

func GetClassId(db *sql.DB, checksum string) (uint, error) {
	if checksum == "" {
		return 0, nil
	}
	var classId uint
	err := db.QueryRow("SELECT query_class_id FROM query_classes WHERE checksum = ?", checksum).Scan(&classId)
	if err != nil {
		return 0, mysql.Error(err, "GetClassId: SELECT query_classes")
	}
	return classId, nil
}

type MySQLHandler struct {
	dbm   db.Manager
	stats *stats.Stats
}

func NewMySQLHandler(dbm db.Manager, stats *stats.Stats) *MySQLHandler {
	h := &MySQLHandler{
		dbm:   dbm,
		stats: stats,
	}
	return h
}

func (h *MySQLHandler) Get(ids []string) (map[string]queryProto.Query, error) {
	q := "SELECT checksum, COALESCE(abstract, ''), fingerprint, COALESCE(tables, ''), COALESCE(procedures, ''), first_seen, last_seen, status" +
		" FROM query_classes" +
		" WHERE checksum IN (" + shared.Placeholders(len(ids)) + ")"
	v := shared.GenericStringList(ids)
	rows, err := h.dbm.DB().Query(q, v...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	queries := map[string]queryProto.Query{}
	for rows.Next() {
		query := queryProto.Query{}
		var tablesJSON, proceduresJSON string
		err := rows.Scan(
			&query.Id,
			&query.Abstract,
			&query.Fingerprint,
			&tablesJSON,
			&proceduresJSON,
			&query.FirstSeen,
			&query.LastSeen,
			&query.Status,
		)
		if err != nil {
			return nil, err
		}
		if tablesJSON != "" {
			var tables []queryProto.Table
			if err := json.Unmarshal([]byte(tablesJSON), &tables); err != nil {
				return nil, err
			}
			query.Tables = tables
		}
		if proceduresJSON != "" {
			var procedures []queryProto.Procedure
			if err := json.Unmarshal([]byte(proceduresJSON), &procedures); err != nil {
				return nil, err
			}
			query.Procedures = procedures
		}
		queries[query.Id] = query
	}

	return queries, nil
}

func (h *MySQLHandler) Examples(classId, instanceId uint) ([]queryProto.Example, error) {
	params := []interface{}{classId}
	q := "SELECT c.checksum, i.uuid, e.period, e.ts, e.db, e.Query_time, e.query" +
		" FROM query_examples e" +
		" JOIN query_classes c USING (query_class_id)" +
		" JOIN instances i USING (instance_id)" +
		" WHERE query_class_id = ?"
	if instanceId != 0 {
		q += " AND instance_id = ?"
		params = append(params, instanceId)
	}
	q += " ORDER BY period DESC"

	rows, err := h.dbm.DB().Query(q, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	examples := []queryProto.Example{}
	for rows.Next() {
		e := queryProto.Example{}
		err := rows.Scan(
			&e.QueryId,
			&e.InstanceUUID,
			&e.Period,
			&e.Ts,
			&e.Db,
			&e.QueryTime,
			&e.Query,
		)
		if err != nil {
			return nil, err
		}
		examples = append(examples, e)
	}

	return examples, nil
}

func (h *MySQLHandler) Example(classId uint, instanceIds []uint, period time.Time) (queryProto.Example, error) {
	e := queryProto.Example{}
	placeholders := "?" + strings.Repeat(",?", len(instanceIds)-1)
	values := []interface{}{classId}
	for i := range instanceIds {
		values = append(values, instanceIds[i])
	}
	values = append(values, period)
	q := fmt.Sprintf("SELECT qe.period, qe.ts, qe.db, .qe.Query_time, qe.query, qe.`explain`, i.uuid"+
		" FROM query_examples qe"+
		" JOIN instances i ON qe.instance_id = i.instance_id"+
		" WHERE query_class_id = ? AND qe.instance_id IN (%s) AND period <= ?"+
		" ORDER BY period DESC, Query_time DESC"+
		" LIMIT 1", placeholders)
	err := h.dbm.DB().QueryRow(q, values...).Scan(&e.Period, &e.Ts, &e.Db, &e.QueryTime, &e.Query, &e.Explain, &e.InstanceUUID)
	if err != nil {
		return e, mysql.Error(err, "Example: SELECT query_examples")
	}
	return e, nil
}

type UserSource struct {
	User      string
	Host      string
	FirstSeen int64
	LastSeen  int64
	Count     uint
}

func (h *MySQLHandler) UserSources(classId uint, instanceIds []uint, begin, end time.Time) ([]UserSource, error) {
	query, args, err := sqlx.In(`
		SELECT uc.user, uc.host, min(qus.ts), max(qus.ts), SUM(`+"qus.`count`"+`)
		FROM query_user_sources qus
		JOIN user_classes uc ON qus.user_class_id = uc.id
		WHERE qus.query_class_id = ? AND qus.instance_id IN (?) AND qus.ts >= ? AND qus.ts < ?
		GROUP BY query_class_id, instance_id, uc.user, uc.host
	`, classId, instanceIds, begin, end)
	if err != nil {
		return nil, mysql.Error(err, "UserSource: sqlx IN")
	}

	rows, err := h.dbm.DB().Query(query, args...)
	if err != nil {
		return nil, mysql.Error(err, "UserSource: SELECT query_user_sources")
	}
	defer rows.Close()

	userSources := make([]UserSource, 0)
	var firstSeen, lastSeen time.Time
	var user, host string
	var count uint
	for rows.Next() {
		if err := rows.Scan(&user, &host, &firstSeen, &lastSeen, &count); err != nil {
			return nil, mysql.Error(err, "UserSource: rows.Scan")
		}
		userSources = append(userSources, UserSource{
			User:      user,
			Host:      host,
			FirstSeen: firstSeen.UnixNano(),
			LastSeen:  lastSeen.UnixNano(),
			Count:     count,
		})
	}

	return userSources, nil
}

func (h *MySQLHandler) UpdateExample(classId, instanceId uint, example queryProto.Example) error {
	// todo: WHERE query_class_id=? AND instance_id=? AND period=?
	r, err := h.dbm.DB().Exec(
		"UPDATE query_examples SET db = ?"+
			" WHERE query_class_id = ? AND instance_id = ? AND period = ?",
		example.Db, classId, instanceId, example.Period,
	)
	if err != nil {
		return mysql.Error(err, "UpdateExample: UPDATE query_examples")
	}
	rowsAffected, err := r.RowsAffected()
	if rowsAffected == 0 {
		return shared.ErrNotFound
	}
	return nil
}

// UpdateTables writes JSON data of tables
// into table query_classes
func (h *MySQLHandler) UpdateTables(classID uint, tables []queryProto.Table) error {
	// We store []query.Table as a JSON string because this is SQL, not NoSQL.
	bytes, err := json.Marshal(tables)
	if err != nil {
		return err
	}
	_, err = h.dbm.DB().Exec("UPDATE query_classes SET tables = ? WHERE query_class_id = ?", string(bytes), classID)
	if err != nil {
		return mysql.Error(err, "UpdateTables: UPDATE query_classes")
	}
	return nil
}

// UpdateProcedures writes JSON data of procedures
// into table query_classes
func (h *MySQLHandler) UpdateProcedures(classID uint, procedures []queryProto.Procedure) error {
	// We store []query.Procedure as a JSON string because this is SQL, not NoSQL.
	bytes, err := json.Marshal(procedures)
	if err != nil {
		return err
	}
	_, err = h.dbm.DB().Exec("UPDATE query_classes SET procedures = ? WHERE query_class_id = ?", string(bytes), classID)
	if err != nil {
		return mysql.Error(err, "UpdateProcedures: UPDATE query_classes")
	}
	return nil
}

// UpdateTablesAndProcedures writes JSON data of talbes and procedures
// into table query_classes
func (h *MySQLHandler) UpdateTablesAndProcedures(classID uint, tables []queryProto.Table, procedures []queryProto.Procedure) error {
	// We store []query.Table and []query.Procedure as a JSON string because this is SQL, not NoSQL.
	tableBytes, err := json.Marshal(tables)
	if err != nil {
		return err
	}
	procedureBytes, err := json.Marshal(procedures)
	if err != nil {
		return err
	}

	_, err = h.dbm.DB().Exec("UPDATE query_classes SET tables = ?, procedures = ? WHERE query_class_id = ?", string(tableBytes), string(procedureBytes), classID)
	if err != nil {
		return mysql.Error(err, "UpdateTablesAndProcedures: UPDATE query_classes")
	}
	return nil
}

func (h *MySQLHandler) Tables(classId uint, m *queryService.Mini) ([]queryProto.Table, error) {
	// First try to get the tables. If we're lucky, they've already been parsed
	// and we're done.
	var tablesJSON string
	err := h.dbm.DB().QueryRow("SELECT COALESCE(tables, '') FROM query_classes WHERE query_class_id = ?", classId).Scan(&tablesJSON)
	if err != nil {
		return nil, mysql.Error(err, "Tables: SELECT query_classes (tables)")
	}

	// We're lucky: we already have tables.
	if tablesJSON != "" {
		var tables []queryProto.Table
		if err := json.Unmarshal([]byte(tablesJSON), &tables); err != nil {
			return nil, err
		}
		return tables, nil
	}

	// We're not lucky: this query hasn't been parsed yet, so do it now, if possible.
	var fingerprint string
	err = h.dbm.DB().QueryRow("SELECT fingerprint FROM query_classes WHERE query_class_id = ?", classId).Scan(&fingerprint)
	if err != nil {
		return nil, mysql.Error(err, "Tables: SELECT query_classes (fingerprint)")
	}

	// Get database from latest example.
	var example, db string
	err = h.dbm.DB().QueryRow(
		"SELECT query, db "+
			" FROM query_examples "+
			" JOIN query_classes c USING (query_class_id)"+
			" JOIN instances i USING (instance_id)"+
			" WHERE query_class_id = ?"+
			" ORDER BY period DESC",
		classId,
	).Scan(&example, &db)
	if err != nil {
		return nil, mysql.Error(err, "Tables: SELECT query_examples (db)")
	}

	// If this returns an error, then youtube/vitess/go/sqltypes/sqlparser
	// doesn't support the query type.
	tableInfo, err := m.Parse(fingerprint, example, db)
	if err != nil {
		return nil, shared.ErrNotImplemented
	}

	// The sqlparser was able to handle the query, so marshal the tables
	// into a string and update the tables column so next time we don't
	// have to parse the query.
	if err := h.UpdateTablesAndProcedures(classId, tableInfo.Tables, tableInfo.Procedures); err != nil {
		return nil, err
	}

	return tableInfo.Tables, nil
}
