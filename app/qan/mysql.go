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
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shatteredsilicon/qan-api/app/db"
	"github.com/shatteredsilicon/qan-api/app/db/mysql"
	"github.com/shatteredsilicon/qan-api/app/instance"
	"github.com/shatteredsilicon/qan-api/app/shared"
	"github.com/shatteredsilicon/qan-api/service/query"
	"github.com/shatteredsilicon/qan-api/stats"
	"github.com/shatteredsilicon/ssm/proto/metrics"
	"github.com/shatteredsilicon/ssm/proto/qan"
	qp "github.com/shatteredsilicon/ssm/proto/qan"
)

const (
	MAX_ABSTRACT    = 100  // query_classes.abstract
	MAX_FINGERPRINT = 5000 // query_classes.fingerprint
)

type MySQLMetricWriter struct {
	dbm   db.Manager
	ih    instance.DbHandler
	m     *query.Mini
	stats *stats.Stats
	// --
	stmtSelectClassId             *sql.Stmt
	stmtInsertGlobalMetrics       *sql.Stmt
	stmtInsertClassMetrics        *sql.Stmt
	stmtInsertQueryExample        *sql.Stmt
	stmtInsertQueryUserSource     *sql.Stmt
	stmtInsertUserClass           *sql.Stmt
	stmtInsertQueryClass          *sql.Stmt
	stmtUpdateQueryClass          *sql.Stmt
	stmtUpdateQueryClassWithoutTP *sql.Stmt
}

func NewMySQLMetricWriter(
	dbm db.Manager,
	ih instance.DbHandler,
	m *query.Mini,
	stats *stats.Stats,
) *MySQLMetricWriter {
	h := &MySQLMetricWriter{
		dbm:   dbm,
		ih:    ih,
		m:     m,
		stats: stats,
	}
	return h
}

func (h *MySQLMetricWriter) Write(report qp.Report) error {
	var err error

	instanceId, in, err := h.ih.Get(report.UUID)
	if err != nil {
		return fmt.Errorf("cannot get instance of %s: %s", report.UUID, err)
	}

	if report.Global == nil {
		return fmt.Errorf("missing report.Global")
	}

	if report.Global.Metrics == nil {
		return fmt.Errorf("missing report.Global.Metrics")
	}

	trace := fmt.Sprintf("MySQL %s", report.UUID)

	h.prepareStatements()
	defer h.closeStatements()

	// Default last_seen if no example query ts.
	var reportStartTs string
	if !report.StartTs.IsZero() {
		reportStartTs = report.StartTs.Format(shared.MYSQL_DATETIME_LAYOUT)
	}

	// Internal metrics
	h.stats.SetComponent("db")
	t := time.Now()

	handleClass := func(id uint, class *qp.Class, lastSeen string) (uint, int64, int64, error) {
		if id == 0 {
			// New class, create it.
			id, err = h.newClass(instanceId, in.Subsystem, class, lastSeen)
			if err != nil {
				log.Printf("WARNING: cannot create new query class, skipping: %s: %#v: %s", err, class, trace)
				return 0, 0, 0, err
			}
		}

		// Update query example if this example has a greater Query_time.
		// The "!= nil" is for agent >= v1.0.11 which use *event.Class.Example,
		// but agent <= v1.0.10 don't use a pointer so the struct is always
		// present, so "class.Example.Query != """ filters out empty examples.
		var lastExampleId, exampleRowsAffected int64
		if class.Example != nil && class.Example.Query != "" {
			if lastExampleId, exampleRowsAffected, err = h.updateQueryExample(instanceId, class, id, lastSeen); err != nil {
				log.Printf("WARNING: cannot update query example: %s: %#v: %s", err, class, trace)
			}
		}

		for i := range class.UserSources {
			if err := h.insertUserSource(id, instanceId, class.UserSources[i]); err != nil {
				log.Printf("WARNING: cannot insert query user source: %s: %#v", err, class.UserSources[i])
			}
		}

		return id, lastExampleId, exampleRowsAffected, nil
	}

	// //////////////////////////////////////////////////////////////////////
	// Insert class metrics into query_class_metrics
	// //////////////////////////////////////////////////////////////////////
	classDupes := 0
	for _, class := range report.Class {
		lastSeen := ""
		if class.Example != nil {
			lastSeen = class.Example.Ts
		}
		if lastSeen == "" {
			if reportStartTs != "" {
				lastSeen = reportStartTs
			} else {
				lastSeen = class.StartAt.Format(shared.MYSQL_DATETIME_LAYOUT)
			}
		}

		id, err := h.getClassId(class.Id)
		if err != nil && err != sql.ErrNoRows {
			log.Printf("WARNING: cannot get query class ID, skipping: %s: %#v: %s", err, class, trace)
			continue
		}

		classExists := id != 0
		var lastExampleId, exampleRowsAffected int64

		id, lastExampleId, exampleRowsAffected, err = handleClass(id, class, lastSeen)
		if err != nil {
			continue
		}

		if classExists {
			// Existing class, update it.  These update aren't fatal, but they shouldn't fail.
			var q query.QueryInfo
			var err error

			// Parse the tables/procedures only if the query example gets updated,
			// so the tables/procedures column in query_classes table are in sync
			// with the db/query column in query_examples table
			if in.Subsystem == instance.SubsystemNameMySQL && (lastExampleId > 0 || exampleRowsAffected > 0) {
				q, err = h.getQuery(class)
				if err != nil {
					log.Printf("WARNING: cannot parse query to update: %s", err)
				}
			}

			// Update the table/procedures column only if query example gets updated
			if lastExampleId > 0 || exampleRowsAffected > 0 {
				err = h.updateQueryClass(id, lastSeen, q.TableJSON(), q.ProcedureJSON())
			} else {
				err = h.updateQueryClassWithoutTP(id, lastSeen)
			}

			// it existed previously but non-exist now, could be just deleted
			// by another process (most likely the auto-purge process), we re-create
			// it in this case
			if err == shared.ErrNotFound {
				id, _, _, err = handleClass(0, class, lastSeen)
				if err != nil {
					continue
				}
			} else if err != nil {
				log.Printf("WARNING: cannot update query class, skipping: %s: %#v: %s", err, class, trace)
				continue
			}
		}

		var classStartTs, classEndTs time.Time
		if report.StartTs.IsZero() {
			classStartTs = class.StartAt
		} else {
			classStartTs = report.StartTs
		}
		if report.EndTs.IsZero() {
			classEndTs = class.EndAt
		} else {
			classEndTs = report.EndTs
		}

		vals := h.getMetricValues(class.Metrics)
		classVals := []interface{}{
			id,
			instanceId,
			classStartTs,
			classEndTs,
			class.TotalQueries,
			0, // todo: `lrq_count`,
		}
		classVals = append(classVals, vals...)

		// INSERT query_class_metrics
		_, err = h.stmtInsertClassMetrics.Exec(classVals...)
		if err != nil {
			if mysql.ErrorCode(err) == mysql.ER_DUP_ENTRY {
				classDupes++
				// warn below
			} else {
				log.Printf("WARNING: cannot insert query class metrics: %s: %#v: %s", err, class, trace)
			}
		}
	}

	h.stats.TimingDuration(h.stats.System("insert-class-metrics"), time.Now().Sub(t), h.stats.SampleRate)

	if classDupes > 0 {
		log.Printf("WARNING: %d duplicate query class metrics: start_ts='%s': %s", classDupes, report.StartTs, trace)
	}

	// //////////////////////////////////////////////////////////////////////
	// Insert global metrics into query_global_metrics.
	// //////////////////////////////////////////////////////////////////////

	// It's important to do this after class metics to avoid a race condition:
	// QAN profile looks first at global metrics, then gets corresponding class
	// metrics. If we insert global metrics first, QAN might might get global
	// metrics then class metrics before we've inserted the class metrics for
	// the global metrics. This makes QAN show data for the time range but no
	// queries.

	vals := h.getMetricValues(report.Global.Metrics)

	// Use NULL for Percona Server rate limit values unless set.
	var (
		globalRateType  interface{} = nil
		globalRateLimit interface{} = nil
	)
	if report.RateLimit > 1 {
		globalRateType = "query" // only thing we support
		globalRateLimit = report.RateLimit
	}

	// Use NULL for slow log values unless set.
	var (
		slowLogFile     interface{} = nil
		slowLogFileSize interface{} = nil
		startOffset     interface{} = nil
		endOffset       interface{} = nil
		stopOffset      interface{} = nil
	)
	if report.SlowLogFile != "" {
		slowLogFile = report.SlowLogFile
		slowLogFileSize = report.SlowLogFileSize
		startOffset = report.StartOffset
		endOffset = report.EndOffset
		stopOffset = report.StopOffset
	}

	var globalStartTs, globalEndTs time.Time
	if report.StartTs.IsZero() {
		globalStartTs = report.Global.StartAt
	} else {
		globalStartTs = report.StartTs
	}
	if report.EndTs.IsZero() {
		globalEndTs = report.Global.EndAt
	} else {
		globalEndTs = report.EndTs
	}

	globalVals := []interface{}{
		instanceId,
		globalStartTs,
		globalEndTs,
		report.RunTime,
		report.Global.TotalQueries,
		report.Global.UniqueQueries,
		globalRateType,
		globalRateLimit,
		slowLogFile,
		slowLogFileSize,
		startOffset,
		endOffset,
		stopOffset,
	}

	globalVals = append(globalVals, vals...)
	t = time.Now()
	_, err = h.stmtInsertGlobalMetrics.Exec(globalVals...)
	h.stats.TimingDuration(h.stats.System("insert-global-metrics"), time.Now().Sub(t), h.stats.SampleRate)
	if err != nil {
		if mysql.ErrorCode(err) == mysql.ER_DUP_ENTRY {
			log.Printf("WARNING: duplicate global metrics: start_ts='%s': %s", report.StartTs, trace)
		} else {
			return mysql.Error(err, "writeMetrics insertGlobalMetrics")
		}
	}

	return nil
}

func (h *MySQLMetricWriter) getClassId(checksum string) (uint, error) {
	var classId uint
	if err := h.stmtSelectClassId.QueryRow(checksum).Scan(&classId); err != nil {
		return 0, err
	}
	return classId, nil
}

func (h *MySQLMetricWriter) newClass(instanceId uint, subsystem string, class *qan.Class, lastSeen string) (uint, error) {
	var queryAbstract, queryFingerprint string
	var tables, procedures interface{}

	switch subsystem {
	case instance.SubsystemNameMySQL:
		t := time.Now()
		var query query.QueryInfo
		var err error
		query, err = h.getQuery(class)
		if err != nil {
			return 0, err
		}
		tables, procedures = query.TableJSON(), query.ProcedureJSON()

		h.stats.TimingDuration(h.stats.System("abstract-fingerprint"), time.Now().Sub(t), h.stats.SampleRate)

		// Truncate long fingerprints and abstracts to avoid MySQL warning 1265:
		// Data truncated for column 'abstract'
		if len(query.Fingerprint) > MAX_FINGERPRINT {
			query.Fingerprint = query.Fingerprint[0:MAX_FINGERPRINT-3] + "..."
		}
		if len(query.Abstract) > MAX_ABSTRACT {
			query.Abstract = query.Abstract[0:MAX_ABSTRACT-3] + "..."
		}
		queryAbstract = query.Abstract
		queryFingerprint = query.Fingerprint
	case instance.SubsystemNameMongo:
		queryAbstract = class.Fingerprint
		queryFingerprint = class.Fingerprint
	}

	// Create the query class which is internally identified by its query_class_id.
	// The query checksum is the class is identified externally (in a QAN report).
	// Since this is the first time we've seen the query, firstSeen=lastSeen.
	t := time.Now()
	res, err := h.stmtInsertQueryClass.Exec(class.Id, queryAbstract, queryFingerprint, tables, procedures, lastSeen, lastSeen)

	h.stats.TimingDuration(h.stats.System("insert-query-class"), time.Now().Sub(t), h.stats.SampleRate)
	if err != nil {
		if mysql.ErrorCode(err) == mysql.ER_DUP_ENTRY {
			// Duplicate entry; someone else inserted the same server
			// (or caller didn't check first).  Return its server_id.
			return h.getClassId(class.Id)
		} else {
			// Other error, let caller handle.
			return 0, mysql.Error(err, "newClass INSERT query_classes")
		}
	}
	classId, err := res.LastInsertId()
	if err != nil {
		return 0, mysql.Error(err, "newClass res.LastInsertId")
	}

	return uint(classId), nil // success
}

func (h *MySQLMetricWriter) getQuery(class *qan.Class) (query.QueryInfo, error) {
	var schema string
	var queryInfo query.QueryInfo
	// Default schema to add to the tables if there is no schema in the query like:
	// SELECT a, b, c FROM table
	if class.Example != nil {
		schema = class.Example.Db
	}
	if len(class.Fingerprint) < 2 {
		return queryInfo, fmt.Errorf("empty fingerprint")
	}

	queryExample := ""
	if class.Example != nil && class.Example.Query != "" {
		queryExample = class.Example.Query
	}
	query, err := h.m.Parse(class.Fingerprint, queryExample, schema)
	if err != nil {
		return queryInfo, err
	}

	return query, nil
}

func (h *MySQLMetricWriter) updateQueryClass(queryClassId uint, lastSeen, tables, procedures string) error {
	t := time.Now()
	_, err := h.stmtUpdateQueryClass.Exec(lastSeen, lastSeen, tables, procedures, queryClassId)
	h.stats.TimingDuration(h.stats.System("update-query-class"), time.Now().Sub(t), h.stats.SampleRate)
	return mysql.Error(err, "updateQueryClass UPDATE query_classes")
}

func (h *MySQLMetricWriter) updateQueryClassWithoutTP(queryClassId uint, lastSeen string) error {
	t := time.Now()
	_, err := h.stmtUpdateQueryClassWithoutTP.Exec(lastSeen, lastSeen, queryClassId)
	h.stats.TimingDuration(h.stats.System("update-query-class-without-tables-procedures"), time.Now().Sub(t), h.stats.SampleRate)
	return mysql.Error(err, "updateQueryClassWithoutTP UPDATE query_classes")
}

func (h *MySQLMetricWriter) updateQueryExample(instanceId uint, class *qan.Class, classId uint, lastSeen string) (int64, int64, error) {
	var lastInsertId, rowsAffected int64

	// INSERT ON DUPLICATE KEY UPDATE
	t := time.Now()
	res, err := h.stmtInsertQueryExample.Exec(instanceId, classId, lastSeen, lastSeen, class.Example.Db, class.Example.QueryTime, class.Example.Query, class.Example.Explain)
	if err == nil {
		lastInsertId, _ = res.LastInsertId()
		rowsAffected, _ = res.RowsAffected()
	}
	h.stats.TimingDuration(h.stats.System("update-query-example"), time.Now().Sub(t), h.stats.SampleRate)
	return lastInsertId, rowsAffected, mysql.Error(err, "updateQueryExample INSERT query_examples")
}

func (h *MySQLMetricWriter) insertUserSource(classID, instanceID uint, source qp.UserSource) error {
	if _, err := h.stmtInsertUserClass.Exec(source.User, source.Host); err != nil {
		return err
	}

	_, err := h.stmtInsertQueryUserSource.Exec(classID, instanceID, source.User, source.Host, source.Ts, source.Count)
	return err
}

func (h *MySQLMetricWriter) getMetricValues(e *qan.Metrics) []interface{} {
	t := time.Now()
	defer func() {
		h.stats.TimingDuration(h.stats.System("get-metric-values"), time.Now().Sub(t), h.stats.SampleRate)
	}()

	vals := make([]interface{}, len(metricColumns))
	i := 0
	for _, m := range metrics.Query {

		// Counter/bools
		if (m.Flags & metrics.COUNTER) != 0 {
			stats, haveMetric := e.BoolMetrics[m.Name]
			if haveMetric {
				vals[i] = stats.Sum
			}
			i++
			continue
		}

		// Microsecond/time
		if (m.Flags & metrics.MICROSECOND) != 0 {
			stats, haveMetric := e.TimeMetrics[m.Name]
			for _, stat := range metrics.StatNames {
				if stat == "p5" {
					continue
				}
				var val interface{} = nil
				if haveMetric {
					switch stat {
					case "sum":
						val = stats.Sum
					case "min":
						val = stats.Min
					case "max":
						val = stats.Max
					case "avg":
						val = stats.Avg
					case "p95":
						val = stats.P95
					case "med":
						val = stats.Med
					default:
						log.Printf("ERROR: unknown stat: %s %s\n", m.Name, stat)
					}
				}
				vals[i] = val
				i++
			}
			continue
		}

		// Metric isn't microsecond or bool/counter, so it must be numbers, like Rows_sent.
		stats, haveMetric := e.NumberMetrics[m.Name]
		for _, stat := range metrics.StatNames {
			if stat == "p5" {
				continue
			}
			var val interface{} = nil
			if haveMetric {
				switch stat {
				case "sum":
					val = stats.Sum
				case "min":
					val = stats.Min
				case "max":
					val = stats.Max
				case "avg":
					val = stats.Avg
				case "p95":
					val = stats.P95
				case "med":
					val = stats.Med
				default:
					log.Printf("ERROR: unknown stat: %s %s\n", m.Name, stat)
				}
			}
			vals[i] = val
			i++
		}
	}

	return vals
}

func (h *MySQLMetricWriter) prepareStatements() {
	t := time.Now()
	defer func() {
		h.stats.TimingDuration(h.stats.System("prepare-stmts"), time.Now().Sub(t), h.stats.SampleRate)
	}()

	var err error

	// SELECT
	h.stmtSelectClassId, err = h.dbm.DB().Prepare(
		"SELECT query_class_id" +
			" FROM query_classes" +
			" WHERE checksum = ?")
	if err != nil {
		panic("Failed to prepare stmtSelectClassId:" + err.Error())
	}

	// INSERT
	h.stmtInsertGlobalMetrics, err = h.dbm.DB().Prepare(insertGlobalMetrics)
	if err != nil {
		panic("Failed to prepare stmtInsertGlobalMetrics: " + err.Error())
	}

	h.stmtInsertClassMetrics, err = h.dbm.DB().Prepare(insertClassMetrics)
	if err != nil {
		panic("Failed to prepare stmtInsertClassMetrics: " + err.Error())
	}

	h.stmtInsertQueryExample, err = h.dbm.DB().Prepare(
		"INSERT INTO query_examples" +
			" (instance_id, query_class_id, period, ts, db, Query_time, query, `explain`)" +
			" VALUES (?, ?, DATE(?), ?, ?, ?, ?, ?)" +
			" ON DUPLICATE KEY UPDATE" +
			" query=IF(VALUES(Query_time) > COALESCE(Query_time, 0), VALUES(query), query)," +
			" ts=IF(VALUES(Query_time) > COALESCE(Query_time, 0), VALUES(ts), ts)," +
			" db=IF(VALUES(Query_time) > COALESCE(Query_time, 0), VALUES(db), db)," +
			" Query_time=IF(VALUES(Query_time) > COALESCE(Query_time, 0), VALUES(Query_time), Query_time)")
	if err != nil {
		panic("Failed to prepare stmtInsertQueryExample: " + err.Error())
	}

	h.stmtInsertUserClass, err = h.dbm.DB().Prepare(
		"INSERT IGNORE INTO user_classes" +
			" (user, host)" +
			" VALUES(?, ?)",
	)
	if err != nil {
		panic("Failed to prepare stmtInsertUserClass: " + err.Error())
	}

	h.stmtInsertQueryUserSource, err = h.dbm.DB().Prepare(
		"INSERT INTO query_user_sources" +
			" (query_class_id, instance_id, user_class_id, ts, `count`)" +
			" VALUES (?, ?, (SELECT id FROM user_classes WHERE user = ? AND host = ?), ?, ?)" +
			" ON DUPLICATE KEY UPDATE" +
			" `count`=VALUES(`count`)+`count`," +
			" ts=ts",
	)
	if err != nil {
		panic("Failed to prepare stmtInsertQueryUserSource: " + err.Error())
	}

	/* Why use LEAST and GREATEST and update first_seen?
	   Because of the asynchronous nature of agents communication, we can receive
	   the same query from 2 different agents but it isn't madatory that the first
	   one we receive, is the older one. There could have been a network error on
	   the agent having the oldest data
	*/
	h.stmtInsertQueryClass, err = h.dbm.DB().Prepare(
		"INSERT INTO query_classes" +
			" (checksum, abstract, fingerprint, tables, procedures, first_seen, last_seen)" +
			" VALUES (?, ?, ?, ?, ?, COALESCE(?, NOW()), ?)")
	if err != nil {
		panic("Failed to prepare stmtInsertQueryClass: " + err.Error())
	}

	// UPDATE
	h.stmtUpdateQueryClass, err = h.dbm.DB().Prepare(
		"UPDATE query_classes" +
			" SET first_seen = LEAST(first_seen, ?), " +
			" last_seen = GREATEST(last_seen, ?), " +
			" tables = ?, " +
			" procedures = ? " +
			" WHERE query_class_id = ?")
	if err != nil {
		panic("Failed to prepare stmtUpdateQueryClass: " + err.Error())
	}

	// UPDATE query_classes without tables and procedures
	h.stmtUpdateQueryClassWithoutTP, err = h.dbm.DB().Prepare(
		"UPDATE query_classes" +
			" SET first_seen = LEAST(first_seen, ?), " +
			" last_seen = GREATEST(last_seen, ?) " +
			" WHERE query_class_id = ?")
	if err != nil {
		panic("Failed to prepare stmtUpdateQueryClassWithoutTP: " + err.Error())
	}
}

func (h *MySQLMetricWriter) closeStatements() {
	h.stmtSelectClassId.Close()
	h.stmtInsertGlobalMetrics.Close()
	h.stmtInsertClassMetrics.Close()
	h.stmtInsertQueryExample.Close()
	h.stmtInsertQueryUserSource.Close()
	h.stmtInsertUserClass.Close()
	h.stmtInsertQueryClass.Close()
	h.stmtUpdateQueryClass.Close()
	h.stmtUpdateQueryClassWithoutTP.Close()
}

// --------------------------------------------------------------------------

var metricColumns []string
var metricDuplicateUpdates []string
var globalMetricDuplicateUpdates []string
var insertGlobalMetrics string
var insertClassMetrics string

func init() {
	nCounters := 0
	for _, m := range metrics.Query {
		if (m.Flags & metrics.COUNTER) == 0 {
			nCounters++
		}
	}
	n := ((len(metrics.Query) - nCounters) * (len(metrics.StatNames) - 1)) + nCounters
	metricColumns = make([]string, n)
	metricDuplicateUpdates = make([]string, n)
	globalMetricDuplicateUpdates = make([]string, n)

	i := 0
	for _, m := range metrics.Query {
		if (m.Flags & metrics.COUNTER) == 0 {
			for _, stat := range metrics.StatNames {
				if stat == "p5" {
					continue
				}

				metricColumns[i] = m.Name + "_" + stat
				switch stat {
				case "sum":
					metricDuplicateUpdates[i] = fmt.Sprintf("%s = COALESCE(%s+VALUES(%s), %s, VALUES(%s))", metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i])
					globalMetricDuplicateUpdates[i] = metricDuplicateUpdates[i]
				case "min":
					metricDuplicateUpdates[i] = fmt.Sprintf("%s = IF(VALUES(%s) < %s, COALESCE(VALUES(%s), %s), COALESCE(%s, VALUES(%s)))", metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i])
					globalMetricDuplicateUpdates[i] = metricDuplicateUpdates[i]
				case "avg":
					metricDuplicateUpdates[i] = fmt.Sprintf("%s = COALESCE((VALUES(query_count)*VALUES(%s) + query_count*%s)/(query_count+VALUES(query_count)), %s, VALUES(%s))", metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i])
					globalMetricDuplicateUpdates[i] = fmt.Sprintf("%s = COALESCE((VALUES(total_query_count)*VALUES(%s) + total_query_count*%s)/(total_query_count+VALUES(total_query_count)), %s, VALUES(%s))", metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i])
				case "med", "p95", "max":
					metricDuplicateUpdates[i] = fmt.Sprintf("%s = IF(VALUES(%s) > %s, COALESCE(VALUES(%s), %s), COALESCE(%s, VALUES(%s)))", metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i])
					globalMetricDuplicateUpdates[i] = metricDuplicateUpdates[i]
				default:
					metricDuplicateUpdates[i] = fmt.Sprintf("%s = COALESCE((VALUES(%s)+%s)/2, VALUES(%s), %s)", metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i])
					globalMetricDuplicateUpdates[i] = metricDuplicateUpdates[i]
				}
				i++
			}
		} else {
			metricColumns[i] = m.Name + "_sum"
			metricDuplicateUpdates[i] = fmt.Sprintf("%s = COALESCE(%s+VALUES(%s), %s, VALUES(%s))", metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i], metricColumns[i])
			globalMetricDuplicateUpdates[i] = metricDuplicateUpdates[i]
			i++
		}
	}

	insertGlobalMetrics = "INSERT INTO query_global_metrics" +
		" (" + strings.Join(GlobalCols, ",") + "," + strings.Join(metricColumns, ",") + ")" +
		" VALUES (" + shared.Placeholders(len(GlobalCols)+len(metricColumns)) + ")" +
		" ON DUPLICATE KEY UPDATE " +
		"	end_ts = IF(VALUES(end_ts) > end_ts, COALESCE(VALUES(end_ts), end_ts), COALESCE(end_ts, VALUES(end_ts))), " +
		"	run_time = COALESCE(VALUES(run_time) + run_time, run_time, VALUES(run_time)), " +
		"	total_query_count = COALESCE(VALUES(total_query_count) + total_query_count, total_query_count, VALUES(total_query_count)), " +
		strings.Join(globalMetricDuplicateUpdates, ", ")

	insertClassMetrics = "INSERT INTO query_class_metrics" +
		" (" + strings.Join(ClassCols, ",") + "," + strings.Join(metricColumns, ",") + ")" +
		" VALUES (" + shared.Placeholders(len(ClassCols)+len(metricColumns)) + ")" +
		" ON DUPLICATE KEY UPDATE " +
		"	end_ts = IF(VALUES(end_ts) > end_ts, COALESCE(VALUES(end_ts), end_ts), COALESCE(end_ts, VALUES(end_ts))), " +
		"	query_count = COALESCE(VALUES(query_count) + query_count, query_count, VALUES(query_count)), " +
		strings.Join(metricDuplicateUpdates, ", ")
}

var GlobalCols []string = []string{
	`instance_id`,
	`start_ts`,
	`end_ts`,
	`run_time`,
	`total_query_count`,
	`unique_query_count`,
	`rate_type`,
	`rate_limit`,
	`log_file`,
	`log_file_size`,
	`start_offset`,
	`end_offset`,
	`stop_offset`,
}

var ClassCols []string = []string{
	`query_class_id`,
	`instance_id`,
	`start_ts`,
	`end_ts`,
	`query_count`,
	`lrq_count`,
}
