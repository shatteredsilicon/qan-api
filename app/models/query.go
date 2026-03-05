package models

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/shatteredsilicon/qan-api/app/db/mysql"
)

// query provide methods to works with query
type query struct{}

// Query instance of report model
var Query = query{}

const summaryQueriesTotalTemplate = `
	SELECT
		COALESCE(SUM(Query_time_sum), 0) AS total_query_time
	FROM query_class_metrics
	WHERE instance_id IN ({{ .InstanceIDs }}) AND start_ts BETWEEN :begin AND :end
`

const summaryQueriesTemplate = `
	SELECT
		qcm.query_class_id AS query_class_id,
		COALESCE(SUM(qcm.query_count), 0) AS query_count,
		COALESCE(SUM(qcm.Query_time_sum), 0) AS query_time_sum,
		COALESCE(MIN(qcm.Query_time_min), 0) AS query_time_min,
		COALESCE(SUM(qcm.Query_time_sum)/SUM(qcm.query_count), 0) AS query_time_avg,
		COALESCE(AVG(qcm.Query_time_med), 0) AS query_time_med,
		COALESCE(AVG(qcm.Query_time_p95), 0) AS query_time_p95,
		COALESCE(MAX(qcm.Query_time_max), 0) AS query_time_max,
		qc.checksum AS checksum,
		qc.abstract AS abstract,
		qc.fingerprint AS fingerprint,
		qc.first_seen AS first_seen,
		qe.query AS example,
		GROUP_CONCAT(DISTINCT(qcm.instance_id)) AS instance_ids
	FROM query_class_metrics AS qcm
	JOIN query_classes AS qc ON qcm.query_class_id = qc.query_class_id
	LEFT JOIN query_examples AS qe ON qcm.query_class_id = qe.query_class_id AND qcm.instance_id = qe.instance_id AND date(qcm.start_ts) = date(qe.period)
	WHERE qcm.instance_id IN ({{ .InstanceIDs }}) AND (qcm.start_ts >= :begin AND qcm.start_ts < :end)
	GROUP BY qcm.query_class_id
	{{ if and .Load .TotalQueryTime }}
	HAVING query_time_sum / :total_query_time >= :load
	{{ end }}
`

type SummaryQuery struct {
	QueryClassID   uint      `db:"query_class_id" json:"-"`
	Checksum       string    `db:"checksum" json:"checksum"`
	Abstract       string    `db:"abstract" json:"abstract"`
	Fingerprint    string    `db:"fingerprint" json:"fingerprint"`
	FirstSeen      time.Time `db:"first_seen" json:"-"`
	InstanceIDsStr string    `db:"instance_ids" json:"-"`
	InstanceIDs    []uint    `db:"-" json:"instance_ids"`
	QueryCnt       uint64    `db:"query_count" json:"query_count"`
	Percentage     float64   `db:"-" json:"percentage"`
	Load           float64   `db:"-" json:"load"`
	Example        string    `db:"example" json:"example"`
	QueryTimeSum   float64   `db:"query_time_sum" json:"query_time_sum"`
	QueryTimeMin   float64   `db:"query_time_min" json:"query_time_min"`
	QueryTimeAvg   float64   `db:"query_time_avg" json:"query_time_avg"`
	QueryTimeMed   float64   `db:"query_time_med" json:"query_time_med"`
	QueryTimeP95   float64   `db:"query_time_p95" json:"query_time_p95"`
	QueryTimeMax   float64   `db:"query_time_max" json:"query_time_max"`
}

func (query) SummaryQueries(instanceIDs []uint, begin, end time.Time, load float64) ([]SummaryQuery, error) {
	instanceIDStrs := make([]string, len(instanceIDs))
	for i := range instanceIDs {
		instanceIDStrs[i] = fmt.Sprintf("%d", instanceIDs[i])
	}

	args := struct {
		InstanceIDs    string
		Begin          time.Time
		End            time.Time
		TotalQueryTime float64 `db:"total_query_time"`
		Load           float64
	}{
		InstanceIDs: strings.Join(instanceIDStrs, ","),
		Begin:       begin,
		End:         end,
		Load:        load,
	}

	var summaryQueriesTotalBuffer bytes.Buffer
	if tmpl, err := template.New("summaryQueriesTotalSQL").Parse(summaryQueriesTotalTemplate); err != nil {
		log.Fatalln(err)
	} else if err = tmpl.Execute(&summaryQueriesTotalBuffer, args); err != nil {
		log.Fatalln(err)
	}

	nstmtQueryReportTotal, err := db.PrepareNamed(summaryQueriesTotalBuffer.String())
	if err != nil {
		return nil, mysql.Error(err, "query.SummaryQueries: db.PrepareNamed: summaryQueriesTotalSQL")
	}
	defer nstmtQueryReportTotal.Close()

	err = nstmtQueryReportTotal.Get(&args, args)
	if err != nil {
		return nil, mysql.Error(err, "query.SummaryQueries: nstmt.Get: summaryQueriesTotalSQL")
	}

	var summaryQueriesBuffer bytes.Buffer
	if tmpl, err := template.New("summaryQueriesSQL").Parse(summaryQueriesTemplate); err != nil {
		log.Fatalln(err)
	} else if err = tmpl.Execute(&summaryQueriesBuffer, args); err != nil {
		log.Fatalln(err)
	}

	queries := []SummaryQuery{}
	nstmtQuerySummary, err := db.PrepareNamed(summaryQueriesBuffer.String())
	if err != nil {
		return queries, mysql.Error(err, "query.SummaryQueries: db.PrepareNamed: summaryQueriesSQL")
	}
	defer nstmtQuerySummary.Close()

	err = nstmtQuerySummary.Select(&queries, args)
	if err != nil {
		return queries, mysql.Error(err, "query.SummaryQueries: nstmt.Select: summaryQueriesSQL")
	}

	intervalTime := end.Sub(begin).Seconds()
	for i, query := range queries {
		if intervalTime > 0 {
			queries[i].Load = query.QueryTimeSum / intervalTime
		}
		if args.TotalQueryTime > 0 {
			queries[i].Percentage = query.QueryTimeSum / args.TotalQueryTime
		}

		ids := []uint{}
		for _, idStr := range strings.Split(strings.TrimSpace(query.InstanceIDsStr), ",") {
			if idStr == "" {
				continue
			}

			id, _ := strconv.ParseUint(idStr, 10, 32)
			ids = append(ids, uint(id))
		}
		queries[i].InstanceIDs = ids
	}

	return queries, nil
}
