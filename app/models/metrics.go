package models

import (
	"bytes"
	"log"
	"reflect"
	"strings"
	"text/template"
	"time"
)

type (

	// MetricsManager provire instruments to works with metrics
	MetricsManager struct {
		conns *ConnectionsPool
	}

	// MetricGroup uses to build query depend on db instance type
	MetricGroup struct {
		Basic             bool
		PerconaServer     bool `db:"percona_server"`
		PerformanceSchema bool `db:"performance_schema"`
		ServerSummary     bool
		CountField        string
	}

	args struct {
		ClassID    uint      `db:"class_id"`
		InstanceID uint      `db:"instance_id"`
		Begin      time.Time `db:"begin"`
		End        time.Time `db:"end"`
		EndTS      int64     `db:"end_ts"`
		IntervalTS int64     `db:"interval_ts"`
	}
)

// NewMetricsManager instance of metrics model
func NewMetricsManager(connsPool interface{}) *MetricsManager {
	conns := connsPool.(*ConnectionsPool)
	return &MetricsManager{conns}
}

const metricGroupQuery = `
	SELECT
    ifNull(
    (
        SELECT 1
        FROM query_class_metrics
        WHERE (instance_id = :instance_id) AND ((start_ts >= toDateTime(:begin)) AND (start_ts < toDateTime(:end))) AND (Query_time_sum > 0)
        LIMIT 1
    ), 0) AS basic,
    ifNull(
    (
        SELECT 1
        FROM query_class_metrics
        WHERE (instance_id = :instance_id) AND ((start_ts >= toDateTime(:begin)) AND (start_ts < toDateTime(:end))) AND (Rows_affected_sum > 0)
        LIMIT 1
    ), 0) AS percona_server,
    ifNull(
    (
        SELECT 1
        FROM query_class_metrics
        WHERE (instance_id = :instance_id) AND ((start_ts >= toDateTime(:begin)) AND (start_ts < toDateTime(:end))) AND (Errors_sum > 0)
        LIMIT 1
    ), 0) AS performance_schema;
`

func (m *MetricsManager) identifyMetricGroup(instanceID uint, begin, end time.Time) *MetricGroup {
	currentMetricGroup := MetricGroup{}
	currentMetricGroup.CountField = "query_count"
	args := struct {
		InstanceID uint `db:"instance_id"`
		Begin      time.Time
		End        time.Time
	}{
		instanceID,
		begin,
		end,
	}
	if nstmt, err := m.conns.ClickHouse.PrepareNamed(metricGroupQuery); err != nil {
		log.Fatalln(err)
	} else if err = nstmt.Get(&currentMetricGroup, args); err != nil {
		log.Fatalln(err)
	}

	return &currentMetricGroup
}

// ClassMetrics all mertics
type ClassMetrics struct {
	*GeneralMetrics
	*MetricsPercentOfTotal
	*RateMetrics
	*SpecialMetrics
}

// GetClassMetrics return metrics for given instance and query class
func (m *MetricsManager) GetClassMetrics(classID, instanceID uint, begin, end time.Time) (*ClassMetrics, *[]RateMetrics) {
	currentMetricGroup := m.identifyMetricGroup(instanceID, begin, end)
	currentMetricGroup.CountField = "query_count"
	endTs := end.Unix()
	intervalTs := (endTs - begin.Unix()) / (amountOfPoints - 1)
	args := args{
		classID,
		instanceID,
		begin,
		end,
		endTs,
		intervalTs,
	}
	// this two lines should be before ServerSummary = true
	generalClassMetrics := m.getMetrics(currentMetricGroup, args)
	sparks := m.getSparklines(currentMetricGroup, args)

	// turns metric group to global
	currentMetricGroup.ServerSummary = true
	currentMetricGroup.CountField = "total_query_count"
	generalGlobalMetrics := m.getMetrics(currentMetricGroup, args)

	classMetricsOfTotal := m.computeOfTotal(generalClassMetrics, generalGlobalMetrics)
	aMetrics := m.computeRateMetrics(generalClassMetrics, begin, end)
	sMetrics := m.computeSpecialMetrics(generalClassMetrics)

	classMetrics := ClassMetrics{
		GeneralMetrics:        generalClassMetrics,
		MetricsPercentOfTotal: classMetricsOfTotal,
		RateMetrics:           aMetrics,
		SpecialMetrics:        sMetrics,
	}
	return &classMetrics, sparks
}

// GlobalMetrics include all metrics
type GlobalMetrics struct {
	*GeneralMetrics
	*RateMetrics
	*SpecialMetrics
}

// GetGlobalMetrics return metrics for given instance
func (m *MetricsManager) GetGlobalMetrics(instanceID uint, begin, end time.Time) (*GlobalMetrics, *[]RateMetrics) {
	currentMetricGroup := m.identifyMetricGroup(instanceID, begin, end)
	currentMetricGroup.ServerSummary = true
	endTs := end.Unix()
	intervalTs := (endTs - begin.Unix()) / (amountOfPoints - 1)
	args := args{
		0,
		instanceID,
		begin,
		end,
		endTs,
		intervalTs,
	}

	generalGlobalMetrics := m.getMetrics(currentMetricGroup, args)
	sparks := m.getSparklines(currentMetricGroup, args)

	aMetrics := m.computeRateMetrics(generalGlobalMetrics, begin, end)
	sMetrics := m.computeSpecialMetrics(generalGlobalMetrics)
	globalMetrics := GlobalMetrics{
		generalGlobalMetrics,
		aMetrics,
		sMetrics,
	}
	return &globalMetrics, sparks
}

func (m *MetricsManager) getMetrics(group *MetricGroup, args args) *GeneralMetrics {
	var queryClassMetricsBuffer bytes.Buffer
	if tmpl, err := template.New("queryClassMetricsSQL").Parse(queryClassMetricsTemplate); err != nil {
		log.Fatalln(err)
	} else if err = tmpl.Execute(&queryClassMetricsBuffer, group); err != nil {
		log.Fatalln(err)
	}

	queryClassMetricsSQL := queryClassMetricsBuffer.String()
	gMetrics := GeneralMetrics{}
	if nstmt, err := m.conns.ClickHouse.PrepareNamed(queryClassMetricsSQL); err != nil {
		log.Fatalln(err)
	} else if err = nstmt.Get(&gMetrics, args); err != nil {
		log.Fatalln(err)
	}

	return &gMetrics
}

const amountOfPoints = 60

func (m *MetricsManager) getSparklines(group *MetricGroup, args args) *[]RateMetrics {
	var querySparklinesBuffer bytes.Buffer
	if tmpl, err := template.New("querySparklinesSQL").Parse(querySparklinesTemplate); err != nil {
		log.Fatalln(err)
	} else if err = tmpl.Execute(&querySparklinesBuffer, group); err != nil {
		log.Fatalln(err)
	}
	querySparklinesSQL := querySparklinesBuffer.String()
	var sparksWithGaps []RateMetrics

	if nstmt, err := m.conns.ClickHouse.PrepareNamed(querySparklinesSQL); err != nil {
		log.Fatalln(err)
	} else if err = nstmt.Select(&sparksWithGaps, args); err != nil {
		log.Fatalln(err)
	}
	metricLogRaw := make(map[int64]RateMetrics)

	for i := range sparksWithGaps {
		key := sparksWithGaps[i].Ts.Unix()
		metricLogRaw[key] = sparksWithGaps[i]
	}

	// fills up gaps in sparklines by zero values
	var sparks []RateMetrics
	var pointN int64
	for pointN = 0; pointN < amountOfPoints; pointN++ {
		ts := args.EndTS - pointN*args.IntervalTS
		if val, ok := metricLogRaw[ts]; ok {
			sparks = append(sparks, val)
		} else {
			val := RateMetrics{Point: pointN, Ts: time.Unix(ts, 0).UTC()}
			sparks = append(sparks, val)
		}
	}
	return &sparks
}

func (m *MetricsManager) computeOfTotal(classMetrics, globalMetrics *GeneralMetrics) *MetricsPercentOfTotal {
	mPercentOfTotal := MetricsPercentOfTotal{}
	reflectPercentOfTotal := reflect.ValueOf(&mPercentOfTotal).Elem()
	reflectClassMetrics := reflect.ValueOf(classMetrics).Elem()
	reflectGlobalMetrics := reflect.ValueOf(globalMetrics).Elem()

	for i := 0; i < reflectPercentOfTotal.NumField(); i++ {
		fieldName := reflectPercentOfTotal.Type().Field(i).Name
		classVal := reflectClassMetrics.FieldByName(strings.TrimSuffix(fieldName, "_of_total")).Float()
		totalVal := reflectGlobalMetrics.FieldByName(strings.TrimSuffix(fieldName, "_of_total")).Float()
		var n float64
		if totalVal > 0 {
			n = classVal / totalVal
		}
		reflectPercentOfTotal.FieldByName(fieldName).SetFloat(n)
	}
	return &mPercentOfTotal
}

func (m *MetricsManager) computeRateMetrics(gMetrics *GeneralMetrics, begin, end time.Time) *RateMetrics {
	duration := end.Sub(begin).Seconds()
	aMetrics := RateMetrics{}
	reflectionAdittionalMetrics := reflect.ValueOf(&aMetrics).Elem()
	reflectionGeneralMetrics := reflect.ValueOf(gMetrics).Elem()

	for i := 0; i < reflectionAdittionalMetrics.NumField(); i++ {
		fieldName := reflectionAdittionalMetrics.Type().Field(i).Name
		if strings.HasSuffix(fieldName, "_per_sec") {
			generalFieldName := strings.TrimSuffix(fieldName, "_per_sec")
			metricVal := reflectionGeneralMetrics.FieldByName(generalFieldName).Float()

			reflectionAdittionalMetrics.FieldByName(fieldName).SetFloat(metricVal / duration)
		}
	}
	return &aMetrics
}

func (m *MetricsManager) computeSpecialMetrics(gMetrics *GeneralMetrics) *SpecialMetrics {
	sMetrics := SpecialMetrics{}
	reflectionSpecialMetrics := reflect.ValueOf(&sMetrics).Elem()
	reflectionGeneralMetrics := reflect.ValueOf(gMetrics).Elem()

	for i := 0; i < reflectionSpecialMetrics.NumField(); i++ {
		field := reflectionSpecialMetrics.Type().Field(i)
		fieldName := field.Name
		fieldTag := field.Tag.Get("divider")
		generalFieldName := strings.Split(fieldName, "_per_")[0]
		dividend := reflectionGeneralMetrics.FieldByName(generalFieldName).Float()
		divider := reflectionGeneralMetrics.FieldByName(fieldTag).Float()
		if divider == 0 {
			continue
		}
		reflectionSpecialMetrics.FieldByName(fieldName).SetFloat(dividend / divider)
	}
	return &sMetrics
}

// SpecialMetrics specific metrics
type SpecialMetrics struct {
	Lock_time_avg_per_query_time                 float32 `json:",omitempty" divider:"Query_time_avg"`
	InnoDB_rec_lock_wait_avg_per_query_time      float32 `json:",omitempty" divider:"Query_time_avg"`
	InnoDB_IO_r_wait_avg_per_query_time          float32 `json:",omitempty" divider:"Query_time_avg"`
	InnoDB_queue_wait_avg_per_query_time         float32 `json:",omitempty" divider:"Query_time_avg"`
	InnoDB_IO_r_bytes_sum_per_io                 float32 `json:",omitempty" divider:"InnoDB_IO_r_ops_sum"`
	QC_Hit_sum_per_query                         float32 `json:",omitempty" divider:"Query_count"`
	Bytes_sent_sum_per_rows                      float32 `json:",omitempty" divider:"Rows_sent_sum"`
	Rows_examined_sum_per_rows                   float32 `json:",omitempty" divider:"Rows_sent_sum"`
	Filesort_sum_per_query                       float32 `json:",omitempty" divider:"Query_count"`
	Filesort_on_disk_sum_per_query               float32 `json:",omitempty" divider:"Query_count"`
	Merge_passes_sum_per_external_sort           float32 `json:",omitempty" divider:"Filesort_sum"`
	Full_join_sum_per_query                      float32 `json:",omitempty" divider:"Query_count"`
	Full_scan_sum_per_query                      float32 `json:",omitempty" divider:"Query_count"`
	Tmp_table_sum_per_query                      float32 `json:",omitempty" divider:"Query_count"`
	Tmp_tables_sum_per_query_with_tmp_table      float32 `json:",omitempty" divider:"Tmp_table_sum"`
	Tmp_table_on_disk_sum_per_query              float32 `json:",omitempty" divider:"Query_count"`
	Tmp_disk_tables_sum_per_query_with_tmp_table float32 `json:",omitempty" divider:"Tmp_table_on_disk_sum"`
	Tmp_table_sizes_sum_per_query                float32 `json:",omitempty" divider:"Query_count"`
}

// RateMetrics is metrics divided by time period
type RateMetrics struct {
	Point                            int64
	Ts                               time.Time
	Query_count_per_sec              float32 `json:",omitempty"`
	Query_time_sum_per_sec           float32 `json:",omitempty"`
	Lock_time_sum_per_sec            float32 `json:",omitempty"` // load
	InnoDB_rec_lock_wait_sum_per_sec float32 `json:",omitempty"` // load
	InnoDB_IO_r_wait_sum_per_sec     float32 `json:",omitempty"` // load
	InnoDB_IO_r_ops_sum_per_sec      float32 `json:",omitempty"`
	InnoDB_IO_r_bytes_sum_per_sec    float32 `json:",omitempty"`
	InnoDB_queue_wait_sum_per_sec    float32 `json:",omitempty"` // load

	QC_Hit_sum_per_sec            float32 `json:",omitempty"`
	Rows_sent_sum_per_sec         float32 `json:",omitempty"`
	Bytes_sent_sum_per_sec        float32 `json:",omitempty"`
	Rows_examined_sum_per_sec     float32 `json:",omitempty"`
	Rows_affected_sum_per_sec     float32 `json:",omitempty"`
	Filesort_sum_per_sec          float32 `json:",omitempty"`
	Filesort_on_disk_sum_per_sec  float32 `json:",omitempty"`
	Merge_passes_sum_per_sec      float32 `json:",omitempty"`
	Full_join_sum_per_sec         float32 `json:",omitempty"`
	Full_scan_sum_per_sec         float32 `json:",omitempty"`
	Tmp_table_sum_per_sec         float32 `json:",omitempty"`
	Tmp_tables_sum_per_sec        float32 `json:",omitempty"`
	Tmp_table_on_disk_sum_per_sec float32 `json:",omitempty"`
	Tmp_disk_tables_sum_per_sec   float32 `json:",omitempty"`
	Tmp_table_sizes_sum_per_sec   float32 `json:",omitempty"`

	/* Perf Schema */

	Errors_sum_per_sec                 float32 `json:",omitempty"`
	Warnings_sum_per_sec               float32 `json:",omitempty"`
	Select_full_range_join_sum_per_sec float32 `json:",omitempty"`
	Select_range_sum_per_sec           float32 `json:",omitempty"`
	Select_range_check_sum_per_sec     float32 `json:",omitempty"`
	Sort_range_sum_per_sec             float32 `json:",omitempty"`
	Sort_rows_sum_per_sec              float32 `json:",omitempty"`
	Sort_scan_sum_per_sec              float32 `json:",omitempty"`
	No_index_used_sum_per_sec          float32 `json:",omitempty"`
	No_good_index_used_sum_per_sec     float32 `json:",omitempty"`
}

type MetricsPercentOfTotal struct {
	Query_count_of_total       float32
	Query_time_sum_of_total    float32
	Lock_time_sum_of_total     float32
	Rows_sent_sum_of_total     float32
	Rows_examined_sum_of_total float32
	// 5
	/* Perf Schema or Percona Server */

	Rows_affected_sum_of_total     float32 `json:",omitempty"`
	Bytes_sent_sum_of_total        float32 `json:",omitempty"`
	Tmp_tables_sum_of_total        float32 `json:",omitempty"`
	Tmp_disk_tables_sum_of_total   float32 `json:",omitempty"`
	Tmp_table_sizes_sum_of_total   float32 `json:",omitempty"`
	QC_Hit_sum_of_total            float32 `json:",omitempty"`
	Full_scan_sum_of_total         float32 `json:",omitempty"`
	Full_join_sum_of_total         float32 `json:",omitempty"`
	Tmp_table_sum_of_total         float32 `json:",omitempty"`
	Tmp_table_on_disk_sum_of_total float32 `json:",omitempty"`
	Filesort_sum_of_total          float32 `json:",omitempty"`
	Filesort_on_disk_sum_of_total  float32 `json:",omitempty"`
	Merge_passes_sum_of_total      float32 `json:",omitempty"`
	// 13
	/* Percona Server */

	InnoDB_IO_r_ops_sum_of_total       float32 `json:",omitempty"`
	InnoDB_IO_r_bytes_sum_of_total     float32 `json:",omitempty"`
	InnoDB_IO_r_wait_sum_of_total      float32 `json:",omitempty"`
	InnoDB_rec_lock_wait_sum_of_total  float32 `json:",omitempty"`
	InnoDB_queue_wait_sum_of_total     float32 `json:",omitempty"`
	InnoDB_pages_distinct_sum_of_total float32 `json:",omitempty"`
	// 6
	/* Perf Schema */

	Errors_sum_of_total                 float32 `json:",omitempty"`
	Warnings_sum_of_total               float32 `json:",omitempty"`
	Select_full_range_join_sum_of_total float32 `json:",omitempty"`
	Select_range_sum_of_total           float32 `json:",omitempty"`
	Select_range_check_sum_of_total     float32 `json:",omitempty"`
	Sort_range_sum_of_total             float32 `json:",omitempty"`
	Sort_rows_sum_of_total              float32 `json:",omitempty"`
	Sort_scan_sum_of_total              float32 `json:",omitempty"`
	No_index_used_sum_of_total          float32 `json:",omitempty"`
	No_good_index_used_sum_of_total     float32 `json:",omitempty"`
	// 10
}

// GeneralMetrics is common metrics for usual query classes
// 34
type GeneralMetrics struct {

	/*  Basic metrics */

	Query_count       float32 // lint: ignore
	Query_time_sum    float32
	Query_time_min    float32
	Query_time_avg    float32
	Query_time_med    float32
	Query_time_p95    float32
	Query_time_max    float32
	Lock_time_sum     float32
	Lock_time_min     float32
	Lock_time_avg     float32
	Lock_time_med     float32
	Lock_time_p95     float32
	Lock_time_max     float32
	Rows_sent_sum     float32
	Rows_sent_min     float32
	Rows_sent_avg     float32
	Rows_sent_med     float32
	Rows_sent_p95     float32
	Rows_sent_max     float32
	Rows_examined_sum float32
	Rows_examined_min float32
	Rows_examined_avg float32
	Rows_examined_med float32
	Rows_examined_p95 float32
	Rows_examined_max float32

	/* Perf Schema or Percona Server */

	Rows_affected_sum     float32 `json:",omitempty"`
	Rows_affected_min     float32 `json:",omitempty"`
	Rows_affected_avg     float32 `json:",omitempty"`
	Rows_affected_med     float32 `json:",omitempty"`
	Rows_affected_p95     float32 `json:",omitempty"`
	Rows_affected_max     float32 `json:",omitempty"`
	Bytes_sent_sum        float32 `json:",omitempty"`
	Bytes_sent_min        float32 `json:",omitempty"`
	Bytes_sent_avg        float32 `json:",omitempty"`
	Bytes_sent_med        float32 `json:",omitempty"`
	Bytes_sent_p95        float32 `json:",omitempty"`
	Bytes_sent_max        float32 `json:",omitempty"`
	Tmp_tables_sum        float32 `json:",omitempty"`
	Tmp_tables_min        float32 `json:",omitempty"`
	Tmp_tables_avg        float32 `json:",omitempty"`
	Tmp_tables_med        float32 `json:",omitempty"`
	Tmp_tables_p95        float32 `json:",omitempty"`
	Tmp_tables_max        float32 `json:",omitempty"`
	Tmp_disk_tables_sum   float32 `json:",omitempty"`
	Tmp_disk_tables_min   float32 `json:",omitempty"`
	Tmp_disk_tables_avg   float32 `json:",omitempty"`
	Tmp_disk_tables_med   float32 `json:",omitempty"`
	Tmp_disk_tables_p95   float32 `json:",omitempty"`
	Tmp_disk_tables_max   float32 `json:",omitempty"`
	Tmp_table_sizes_sum   float32 `json:",omitempty"`
	Tmp_table_sizes_min   float32 `json:",omitempty"`
	Tmp_table_sizes_avg   float32 `json:",omitempty"`
	Tmp_table_sizes_med   float32 `json:",omitempty"`
	Tmp_table_sizes_p95   float32 `json:",omitempty"`
	Tmp_table_sizes_max   float32 `json:",omitempty"`
	QC_Hit_sum            float32 `json:",omitempty"`
	Full_scan_sum         float32 `json:",omitempty"`
	Full_join_sum         float32 `json:",omitempty"`
	Tmp_table_sum         float32 `json:",omitempty"`
	Tmp_table_on_disk_sum float32 `json:",omitempty"`
	Filesort_sum          float32 `json:",omitempty"`
	Filesort_on_disk_sum  float32 `json:",omitempty"`
	Merge_passes_sum      float32 `json:",omitempty"`
	Merge_passes_min      float32 `json:",omitempty"`
	Merge_passes_avg      float32 `json:",omitempty"`
	Merge_passes_med      float32 `json:",omitempty"`
	Merge_passes_p95      float32 `json:",omitempty"`
	Merge_passes_max      float32 `json:",omitempty"`

	/* Percona Server */

	InnoDB_IO_r_ops_sum       float32 `json:",omitempty"`
	InnoDB_IO_r_ops_min       float32 `json:",omitempty"`
	InnoDB_IO_r_ops_avg       float32 `json:",omitempty"`
	InnoDB_IO_r_ops_med       float32 `json:",omitempty"`
	InnoDB_IO_r_ops_p95       float32 `json:",omitempty"`
	InnoDB_IO_r_ops_max       float32 `json:",omitempty"`
	InnoDB_IO_r_bytes_sum     float32 `json:",omitempty"`
	InnoDB_IO_r_bytes_min     float32 `json:",omitempty"`
	InnoDB_IO_r_bytes_avg     float32 `json:",omitempty"`
	InnoDB_IO_r_bytes_med     float32 `json:",omitempty"`
	InnoDB_IO_r_bytes_p95     float32 `json:",omitempty"`
	InnoDB_IO_r_bytes_max     float32 `json:",omitempty"`
	InnoDB_IO_r_wait_sum      float32 `json:",omitempty"`
	InnoDB_IO_r_wait_min      float32 `json:",omitempty"`
	InnoDB_IO_r_wait_avg      float32 `json:",omitempty"`
	InnoDB_IO_r_wait_med      float32 `json:",omitempty"`
	InnoDB_IO_r_wait_p95      float32 `json:",omitempty"`
	InnoDB_IO_r_wait_max      float32 `json:",omitempty"`
	InnoDB_rec_lock_wait_sum  float32 `json:",omitempty"`
	InnoDB_rec_lock_wait_min  float32 `json:",omitempty"`
	InnoDB_rec_lock_wait_avg  float32 `json:",omitempty"`
	InnoDB_rec_lock_wait_med  float32 `json:",omitempty"`
	InnoDB_rec_lock_wait_p95  float32 `json:",omitempty"`
	InnoDB_rec_lock_wait_max  float32 `json:",omitempty"`
	InnoDB_queue_wait_sum     float32 `json:",omitempty"`
	InnoDB_queue_wait_min     float32 `json:",omitempty"`
	InnoDB_queue_wait_avg     float32 `json:",omitempty"`
	InnoDB_queue_wait_med     float32 `json:",omitempty"`
	InnoDB_queue_wait_p95     float32 `json:",omitempty"`
	InnoDB_queue_wait_max     float32 `json:",omitempty"`
	InnoDB_pages_distinct_sum float32 `json:",omitempty"`
	InnoDB_pages_distinct_min float32 `json:",omitempty"`
	InnoDB_pages_distinct_avg float32 `json:",omitempty"`
	InnoDB_pages_distinct_med float32 `json:",omitempty"`
	InnoDB_pages_distinct_p95 float32 `json:",omitempty"`
	InnoDB_pages_distinct_max float32 `json:",omitempty"`

	/* Perf Schema */

	Errors_sum                 float32 `json:",omitempty"`
	Warnings_sum               float32 `json:",omitempty"`
	Select_full_range_join_sum float32 `json:",omitempty"`
	Select_range_sum           float32 `json:",omitempty"`
	Select_range_check_sum     float32 `json:",omitempty"`
	Sort_range_sum             float32 `json:",omitempty"`
	Sort_rows_sum              float32 `json:",omitempty"`
	Sort_scan_sum              float32 `json:",omitempty"`
	No_index_used_sum          float32 `json:",omitempty"`
	No_good_index_used_sum     float32 `json:",omitempty"`
}

const queryClassMetricsTemplate = `
SELECT

	{{ if .Basic }}
	/*  Basic metrics */

	SUM(query_count) AS query_count,
	SUM(Query_time_sum) AS query_time_sum,
	MIN(Query_time_min) AS query_time_min,
	AVG(Query_time_avg) AS query_time_avg,
	AVG(Query_time_med) AS query_time_med,
	AVG(Query_time_p95) AS query_time_p95,
	MAX(Query_time_max) AS query_time_max,
	SUM(Lock_time_sum) AS lock_time_sum,
	MIN(Lock_time_min) AS lock_time_min,
	AVG(Lock_time_avg) AS lock_time_avg,
	AVG(Lock_time_med) AS lock_time_med,
	AVG(Lock_time_p95) AS lock_time_p95,
	MAX(Lock_time_max) AS lock_time_max,
	SUM(Rows_sent_sum) AS rows_sent_sum,
	MIN(Rows_sent_min) AS rows_sent_min,
	AVG(Rows_sent_avg) AS rows_sent_avg,
	AVG(Rows_sent_med) AS rows_sent_med,
	AVG(Rows_sent_p95) AS rows_sent_p95,
	MAX(Rows_sent_max) AS rows_sent_max,
	SUM(Rows_examined_sum) AS rows_examined_sum,
	MIN(Rows_examined_min) AS rows_examined_min,
	AVG(Rows_examined_avg) AS rows_examined_avg,
	AVG(Rows_examined_med) AS rows_examined_med,
	AVG(Rows_examined_p95) AS rows_examined_p95,
	MAX(Rows_examined_max) AS rows_examined_max

	{{ end }}

	{{ if or .PerconaServer .PerformanceSchema }}
	/* Perf Schema or Percona Server */

	, /* <-- final comma for basic metrics */

	SUM(Rows_affected_sum) AS rows_affected_sum,
	MIN(Rows_affected_min) AS rows_affected_min,
	AVG(Rows_affected_avg) AS rows_affected_avg,
	AVG(Rows_affected_med) AS rows_affected_med,
	AVG(Rows_affected_p95) AS rows_affected_p95,
	MAX(Rows_affected_max) AS rows_affected_max,

	SUM(Full_scan_sum) AS full_scan_sum,
	SUM(Full_join_sum) AS full_join_sum,
	SUM(Tmp_table_sum) AS tmp_table_sum,
	SUM(Tmp_table_on_disk_sum) AS tmp_table_on_disk_sum,

	SUM(Merge_passes_sum) AS merge_passes_sum,
	MIN(Merge_passes_min) AS merge_passes_min,
	AVG(Merge_passes_avg) AS merge_passes_avg,
	AVG(Merge_passes_med) AS merge_passes_med,
	AVG(Merge_passes_p95) AS merge_passes_p95,
	MAX(Merge_passes_max) AS merge_passes_max,

	{{ end }}

	{{ if .PerconaServer }}
	/* Percona Server */

	SUM(Bytes_sent_sum) AS bytes_sent_sum,
	MIN(Bytes_sent_min) AS bytes_sent_min,
	AVG(Bytes_sent_avg) AS bytes_sent_avg,
	AVG(Bytes_sent_med) AS bytes_sent_med,
	AVG(Bytes_sent_p95) AS bytes_sent_p95,
	MAX(Bytes_sent_max) AS bytes_sent_max,
	SUM(Tmp_tables_sum) AS tmp_tables_sum,
	MIN(Tmp_tables_min) AS tmp_tables_min,
	AVG(Tmp_tables_avg) AS tmp_tables_avg,
	AVG(Tmp_tables_med) AS tmp_tables_med,
	AVG(Tmp_tables_p95) AS tmp_tables_p95,
	MAX(Tmp_tables_max) AS tmp_tables_max,
	SUM(Tmp_disk_tables_sum) AS tmp_disk_tables_sum,
	MIN(Tmp_disk_tables_min) AS tmp_disk_tables_min,
	AVG(Tmp_disk_tables_avg) AS tmp_disk_tables_avg,
	AVG(Tmp_disk_tables_med) AS tmp_disk_tables_med,
	AVG(Tmp_disk_tables_p95) AS tmp_disk_tables_p95,
	MAX(Tmp_disk_tables_max) AS tmp_disk_tables_max,
	SUM(Tmp_table_sizes_sum) AS tmp_table_sizes_sum,
	MIN(Tmp_table_sizes_min) AS tmp_table_sizes_min,
	AVG(Tmp_table_sizes_avg) AS tmp_table_sizes_avg,
	AVG(Tmp_table_sizes_med) AS tmp_table_sizes_med,
	AVG(Tmp_table_sizes_p95) AS tmp_table_sizes_p95,
	MAX(Tmp_table_sizes_max) AS tmp_table_sizes_max,

	SUM(QC_Hit_sum) AS qc_hit_sum,
	SUM(Filesort_sum) AS filesort_sum,

	SUM(Filesort_on_disk_sum) AS filesort_on_disk_sum,
	SUM(InnoDB_IO_r_ops_sum) AS innodb_io_r_ops_sum,
	MIN(InnoDB_IO_r_ops_min) AS innodb_io_r_ops_min,
	AVG(InnoDB_IO_r_ops_avg) AS innodb_io_r_ops_avg,
	AVG(InnoDB_IO_r_ops_med) AS innodb_io_r_ops_med,
	AVG(InnoDB_IO_r_ops_p95) AS innodb_io_r_ops_p95,
	MAX(InnoDB_IO_r_ops_max) AS innodb_io_r_ops_max,
	SUM(InnoDB_IO_r_bytes_sum) AS innodb_io_r_bytes_sum,
	MIN(InnoDB_IO_r_bytes_min) AS innodb_io_r_bytes_min,
	AVG(InnoDB_IO_r_bytes_avg) AS innodb_io_r_bytes_avg,
	AVG(InnoDB_IO_r_bytes_med) AS innodb_io_r_bytes_med,
	AVG(InnoDB_IO_r_bytes_p95) AS innodb_io_r_bytes_p95,
	MAX(InnoDB_IO_r_bytes_max) AS innodb_io_r_bytes_max,
	SUM(InnoDB_IO_r_wait_sum) AS innodb_io_r_wait_sum,
	MIN(InnoDB_IO_r_wait_min) AS innodb_io_r_wait_min,
	AVG(InnoDB_IO_r_wait_avg) AS innodb_io_r_wait_avg,
	AVG(InnoDB_IO_r_wait_med) AS innodb_io_r_wait_med,
	AVG(InnoDB_IO_r_wait_p95) AS innodb_io_r_wait_p95,
	MAX(InnoDB_IO_r_wait_max) AS innodb_io_r_wait_max,
	SUM(InnoDB_rec_lock_wait_sum) AS innodb_rec_lock_wait_sum,
	MIN(InnoDB_rec_lock_wait_min) AS innodb_rec_lock_wait_min,
	AVG(InnoDB_rec_lock_wait_avg) AS innodb_rec_lock_wait_avg,
	AVG(InnoDB_rec_lock_wait_med) AS innodb_rec_lock_wait_med,
	AVG(InnoDB_rec_lock_wait_p95) AS innodb_rec_lock_wait_p95,
	MAX(InnoDB_rec_lock_wait_max) AS innodb_rec_lock_wait_max,
	SUM(InnoDB_queue_wait_sum) AS innodb_queue_wait_sum,
	MIN(InnoDB_queue_wait_min) AS innodb_queue_wait_min,
	AVG(InnoDB_queue_wait_avg) AS innodb_queue_wait_avg,
	AVG(InnoDB_queue_wait_med) AS innodb_queue_wait_med,
	AVG(InnoDB_queue_wait_p95) AS innodb_queue_wait_p95,
	MAX(InnoDB_queue_wait_max) AS innodb_queue_wait_max,
	SUM(InnoDB_pages_distinct_sum) AS innodb_pages_distinct_sum,
	MIN(InnoDB_pages_distinct_min) AS innodb_pages_distinct_min,
	AVG(InnoDB_pages_distinct_avg) AS innodb_pages_distinct_avg,
	AVG(InnoDB_pages_distinct_med) AS innodb_pages_distinct_med,
	AVG(InnoDB_pages_distinct_p95) AS innodb_pages_distinct_p95,
	MAX(InnoDB_pages_distinct_max) AS innodb_pages_distinct_max
	{{ end }}

	{{ if .PerformanceSchema }}
	/* Perf Schema */

	SUM(Errors_sum) AS errors_sum,
	SUM(Warnings_sum) AS warnings_sum,
	SUM(Select_full_range_join_sum) AS select_full_range_join_sum,
	SUM(Select_range_sum) AS select_range_sum,
	SUM(Select_range_check_sum) AS select_range_check_sum,
	SUM(Sort_range_sum) AS sort_range_sum,
	SUM(Sort_rows_sum) AS sort_rows_sum,
	SUM(Sort_scan_sum) AS sort_scan_sum,
	SUM(No_index_used_sum) AS no_index_used_sum,
	SUM(No_good_index_used_sum) AS no_good_index_used_sum
	{{ end }}

FROM query_class_metrics
WHERE {{if not .ServerSummary }} query_class_id = :class_id AND {{ end }}
	 instance_id = :instance_id AND (start_ts >= :begin AND start_ts < :end);
`

// (:interval_ts) - must be in brackets to be correctly parsed with PrepareNamed
const querySparklinesTemplate = `
SELECT
	intDiv((:end_ts - toRelativeSecondNum(start_ts)), :interval_ts) as point,
	toDateTime(:end_ts - point * (:interval_ts)) AS ts,

	/*  Basic metrics */
	SUM(query_count) / (:interval_ts) AS query_count_per_sec,
	SUM(Query_time_sum) / (:interval_ts) AS query_time_sum_per_sec,
	SUM(Lock_time_sum) / (:interval_ts) AS lock_time_sum_per_sec,
	SUM(Rows_sent_sum) / (:interval_ts) AS rows_sent_sum_per_sec,
	SUM(Rows_examined_sum) / (:interval_ts) AS rows_examined_sum_per_sec,

	/* Perf Schema or Percona Server */
	SUM(Rows_affected_sum) / (:interval_ts) AS rows_affected_sum_per_sec,
	SUM(Merge_passes_sum) / (:interval_ts) AS merge_passes_sum_per_sec,
	SUM(Full_join_sum) / (:interval_ts) AS full_join_sum_per_sec,
	SUM(Full_scan_sum) / (:interval_ts) AS full_scan_sum_per_sec,
	SUM(Tmp_table_sum) / (:interval_ts) AS tmp_table_sum_per_sec,
	SUM(Tmp_table_on_disk_sum) / (:interval_ts) AS tmp_table_on_disk_sum_per_sec,

	/* Percona Server */
	SUM(Bytes_sent_sum) / (:interval_ts) AS bytes_sent_sum_per_sec,
	SUM(InnoDB_IO_r_ops_sum) / (:interval_ts) AS innodb_io_r_ops_sum_per_sec,

	SUM(InnoDB_IO_r_wait_sum) / (:interval_ts) AS innodb_io_r_wait_sum_per_sec,
	SUM(InnoDB_rec_lock_wait_sum) / (:interval_ts) AS innodb_rec_lock_wait_sum_per_sec,
	SUM(InnoDB_queue_wait_sum) / (:interval_ts) AS innodb_queue_wait_sum_per_sec,

	SUM(InnoDB_IO_r_bytes_sum) / (:interval_ts) AS innodb_io_r_bytes_sum_per_sec,
	SUM(QC_Hit_sum) / (:interval_ts) AS qc_hit_sum_per_sec,
	SUM(Filesort_sum) / (:interval_ts) AS filesort_sum_per_sec,
	SUM(Filesort_on_disk_sum) / (:interval_ts) AS filesort_on_disk_sum_per_sec,
	SUM(Tmp_tables_sum) / (:interval_ts) AS tmp_tables_sum_per_sec,
	SUM(Tmp_disk_tables_sum) / (:interval_ts) AS tmp_disk_tables_sum_per_sec,
	SUM(Tmp_table_sizes_sum) / (:interval_ts) AS tmp_table_sizes_sum_per_sec

FROM query_class_metrics
WHERE {{if not .ServerSummary }} query_class_id = :class_id AND {{ end }}
    instance_id = :instance_id AND (start_ts >= :begin AND start_ts < :end)
GROUP BY point;
`
