package qan

import (
	"encoding/json"
	"strings"

	"github.com/shatteredsilicon/qan-api/app/instance"
	"github.com/shatteredsilicon/ssm/proto"
)

// ExtractIndexesFromExplain extracts indexes from a EXPLAIN output,
// it returns [][4]string as result, where [4]string meaning [catalog, schema, table, index].
func ExtractIndexesFromExplain(subsystem, db string, explainResult proto.ExplainResult) ([][4]string, error) {
	var indexes [][4]string

	if subsystem == instance.SubsystemNameMySQL {
		for _, row := range explainResult.Classic {
			if row.Key.String == "" {
				continue
			}

			dbTable := strings.SplitN(row.Table.String, ".", 2)
			schema, table := db, dbTable[len(dbTable)-1]
			if len(dbTable) == 2 {
				schema = dbTable[0]
			}
			indexes = append(indexes, [4]string{"", schema, table, row.Key.String})
		}
	} else if explainResult.JSON != "" && subsystem == instance.SubsystemNamePostgreSQL {
		var plans []struct{ Plan map[string]interface{} }
		if err := json.Unmarshal([]byte(explainResult.JSON), &plans); err != nil {
			return nil, err
		}

		for i := 0; i < len(plans); i++ {
			indexes = append(indexes, extractIndexesFromPgPlanNode(db, "", "", plans[i].Plan)...)
		}
	} else if explainResult.JSON != "" && subsystem == instance.SubsystemNameMongo {
		var plan struct {
			QueryPlanner map[string]interface{}   `json:"queryPlanner"`
			Stages       []map[string]interface{} `json:"stages"`
		}
		if err := json.Unmarshal([]byte(explainResult.JSON), &plan); err != nil {
			return nil, err
		}

		indexes = append(indexes, extractIndexesFromMongoPlan("", db, plan.QueryPlanner)...)
		for _, stage := range plan.Stages {
			indexes = append(indexes, extractIndexesFromMongoPlan("", db, stage)...)
		}
	}

	return indexes, nil
}

func extractIndexesFromPgPlanNode(catalog, schema, table string, plan map[string]interface{}) [][4]string {
	var indexes [][4]string

	if plan == nil || plan["Node Type"] == nil {
		return nil
	}

	if plan["Schema"] != nil {
		if s, ok := plan["Schema"].(string); ok {
			schema = s
		}
	}
	if plan["Relation Name"] != nil {
		if t, ok := plan["Relation Name"].(string); ok {
			table = t
		}
	}

	if nodeType, ok := plan["Node Type"].(string); ok && (strings.Contains(nodeType, "Index Scan") || strings.Contains(nodeType, "Index Only Scan")) && plan["Index Name"] != nil {
		if i, ok := plan["Index Name"].(string); ok {
			indexes = append(indexes, [4]string{catalog, schema, table, i})
		}
	}

	if plan["Plans"] == nil {
		return indexes
	} else if plans, ok := plan["Plans"].([]interface{}); ok {
		for i := range plans {
			if p, ok := plans[i].(map[string]interface{}); ok {
				indexes = append(indexes, extractIndexesFromPgPlanNode(catalog, schema, table, p)...)
			}
		}
	}

	return indexes
}

func extractIndexesFromMongoPlan(db, collection string, plan map[string]interface{}) [][4]string {
	var indexes [][4]string

	if plan == nil {
		return indexes
	}

	if plan["namespace"] != nil {
		if namespace, ok := plan["namespace"].(string); ok {
			dbCollection := strings.SplitN(namespace, ".", 2)
			collection = dbCollection[len(dbCollection)-1]
			if len(dbCollection) == 2 {
				db = dbCollection[0]
			}
		}
	}

	for key, val := range plan {
		switch key {
		case "indexName":
			if indexName, ok := val.(string); ok {
				indexes = append(indexes, [4]string{"", db, collection, indexName})
			}
		default:
			arr, ok := val.([]interface{})
			if !ok {
				arr = append(arr, val)
			}
			for _, item := range arr {
				if dict, ok := item.(map[string]interface{}); ok {
					indexes = append(indexes, extractIndexesFromMongoPlan(db, collection, dict)...)
				}
			}
		}
	}

	return indexes
}
