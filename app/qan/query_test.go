package qan_test

import (
	"slices"
	"testing"

	"github.com/shatteredsilicon/qan-api/app/instance"
	"github.com/shatteredsilicon/qan-api/app/qan"
	"github.com/shatteredsilicon/ssm/proto"
)

var extractIndexesFromExplainTests = map[string][]struct {
	db      string
	explain proto.ExplainResult
	indexes [][4]string
}{
	instance.SubsystemNamePostgreSQL: {
		// 1. Simple Primary Key Lookup (Index Scan)
		{
			db: "production_db",
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Index Scan",
      "Parallel Aware": false,
      "Async Capable": false,
      "Scan Direction": "Forward",
      "Index Name": "users_pkey",
      "Relation Name": "UserProfile",
      "Schema": "tenant-4812",
      "Alias": "UserProfile",
      "Startup Cost": 0.28,
      "Total Cost": 8.29,
      "Plan Rows": 1,
      "Plan Width": 244,
      "Output": ["id", "email", "\"first name\"", "\"last name\"", "created_at"],
      "Index Cond": "(\"UserProfile\".id = 1)"
    }
  }
]`,
			},
			indexes: [][4]string{
				{"production_db", "tenant-4812", "UserProfile", "users_pkey"},
			},
		},

		// 2. Secondary Non-Unique Index Scan (Bitmap Index Scan)
		{
			db: "ecommerce_warehouse",
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Bitmap Heap Scan",
      "Parallel Aware": false,
      "Async Capable": false,
      "Relation Name": "products",
      "Schema": "inventory",
      "Alias": "products",
      "Startup Cost": 4.42,
      "Total Cost": 15.30,
      "Plan Rows": 15,
      "Plan Width": 64,
      "Output": ["id", "name", "\"categoryId\"", "price"],
      "Recheck Cond": "(products.\"categoryId\" = 42)",
      "Plans": [
        {
          "Node Type": "Bitmap Index Scan",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Async Capable": false,
          "Index Name": "idx_products_cat",
          "Startup Cost": 0.00,
          "Total Cost": 4.42,
          "Plan Rows": 15,
          "Plan Width": 0,
          "Index Cond": "(products.\"categoryId\" = 42)"
        }
      ]
    }
  }
]`,
			},
			indexes: [][4]string{
				{"ecommerce_warehouse", "inventory", "products", "idx_products_cat"},
			},
		},

		// 3. Index Range Scan
		{
			db: "telemetry_store",
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Index Scan",
      "Parallel Aware": false,
      "Async Capable": false,
      "Scan Direction": "Forward",
      "Index Name": "idx_logs_created",
      "Relation Name": "AppLogs",
      "Schema": "sys-logging",
      "Alias": "AppLogs",
      "Startup Cost": 0.42,
      "Total Cost": 45.10,
      "Plan Rows": 250,
      "Plan Width": 128,
      "Output": ["id", "message", "level", "created_at"],
      "Index Cond": "(\"AppLogs\".created_at >= '2026-01-01 00:00:00'::timestamp without time zone)"
    }
  }
]`,
			},
			indexes: [][4]string{
				{"telemetry_store", "sys-logging", "AppLogs", "idx_logs_created"},
			},
		},

		// 4. Index Only Scan (Covering Index / No Heap Fetch)
		{
			db: "auth_authority",
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Index Only Scan",
      "Parallel Aware": false,
      "Async Capable": false,
      "Scan Direction": "Forward",
      "Index Name": "idx_users_email",
      "Relation Name": "identities$v2",
      "Schema": "iam",
      "Alias": "identities$v2",
      "Startup Cost": 0.29,
      "Total Cost": 145.00,
      "Plan Rows": 5000,
      "Plan Width": 32,
      "Output": ["email"],
      "Filter": "(\"identities$v2\".is_active = true)",
      "Heap Fetches": 0
    }
  }
]`,
			},
			indexes: [][4]string{
				{"auth_authority", "iam", "identities$v2", "idx_users_email"},
			},
		},

		// 5. Multi-Table Join (Hash Join)
		{
			db: "cluster_shard_1",
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Hash Join",
      "Parallel Aware": false,
      "Async Capable": false,
      "Join Type": "Inner",
      "Startup Cost": 30.50,
      "Total Cost": 185.40,
      "Plan Rows": 3,
      "Plan Width": 112,
      "Output": ["o.id", "u.name", "i.\"itemName\""],
      "Hash Cond": "(i.order_id = o.id)",
      "Plans": [
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Async Capable": false,
          "Relation Name": "order-items",
          "Schema": "sales",
          "Alias": "i",
          "Startup Cost": 0.00,
          "Total Cost": 140.00,
          "Plan Rows": 3000,
          "Plan Width": 16,
          "Output": ["i.id", "i.order_id", "i.\"itemName\""]
        },
        {
          "Node Type": "Hash",
          "Parent Relationship": "Inner",
          "Parallel Aware": false,
          "Async Capable": false,
          "Startup Cost": 28.00,
          "Total Cost": 28.00,
          "Plan Rows": 200,
          "Plan Width": 96,
          "Output": ["o.id", "u.name"],
          "Plans": [
            {
              "Node Type": "Hash Join",
              "Parallel Aware": false,
              "Async Capable": false,
              "Join Type": "Inner",
              "Startup Cost": 12.50,
              "Total Cost": 28.00,
              "Plan Rows": 1,
              "Plan Width": 96,
              "Output": ["o.id", "u.name"],
              "Hash Cond": "(o.user_id = u.id)",
              "Plans": [
                {
                  "Node Type": "Seq Scan",
                  "Parent Relationship": "Outer",
                  "Parallel Aware": false,
                  "Async Capable": false,
                  "Relation Name": "orders",
                  "Schema": "sales",
                  "Alias": "o",
                  "Startup Cost": 0.00,
                  "Total Cost": 14.50,
                  "Plan Rows": 450,
                  "Plan Width": 48,
                  "Output": ["o.id", "o.user_id"]
                },
                {
                  "Node Type": "Hash",
                  "Parent Relationship": "Inner",
                  "Parallel Aware": false,
                  "Async Capable": false,
                  "Startup Cost": 12.40,
                  "Total Cost": 12.40,
                  "Plan Rows": 8,
                  "Plan Width": 48,
                  "Output": ["u.id", "u.name"],
                  "Plans": [
                    {
                      "Node Type": "Index Scan",
                      "Parallel Aware": false,
                      "Async Capable": false,
                      "Scan Direction": "Forward",
                      "Index Name": "users_pkey",
                      "Relation Name": "UserProfile",
                      "Schema": "tenant-4812",
                      "Alias": "u",
                      "Startup Cost": 0.28,
                      "Total Cost": 12.40,
                      "Plan Rows": 8,
                      "Plan Width": 48,
                      "Output": ["u.id", "u.name"]
                    }
                  ]
                }
              ]
            }
          ]
        }
      ]
    }
  }
]`,
			},
			indexes: [][4]string{
				{"cluster_shard_1", "tenant-4812", "UserProfile", "users_pkey"},
			},
		},

		// 6. Subquery in WHERE Clause (Hash Semi Join)
		{
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Hash Join",
      "Parallel Aware": false,
      "Async Capable": false,
      "Join Type": "Semi",
      "Startup Cost": 15.20,
      "Total Cost": 42.80,
      "Plan Rows": 30,
      "Plan Width": 72,
      "Output": ["e1.id", "e1.name", "e1.\"dept-id\""],
      "Hash Cond": "(e1.\"dept-id\" = e2.\"dept-id\")",
      "Plans": [
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Async Capable": false,
          "Relation Name": "employees",
          "Schema": "corporate-hr",
          "Alias": "e1",
          "Startup Cost": 0.00,
          "Total Cost": 22.00,
          "Plan Rows": 1200,
          "Plan Width": 72,
          "Output": ["e1.id", "e1.name", "e1.\"dept-id\""]
        },
        {
          "Node Type": "Hash",
          "Parent Relationship": "Inner",
          "Parallel Aware": false,
          "Async Capable": false,
          "Startup Cost": 14.10,
          "Total Cost": 14.10,
          "Plan Rows": 88,
          "Plan Width": 4,
          "Output": ["e2.\"dept-id\""],
          "Plans": [
            {
              "Node Type": "Seq Scan",
              "Parallel Aware": false,
              "Async Capable": false,
              "Relation Name": "departments",
              "Schema": "corporate-hr",
              "Alias": "e2",
              "Startup Cost": 0.00,
              "Total Cost": 14.10,
              "Plan Rows": 88,
              "Plan Width": 4,
              "Output": ["e2.\"dept-id\""],
              "Filter": "(e2.location = 'US'::text)"
            }
          ]
        }
      ]
    }
  }
]`,
			},
			indexes: nil,
		},

		// 7. Derived Table / Common Table Expression (CTE Scan)
		{
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "CTE Scan",
      "Parallel Aware": false,
      "Async Capable": false,
      "CTE Name": "filtered-payments",
      "Alias": "filtered-payments",
      "Startup Cost": 45.00,
      "Total Cost": 65.00,
      "Plan Rows": 1000,
      "Plan Width": 64,
      "Output": ["\"filtered-payments\".id", "\"filtered-payments\".amount"],
      "Filter": "(\"filtered-payments\".amount > 100.00)",
      "Plans": [
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "InitPlan",
          "Subplan Name": "CTE filtered-payments",
          "Parallel Aware": false,
          "Async Capable": false,
          "Relation Name": "payments",
          "Schema": "billing",
          "Alias": "payments",
          "Startup Cost": 0.00,
          "Total Cost": 45.00,
          "Plan Rows": 2500,
          "Plan Width": 64,
          "Output": ["payments.id", "payments.amount", "payments.status"],
          "Filter": "(payments.status = 'COMPLETED'::text)"
        }
      ]
    }
  }
]`,
			},
			indexes: nil,
		},

		// 8. Index Merge Optimization (BitmapAnd)
		{
			db: "saas_engine",
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Bitmap Heap Scan",
      "Parallel Aware": false,
      "Async Capable": false,
      "Relation Name": "user-tasks",
      "Schema": "workflow",
      "Alias": "user-tasks",
      "Startup Cost": 8.84,
      "Total Cost": 24.50,
      "Plan Rows": 25,
      "Plan Width": 40,
      "Output": ["id", "title", "status", "\"userId\""],
      "Recheck Cond": "((\"user-tasks\".status = 'PENDING'::text) AND (\"user-tasks\".\"userId\" = 101))",
      "Plans": [
        {
          "Node Type": "BitmapAnd",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Async Capable": false,
          "Startup Cost": 8.84,
          "Total Cost": 8.84,
          "Plan Rows": 25,
          "Plan Width": 0,
          "Plans": [
            {
              "Node Type": "Bitmap Index Scan",
              "Parent Relationship": "Member",
              "Parallel Aware": false,
              "Async Capable": false,
              "Index Name": "idx_tasks_status",
              "Startup Cost": 0.00,
              "Total Cost": 4.15,
              "Plan Rows": 120,
              "Plan Width": 0,
              "Index Cond": "(\"user-tasks\".status = 'PENDING'::text)"
            },
            {
              "Node Type": "Bitmap Index Scan",
              "Parent Relationship": "Member",
              "Parallel Aware": false,
              "Async Capable": false,
              "Index Name": "idx_tasks_user",
              "Startup Cost": 0.00,
              "Total Cost": 4.44,
              "Plan Rows": 80,
              "Plan Width": 0,
              "Index Cond": "(\"user-tasks\".\"userId\" = 101)"
            }
          ]
        }
      ]
    }
  }
]`,
			},
			indexes: [][4]string{
				{"saas_engine", "workflow", "user-tasks", "idx_tasks_status"},
				{"saas_engine", "workflow", "user-tasks", "idx_tasks_user"},
			},
		},

		// 9. Sequential Scan (No Index Used)
		{
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Seq Scan",
      "Parallel Aware": false,
      "Async Capable": false,
      "Relation Name": "BlogPosts",
      "Schema": "cms",
      "Alias": "BlogPosts",
      "Startup Cost": 0.00,
      "Total Cost": 245.00,
      "Plan Rows": 8540,
      "Plan Width": 512,
      "Output": ["id", "title", "\"rawBody\"", "user_id"],
      "Filter": "(\"BlogPosts\".\"rawBody\" ~~ '%golang%'::text)"
    }
  }
]`,
			},
			indexes: nil,
		},

		// 10. Index Merge Optimization (BitmapOr)
		{
			db: "sales_ledger",
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Bitmap Heap Scan",
      "Parallel Aware": false,
      "Async Capable": false,
      "Relation Name": "orders",
      "Schema": "sales",
      "Alias": "orders",
      "Startup Cost": 9.12,
      "Total Cost": 35.80,
      "Plan Rows": 120,
      "Plan Width": 96,
      "Output": ["id", "\"customerId\"", "total", "created_at"],
      "Recheck Cond": "((orders.\"customerId\" = 5) OR (orders.created_at > '2026-07-01'::date))",
      "Plans": [
        {
          "Node Type": "BitmapOr",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Async Capable": false,
          "Startup Cost": 9.12,
          "Total Cost": 9.12,
          "Plan Rows": 120,
          "Plan Width": 0,
          "Plans": [
            {
              "Node Type": "Bitmap Index Scan",
              "Parent Relationship": "Member",
              "Parallel Aware": false,
              "Async Capable": false,
              "Index Name": "idx_orders_cust",
              "Startup Cost": 0.00,
              "Total Cost": 4.20,
              "Plan Rows": 10,
              "Plan Width": 0,
              "Index Cond": "(orders.\"customerId\" = 5)"
            },
            {
              "Node Type": "Bitmap Index Scan",
              "Parent Relationship": "Member",
              "Parallel Aware": false,
              "Async Capable": false,
              "Index Name": "idx_orders_date",
              "Startup Cost": 0.00,
              "Total Cost": 4.67,
              "Plan Rows": 110,
              "Plan Width": 0,
              "Index Cond": "(orders.created_at > '2026-07-01'::date)"
            }
          ]
        }
      ]
    }
  }
]`,
			},
			indexes: [][4]string{
				{"sales_ledger", "sales", "orders", "idx_orders_cust"},
				{"sales_ledger", "sales", "orders", "idx_orders_date"},
			},
		},

		// 11. Composite Index Scan
		{
			db: "operations_db",
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Index Scan",
      "Parallel Aware": false,
      "Async Capable": false,
      "Scan Direction": "Forward",
      "Index Name": "idx_route_date_status",
      "Relation Name": "flight_bookings",
      "Schema": "logistics",
      "Alias": "flight_bookings",
      "Startup Cost": 0.42,
      "Total Cost": 12.50,
      "Plan Rows": 2,
      "Plan Width": 64,
      "Output": ["id", "\"routeId\"", "flight_date", "status"],
      "Index Cond": "(2026-08-15'::date) AND (flight_bookings.status = 'CONFIRMED'::text))"
    }
  }
]`,
			},
			indexes: [][4]string{
				{"operations_db", "logistics", "flight_bookings", "idx_route_date_status"},
			},
		},

		// 12. Append (UNION ALL / UNION)
		{
			db: "cluster_shard_1",
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Aggregate",
      "Strategy": "Hashed",
      "Partial Mode": "Simple",
      "Parallel Aware": false,
      "Async Capable": false,
      "Startup Cost": 185.00,
      "Total Cost": 195.00,
      "Plan Rows": 1000,
      "Plan Width": 64,
      "Output": ["\"active users\".id", "\"active users\".name"],
      "Group Key": ["\"active users\".id", "\"active users\".name"],
      "Plans": [
        {
          "Node Type": "Append",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Async Capable": false,
          "Startup Cost": 0.28,
          "Total Cost": 155.00,
          "Plan Rows": 3000,
          "Plan Width": 64,
          "Plans": [
            {
              "Node Type": "Index Scan",
              "Parent Relationship": "Member",
              "Parallel Aware": false,
              "Async Capable": false,
              "Scan Direction": "Forward",
              "Index Name": "idx_active_status",
              "Relation Name": "UserProfile",
              "Schema": "tenant-4812",
              "Alias": "active users",
              "Startup Cost": 0.28,
              "Total Cost": 45.00,
              "Plan Rows": 450,
              "Plan Width": 64,
              "Output": ["\"active users\".id", "\"active users\".name"],
              "Index Cond": "(\"active users\".status = 'ACTIVE'::text)"
            },
            {
              "Node Type": "Seq Scan",
              "Parent Relationship": "Member",
              "Parallel Aware": false,
              "Async Capable": false,
              "Relation Name": "users_history",
              "Schema": "archive",
              "Alias": "archived_users",
              "Startup Cost": 0.00,
              "Total Cost": 95.00,
              "Plan Rows": 2550,
              "Plan Width": 64,
              "Output": ["archived_users.id", "archived_users.name"]
            }
          ]
        }
      ]
    }
  }
]`,
			},
			indexes: [][4]string{
				{"cluster_shard_1", "tenant-4812", "UserProfile", "idx_active_status"},
			},
		},

		// 13. Unique Secondary Index Lookup
		{
			db: "banking_core",
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Index Scan",
      "Parallel Aware": false,
      "Async Capable": false,
      "Scan Direction": "Forward",
      "Index Name": "uk_accounts_uuid",
      "Relation Name": "SystemAccounts",
      "Schema": "ledger",
      "Alias": "SystemAccounts",
      "Startup Cost": 0.42,
      "Total Cost": 8.44,
      "Plan Rows": 1,
      "Plan Width": 16,
      "Output": ["id", "\"uuidKey\"", "balance"],
      "Index Cond": "(\"SystemAccounts\".\"uuidKey\" = 'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d'::uuid)"
    }
  }
]`,
			},
			indexes: [][4]string{
				{"banking_core", "ledger", "SystemAccounts", "uk_accounts_uuid"},
			},
		},

		// 14. Nested Loop Join
		{
			db: "web_content",
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Nested Loop",
      "Parallel Aware": false,
      "Async Capable": false,
      "Join Type": "Inner",
      "Startup Cost": 0.28,
      "Total Cost": 25.50,
      "Plan Rows": 5,
      "Plan Width": 128,
      "Output": ["articles.id", "articles.title", "options.\"opt Value\""],
      "Plans": [
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Async Capable": false,
          "Relation Name": "articles",
          "Schema": "cms",
          "Alias": "articles",
          "Startup Cost": 0.00,
          "Total Cost": 12.50,
          "Plan Rows": 10,
          "Plan Width": 96,
          "Output": ["articles.id", "articles.title", "articles.author_id"],
          "Filter": "(articles.author_id = 12)"
        },
        {
          "Node Type": "Index Scan",
          "Parent Relationship": "Inner",
          "Parallel Aware": false,
          "Async Capable": false,
          "Scan Direction": "Forward",
          "Index Name": "idx_options_art",
          "Relation Name": "article Options",
          "Schema": "configuration",
          "Alias": "options",
          "Startup Cost": 0.28,
          "Total Cost": 1.28,
          "Plan Rows": 1,
          "Plan Width": 32,
          "Output": ["options.article_id", "options.\"opt Value\""],
          "Index Cond": "(options.article_id = articles.id)"
        }
      ]
    }
  }
]`,
			},
			indexes: [][4]string{
				{"web_content", "configuration", "article Options", "idx_options_art"},
			},
		},

		// 15. Partitioned Table Scan
		{
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Append",
      "Parallel Aware": false,
      "Async Capable": false,
      "Startup Cost": 0.00,
      "Total Cost": 340.50,
      "Plan Rows": 14200,
      "Plan Width": 64,
      "Output": ["sensor_data_1.id", "sensor_data_1.timestamp"],
      "Plans": [
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Member",
          "Parallel Aware": false,
          "Async Capable": false,
          "Relation Name": "sensor.data.p2025",
          "Schema": "telemetry",
          "Alias": "sensor_data_1",
          "Startup Cost": 0.00,
          "Total Cost": 120.00,
          "Plan Rows": 5000,
          "Plan Width": 64,
          "Output": ["sensor_data_1.id", "sensor_data_1.timestamp"],
          "Filter": "((sensor_data_1.timestamp >= '2025-01-01'::timestamp without time zone) AND (sensor_data_1.timestamp <= '2026-01-01'::timestamp without time zone))"
        },
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Member",
          "Parallel Aware": false,
          "Async Capable": false,
          "Relation Name": "sensor.data.p2026",
          "Schema": "telemetry",
          "Alias": "sensor_data_2",
          "Startup Cost": 0.00,
          "Total Cost": 220.50,
          "Plan Rows": 9200,
          "Plan Width": 64,
          "Output": ["sensor_data_2.id", "sensor_data_2.timestamp"],
          "Filter": "((sensor_data_2.timestamp >= '2025-01-01'::timestamp without time zone) AND (sensor_data_2.timestamp <= '2026-01-01'::timestamp without time zone))"
        }
      ]
    }
  }
]`,
			},
			indexes: nil,
		},

		// 16. GroupAggregate (Presorted Index Group By)
		{
			db: "bi_reporting",
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Aggregate",
      "Strategy": "Sorted",
      "Partial Mode": "Simple",
      "Parallel Aware": false,
      "Async Capable": false,
      "Startup Cost": 0.42,
      "Total Cost": 85.50,
      "Plan Rows": 40,
      "Plan Width": 48,
      "Output": ["transactions.\"userId\"", "count(*)"],
      "Group Key": ["transactions.\"userId\""],
      "Plans": [
        {
          "Node Type": "Index Scan",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Async Capable": false,
          "Scan Direction": "Forward",
          "Index Name": "idx_transactions_user",
          "Relation Name": "financial Transactions",
          "Schema": "analytics",
          "Alias": "transactions",
          "Startup Cost": 0.42,
          "Total Cost": 72.00,
          "Plan Rows": 2500,
          "Plan Width": 24,
          "Output": ["transactions.\"userId\""]
        }
      ]
    }
  }
]`,
			},
			indexes: [][4]string{
				{"bi_reporting", "analytics", "financial Transactions", "idx_transactions_user"},
			},
		},

		// 17. Explicit Sort
		{
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Sort",
      "Parallel Aware": false,
      "Async Capable": false,
      "Startup Cost": 145.00,
      "Total Cost": 151.25,
      "Plan Rows": 2500,
      "Plan Width": 128,
      "Output": ["id", "\"firstName\"", "\"lastName\""],
      "Sort Key": ["customers.\"lastName\"", "customers.\"firstName\""],
      "Plans": [
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Async Capable": false,
          "Relation Name": "customers",
          "Schema": "crm",
          "Alias": "customers",
          "Startup Cost": 0.00,
          "Total Cost": 45.00,
          "Plan Rows": 2500,
          "Plan Width": 128,
          "Output": ["id", "\"firstName\"", "\"lastName\""]
        }
      ]
    }
  }
]`,
			},
			indexes: nil,
		},

		// 18. Subquery Materialization (In Clause)
		{
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Hash Join",
      "Parallel Aware": false,
      "Async Capable": false,
      "Join Type": "Inner",
      "Startup Cost": 22.50,
      "Total Cost": 68.00,
      "Plan Rows": 80,
      "Plan Width": 32,
      "Output": ["store.id", "store.name"],
      "Hash Cond": "(store.discount_id = \"Subquery\".id)",
      "Plans": [
        {
          "Node Type": "Seq Scan",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Async Capable": false,
          "Relation Name": "store_locations",
          "Schema": "retail",
          "Alias": "store",
          "Startup Cost": 0.00,
          "Total Cost": 18.00,
          "Plan Rows": 80,
          "Plan Width": 32,
          "Output": ["store.id", "store.name", "store.discount_id"]
        },
        {
          "Node Type": "Hash",
          "Parent Relationship": "Inner",
          "Parallel Aware": false,
          "Async Capable": false,
          "Startup Cost": 20.00,
          "Total Cost": 20.00,
          "Plan Rows": 200,
          "Plan Width": 4,
          "Output": ["\"Subquery\".id"],
          "Plans": [
            {
              "Node Type": "Aggregate",
              "Strategy": "Hashed",
              "Partial Mode": "Simple",
              "Parallel Aware": false,
              "Async Capable": false,
              "Startup Cost": 18.00,
              "Total Cost": 20.00,
              "Plan Rows": 200,
              "Plan Width": 4,
              "Output": ["\"Subquery\".id"],
              "Group Key": ["discounts.id"],
              "Plans": [
                {
                  "Node Type": "Seq Scan",
                  "Parent Relationship": "Outer",
                  "Parallel Aware": false,
                  "Async Capable": false,
                  "Relation Name": "discounts",
                  "Schema": "marketing",
                  "Alias": "discounts",
                  "Startup Cost": 0.00,
                  "Total Cost": 15.00,
                  "Plan Rows": 300,
                  "Plan Width": 4,
                  "Output": ["discounts.id"],
                  "Filter": "(discounts.is_active = true)"
                }
              ]
            }
          ]
        }
      ]
    }
  }
]`,
			},
			indexes: nil,
		},

		// 19. Full Text Index Scan (GIN/GiST Index)
		{
			db: "knowledge_base",
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Bitmap Heap Scan",
      "Parallel Aware": false,
      "Async Capable": false,
      "Relation Name": "documents",
      "Schema": "kb",
      "Alias": "documents",
      "Startup Cost": 12.25,
      "Total Cost": 45.30,
      "Plan Rows": 10,
      "Plan Width": 512,
      "Output": ["id", "title", "body"],
      "Recheck Cond": "(documents.\"tsvBody\" @@ to_tsquery('golang & database'::text))",
      "Plans": [
        {
          "Node Type": "Bitmap Index Scan",
          "Parent Relationship": "Outer",
          "Parallel Aware": false,
          "Async Capable": false,
          "Index Name": "gin_idx_doc_body",
          "Startup Cost": 0.00,
          "Total Cost": 12.25,
          "Plan Rows": 10,
          "Plan Width": 0,
          "Index Cond": "(documents.\"tsvBody\" @@ to_tsquery('golang & database'::text))"
        }
      ]
    }
  }
]`,
			},
			indexes: [][4]string{
				{"knowledge_base", "kb", "documents", "gin_idx_doc_body"},
			},
		},

		// 20. Result Node (Const Evaluation / Impossible Where)
		{
			explain: proto.ExplainResult{
				JSON: `[
  {
    "Plan": {
      "Node Type": "Result",
      "Parallel Aware": false,
      "Async Capable": false,
      "Startup Cost": 0.00,
      "Total Cost": 0.00,
      "Plan Rows": 0,
      "Plan Width": 0,
      "Output": ["id", "name"],
      "One-Time Filter": "false"
    }
  }
]`,
			},
			indexes: nil,
		},
	},
	instance.SubsystemNameMongo: {
		// 1. Simple Primary Key Lookup (ID Scan via IXSCAN)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "production_db.UserProfile",
    "winningPlan": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "IXSCAN",
        "keyPattern": {
          "_id": 1
        },
        "indexName": "_id_"
      }
    }
  }
}`,
			},
			db: "production_db",
			indexes: [][4]string{
				{"", "production_db", "UserProfile", "_id_"},
			},
		},

		// 2. Secondary Non-Unique Index Scan
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "ecommerce_warehouse.products",
    "winningPlan": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "IXSCAN",
        "keyPattern": {
          "categoryId": 1
        },
        "indexName": "idx_products_cat"
      }
    }
  }
}`,
			},
			db: "ecommerce_warehouse",
			indexes: [][4]string{
				{"", "ecommerce_warehouse", "products", "idx_products_cat"},
			},
		},

		// 3. Index Range Scan
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "telemetry_store.AppLogs",
    "winningPlan": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "IXSCAN",
        "keyPattern": {
          "created_at": 1
        },
        "indexName": "idx_logs_created"
      }
    }
  }
}`,
			},
			db: "telemetry_store",
			indexes: [][4]string{
				{"", "telemetry_store", "AppLogs", "idx_logs_created"},
			},
		},

		// 4. Covered Index Scan (PROJECTION_COVERED)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "auth_authority.identities",
    "winningPlan": {
      "stage": "PROJECTION_COVERED",
      "inputStage": {
        "stage": "IXSCAN",
        "keyPattern": {
          "email": 1,
          "is_active": 1
        },
        "indexName": "idx_users_email"
      }
    }
  }
}`,
			},
			db: "auth_authority",
			indexes: [][4]string{
				{"", "auth_authority", "identities", "idx_users_email"},
			},
		},

		// 5. Multi-Index Intersection (AND_SORTED)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "saas_engine.user-tasks",
    "winningPlan": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "AND_SORTED",
        "inputStages": [
          {
            "stage": "IXSCAN",
            "indexName": "idx_tasks_status"
          },
          {
            "stage": "IXSCAN",
            "indexName": "idx_tasks_user"
          }
        ]
      }
    }
  }
}`,
			},
			db: "saas_engine",
			indexes: [][4]string{
				{"", "saas_engine", "user-tasks", "idx_tasks_status"},
				{"", "saas_engine", "user-tasks", "idx_tasks_user"},
			},
		},

		// 6. Collection Scan (COLLSCAN - No Index used)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "web_content.BlogPosts",
    "winningPlan": {
      "stage": "COLLSCAN",
      "filter": {
        "rawBody": {
          "$regex": "golang"
        }
      }
    }
  }
}`,
			},
			db:      "web_content",
			indexes: nil,
		},

		// 7. Multi-Index Union (OR Stage)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "sales_ledger.orders",
    "winningPlan": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "OR",
        "inputStages": [
          {
            "stage": "IXSCAN",
            "indexName": "idx_orders_cust"
          },
          {
            "stage": "IXSCAN",
            "indexName": "idx_orders_date"
          }
        ]
      }
    }
  }
}`,
			},
			db: "sales_ledger",
			indexes: [][4]string{
				{"", "sales_ledger", "orders", "idx_orders_cust"},
				{"", "sales_ledger", "orders", "idx_orders_date"},
			},
		},

		// 8. Text Search Index Scan (TEXT stage)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "knowledge_base.documents",
    "winningPlan": {
      "stage": "TEXT_MATCH",
      "inputStage": {
        "stage": "TEXT",
        "indexName": "text_idx_doc_body"
      }
    }
  }
}`,
			},
			db: "knowledge_base",
			indexes: [][4]string{
				{"", "knowledge_base", "documents", "text_idx_doc_body"},
			},
		},

		// 9. Impossible Query Optimization (EOF Stage)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "system_catalog.users",
    "winningPlan": {
      "stage": "EOF"
    }
  }
}`,
			},
			db:      "system_catalog",
			indexes: nil,
		},

		// 10. Compound Index Prefix Scan
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "shipping_provider.shipments",
    "winningPlan": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "IXSCAN",
        "keyPattern": {
          "tenant_id": 1,
          "status": 1,
          "date": -1
        },
        "indexName": "idx_tenant_status_date"
      }
    }
  }
}`,
			},
			db: "shipping_provider",
			indexes: [][4]string{
				{"", "shipping_provider", "shipments", "idx_tenant_status_date"},
			},
		},

		// 11. Geospatial 2dsphere Index Scan (GEO_NEAR_2DSPHERE)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "fleet_management.vehicles",
    "winningPlan": {
      "stage": "GEO_NEAR_2DSPHERE",
      "indexName": "geo_idx_location"
    }
  }
}`,
			},
			db: "fleet_management",
			indexes: [][4]string{
				{"", "fleet_management", "vehicles", "geo_idx_location"},
			},
		},

		// 12. Hashed Index Scan (Shard Key / Equality Lookup)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "user_analytics.events",
    "winningPlan": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "IXSCAN",
        "keyPattern": {
          "user_id": "hashed"
        },
        "indexName": "idx_user_id_hashed"
      }
    }
  }
}`,
			},
			db: "user_analytics",
			indexes: [][4]string{
				{"", "user_analytics", "events", "idx_user_id_hashed"},
			},
		},

		// 13. Index-Assisted Sorting (No In-Memory SORT stage needed)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "finance_ledger.transactions",
    "winningPlan": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "IXSCAN",
        "keyPattern": {
          "account_id": 1,
          "timestamp": -1
        },
        "indexName": "idx_account_time"
      }
    }
  }
}`,
			},
			db: "finance_ledger",
			indexes: [][4]string{
				{"", "finance_ledger", "transactions", "idx_account_time"},
			},
		},

		// 14. In-Memory Sort Fallback (COLLSCAN + SORT)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "media_catalog.movies",
    "winningPlan": {
      "stage": "SORT",
      "inputStage": {
        "stage": "COLLSCAN"
      }
    }
  }
}`,
			},
			db:      "media_catalog",
			indexes: nil,
		},

		// 15. In-Memory Sort with Index Fetch (IXSCAN + SORT)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "inventory_app.items",
    "winningPlan": {
      "stage": "SORT",
      "inputStage": {
        "stage": "FETCH",
        "inputStage": {
          "stage": "IXSCAN",
          "indexName": "idx_items_tags"
        }
      }
    }
  }
}`,
			},
			db: "inventory_app",
			indexes: [][4]string{
				{"", "inventory_app", "items", "idx_items_tags"},
			},
		},

		// 16. Partial Index Match (Query satisfies criteria expression)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "crm_platform.contacts",
    "winningPlan": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "IXSCAN",
        "keyPattern": {
          "email": 1
        },
        "indexName": "partial_idx_active_emails"
      }
    }
  }
}`,
			},
			db: "crm_platform",
			indexes: [][4]string{
				{"", "crm_platform", "contacts", "partial_idx_active_emails"},
			},
		},

		// 17. Multikey Index Scan (Array field lookup)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "social_graph.posts",
    "winningPlan": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "IXSCAN",
        "isMultiKey": true,
        "indexName": "idx_posts_tags"
      }
    }
  }
}`,
			},
			db: "social_graph",
			indexes: [][4]string{
				{"", "social_graph", "posts", "idx_posts_tags"},
			},
		},

		// 18. Count Scanning Execution Optimization (COUNT_SCAN)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "billing_service.invoices",
    "winningPlan": {
      "stage": "COUNT_SCAN",
      "indexName": "idx_invoice_status"
    }
  }
}`,
			},
			db: "billing_service",
			indexes: [][4]string{
				{"", "billing_service", "invoices", "idx_invoice_status"},
			},
		},

		// 19. Wildcard Index Scan ($** path routing)
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "custom_attributes.entities",
    "winningPlan": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "IXSCAN",
        "keyPattern": {
          "dynamic_fields.$**": 1
        },
        "indexName": "idx_wildcard_fields"
      }
    }
  }
}`,
			},
			db: "custom_attributes",
			indexes: [][4]string{
				{"", "custom_attributes", "entities", "idx_wildcard_fields"},
			},
		},

		// 20. Multi-Index Intersection via AND_HASH Stage
		{
			explain: proto.ExplainResult{
				JSON: `{
  "queryPlanner": {
    "namespace": "support_ticketing.tickets",
    "winningPlan": {
      "stage": "FETCH",
      "inputStage": {
        "stage": "AND_HASH",
        "inputStages": [
          {
            "stage": "IXSCAN",
            "indexName": "idx_tickets_assignee"
          },
          {
            "stage": "IXSCAN",
            "indexName": "idx_tickets_priority"
          }
        ]
      }
    }
  }
}`,
			},
			db: "support_ticketing",
			indexes: [][4]string{
				{"", "support_ticketing", "tickets", "idx_tickets_assignee"},
				{"", "support_ticketing", "tickets", "idx_tickets_priority"},
			},
		},

		// 21. Cross-Collection Join ($lookup) using Indexes on both Collections
		{
			explain: proto.ExplainResult{
				JSON: `{
  "stages": [
    {
      "$cursor": {
        "queryPlanner": {
          "namespace": "ecommerce_warehouse.orders",
          "winningPlan": {
            "stage": "FETCH",
            "inputStage": {
              "stage": "IXSCAN",
              "keyPattern": { "status": 1 },
              "indexName": "idx_orders_status"
            }
          }
        }
      }
    },
    {
      "$lookup": {
        "from": "products",
        "as": "product_details",
        "localField": "product_id",
        "foreignField": "_id",
        "strategy": "indexBounded",
        "expandingDrivenStage": {
          "queryPlanner": {
            "namespace": "ecommerce_warehouse.products",
            "winningPlan": {
              "stage": "IXSCAN",
              "keyPattern": { "_id": 1 },
              "indexName": "_id_"
            }
          }
        }
      }
    }
  ]
}`,
			},
			db: "ecommerce_warehouse",
			indexes: [][4]string{
				{"", "ecommerce_warehouse", "orders", "idx_orders_status"},
				{"", "ecommerce_warehouse", "products", "_id_"},
			},
		},

		// 22. Cross-Collection Join ($lookup) where Foreign Side uses a Secondary Index
		{
			explain: proto.ExplainResult{
				JSON: `{
  "stages": [
    {
      "$cursor": {
        "queryPlanner": {
          "namespace": "auth_authority.users",
          "winningPlan": {
            "stage": "COLLSCAN"
          }
        }
      }
    },
    {
      "$lookup": {
        "from": "sessions",
        "as": "active_sessions",
        "localField": "_id",
        "foreignField": "user_id",
        "strategy": "indexBounded",
        "expandingDrivenStage": {
          "queryPlanner": {
            "namespace": "auth_authority.sessions",
            "winningPlan": {
              "stage": "IXSCAN",
              "keyPattern": { "user_id": 1 },
              "indexName": "idx_sessions_user"
            }
          }
        }
      }
    }
  ]
}`,
			},
			db: "auth_authority",
			indexes: [][4]string{
				{"", "auth_authority", "sessions", "idx_sessions_user"},
			},
		},

		// 23. 3-Collection Join Pipeline (Orders -> Products -> Categories) All Indexed
		{
			explain: proto.ExplainResult{
				JSON: `{
  "stages": [
    {
      "$cursor": {
        "queryPlanner": {
          "namespace": "ecommerce_warehouse.orders",
          "winningPlan": {
            "stage": "FETCH",
            "inputStage": {
              "stage": "IXSCAN",
              "keyPattern": { "status": 1 },
              "indexName": "idx_orders_status"
            }
          }
        }
      }
    },
    {
      "$lookup": {
        "from": "products",
        "as": "product_docs",
        "strategy": "indexBounded",
        "expandingDrivenStage": {
          "queryPlanner": {
            "namespace": "ecommerce_warehouse.products",
            "winningPlan": {
              "stage": "IXSCAN",
              "keyPattern": { "_id": 1 },
              "indexName": "_id_"
            }
          }
        }
      }
    },
    {
      "$lookup": {
        "from": "categories",
        "as": "category_docs",
        "strategy": "indexBounded",
        "expandingDrivenStage": {
          "queryPlanner": {
            "namespace": "ecommerce_warehouse.categories",
            "winningPlan": {
              "stage": "IXSCAN",
              "keyPattern": { "code": 1 },
              "indexName": "idx_cat_code"
            }
          }
        }
      }
    }
  ]
}`,
			},
			db: "ecommerce_warehouse",
			indexes: [][4]string{
				{"", "ecommerce_warehouse", "orders", "idx_orders_status"},
				{"", "ecommerce_warehouse", "products", "_id_"},
				{"", "ecommerce_warehouse", "categories", "idx_cat_code"},
			},
		},

		// 24. 4-Collection Pipeline (Users -> Logs -> Locations -> Alerts) Mixed Indexing Modes
		{
			explain: proto.ExplainResult{
				JSON: `{
  "stages": [
    {
      "$cursor": {
        "queryPlanner": {
          "namespace": "telemetry_store.users",
          "winningPlan": {
            "stage": "COLLSCAN"
          }
        }
      }
    },
    {
      "$lookup": {
        "from": "logs",
        "as": "user_logs",
        "strategy": "indexBounded",
        "expandingDrivenStage": {
          "queryPlanner": {
            "namespace": "telemetry_store.logs",
            "winningPlan": {
              "stage": "IXSCAN",
              "keyPattern": { "user_id": 1 },
              "indexName": "idx_logs_user"
            }
          }
        }
      }
    },
    {
      "$lookup": {
        "from": "locations",
        "as": "geo_data",
        "strategy": "indexBounded",
        "expandingDrivenStage": {
          "queryPlanner": {
            "namespace": "telemetry_store.locations",
            "winningPlan": {
              "stage": "GEO_NEAR_2DSPHERE",
              "indexName": "geo_idx_coordinates"
            }
          }
        }
      }
    },
    {
      "$lookup": {
        "from": "alerts",
        "as": "system_alerts",
        "strategy": "indexBounded",
        "expandingDrivenStage": {
          "queryPlanner": {
            "namespace": "telemetry_store.alerts",
            "winningPlan": {
              "stage": "IXSCAN",
              "keyPattern": { "severity": 1, "resolved": 1 },
              "indexName": "idx_alerts_severity_resolved"
            }
          }
        }
      }
    }
  ]
}`,
			},
			db: "telemetry_store",
			indexes: [][4]string{
				{"", "telemetry_store", "logs", "idx_logs_user"},
				{"", "telemetry_store", "locations", "geo_idx_coordinates"},
				{"", "telemetry_store", "alerts", "idx_alerts_severity_resolved"},
			},
		},
	},
}

func TestExtractIndexesFromExplain(t *testing.T) {
	for subsystem, tests := range extractIndexesFromExplainTests {
		for i, test := range tests {
			indexes, err := qan.ExtractIndexesFromExplain(subsystem, test.db, test.explain)
			if err != nil {
				t.Fatal(err)
			}

			if !slices.Equal(indexes, test.indexes) {
				t.Fatalf("subsystem %s test %d, expected: %+v, got: %+v", subsystem, i+1, test.indexes, indexes)
			}
		}
	}
}
