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
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os/exec"
	"reflect"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	queryProto "github.com/shatteredsilicon/ssm/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

// QueryInfo information about query
type QueryInfo struct {
	Fingerprint string
	Abstract    string
	Tables      []queryProto.Table
	Procedures  []queryProto.Procedure
}

// TableJSON returns tables as JSON string
func (q QueryInfo) TableJSON() string {
	if len(q.Tables) == 0 {
		return ""
	}

	bytes, _ := json.Marshal(q.Tables)
	return string(bytes)
}

// ProcedureJSON returns procedures as JSON string
func (q QueryInfo) ProcedureJSON() string {
	if len(q.Procedures) == 0 {
		return ""
	}

	bytes, _ := json.Marshal(q.Procedures)
	return string(bytes)
}

type parseTry struct {
	subsystem string
	query     string
	q         QueryInfo
	s         sqlparser.Statement
	pr        *pg_query.ParseResult
	queryChan chan QueryInfo
	crashChan chan bool
}

type protoTables []queryProto.Table

func (t protoTables) String() string {
	s := ""
	sep := ""
	for _, table := range t {
		s += sep + table.String()
		sep = " "
	}
	return s
}

const (
	MAX_JOIN_DEPTH = 100
	MAX_EXPR_DEPTH = 100
)

var (
	ErrNotSupported = errors.New("SQL parser does not support the query")
)

type Mini struct {
	Debug      bool
	cwd        string
	queryIn    chan string
	miniOut    chan string
	parseChan  chan parseTry
	onlyTables bool
	stopChan   chan struct{}
}

func NewMini(cwd string) *Mini {
	m := &Mini{
		cwd:        cwd,
		onlyTables: cwd == "",         // only tables if no path to mini.pl given
		queryIn:    make(chan string), // XXX see note below
		miniOut:    make(chan string), // XXX see note below
		parseChan:  make(chan parseTry, 1),
		stopChan:   make(chan struct{}),
	}
	return m
	/// XXX DO NOT BUFFER queryIn or miniOut, else everything will break!
	//      There's only 1 mini.pl proc per Mini instance, and the Mini instance
	//      can be shared (e.g. processing QAN data for mulitple agents).
	//      Unbuffered chans serialize access to mini.pl in usePerl(). If either
	//      one of the chans is buffered, a race condition is created which
	//      results in goroutines receiving the wrong data. -- parseChan is a
	///     different approach; it can be buffered.
}

func (m *Mini) Stop() {
	close(m.stopChan)
}

func (m *Mini) Run() {
	// Go-based SQL parsing
	go m.parse()

	// Perl-based SQL parsing
	if !m.onlyTables {
		cmd := exec.Command(m.cwd + "/mini.pl")

		stdin, err := cmd.StdinPipe()
		if err != nil {
			log.Fatal(err)
		}

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			log.Fatal(err)
		}

		r := bufio.NewReader(stdout)

		if err := cmd.Start(); err != nil {
			log.Fatal(err)
		}

		for {
			select {
			case query := <-m.queryIn:
				// Do not use buffered IO so input/output is immediate.
				// Do not forget "\n" because mini.pl is reading lines.
				if _, err := io.WriteString(stdin, query+"\n"); err != nil {
					log.Fatal(err)
				}
				q, err := r.ReadString('\n')
				if err != nil {
					log.Fatal(err)
				}
				m.miniOut <- q
			case <-m.stopChan:
				return
			}
		}
	}
}

func (m *Mini) Parse(fingerprint, example, defaultDb string, isPg bool) (QueryInfo, error) {
	fingerprint = strings.TrimSpace(fingerprint)
	example = strings.TrimSpace(example)
	q := QueryInfo{
		Fingerprint: fingerprint,
		Tables:      []queryProto.Table{},
	}
	defer func() {
		q.Abstract = strings.TrimSpace(q.Abstract)
	}()

	if m.Debug {
		fmt.Printf("\n\nexample: %s\n", example)
		fmt.Printf("\n\nfingerprint: %s\n", fingerprint)
	}

	query := fingerprint
	// If we have a query example, that's better to parse than a fingerprint.
	if example != "" {
		query = example
	}

	// Fingerprints replace IN (1, 2) -> in (?+) but "?+" is not valid SQL so
	// it breaks sqlparser/.
	query = strings.Replace(query, "?+", "? ", -1)

	// Strip leading comments before parsing as it could cause problem
	query = sqlparser.StripLeadingComments(query)

	// Internal newlines break everything.
	query = strings.Replace(query, "\n", " ", -1)

	var pr *pg_query.ParseResult
	var s sqlparser.Statement
	var err error
	if isPg {
		pr, err = pg_query.Parse(query)
		if err != nil {
			if m.Debug {
				fmt.Println("ERROR:", err)
			}
		} else if len(pr.Stmts) == 0 {
			pr = nil
		}
	}

	if !isPg || pr == nil {
		s, err = sqlparser.NewTestParser().Parse(query)
		if err != nil {
			if m.Debug {
				fmt.Println("ERROR:", err)
			}
			return m.usePerl(query, q, err)
		}
	}

	// Parse the SQL structure. The sqlparser is rather terrible, incomplete code,
	// so it's prone to crash. If that happens, fall back to using the Perl code
	// which only gets the abstract. Be sure to re-run the parse() goroutine for
	// other callers and queries.
	try := parseTry{
		query:     query,
		q:         q,
		s:         s,
		pr:        pr,
		queryChan: make(chan QueryInfo, 1),
		crashChan: make(chan bool, 1),
	}
	m.parseChan <- try
	select {
	case q = <-try.queryChan:
	case expected := <-try.crashChan:
		if !expected {
			fmt.Printf("WARN: query crashes sqlparser: %s\n", query)
		}
		go m.parse()
		return m.usePerl(query, q, err)
	}

	if defaultDb != "" {
		for n, t := range q.Tables {
			if t.Db == "" {
				q.Tables[n].Db = defaultDb
			}
		}
		for n, t := range q.Procedures {
			if t.DB == "" {
				q.Procedures[n].DB = defaultDb
			}
		}
	}

	return q, nil
}

func (m *Mini) parse() {
	var crashChan chan bool
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok && err == ErrNotSupported {
				crashChan <- true
			} else {
				crashChan <- false
			}
		}
	}()
	for {
		select {
		case p := <-m.parseChan:
			q := p.q
			crashChan = p.crashChan
			if p.pr != nil {
				if len(p.pr.Stmts) > 0 {
					// Only parse first stmt
					stmt := p.pr.Stmts[0].Stmt
					var tables, extraTables protoTables
					switch stmt.Node.(type) {
					case *pg_query.Node_SelectStmt:
						q.Abstract = "SELECT"
					case *pg_query.Node_UpdateStmt:
						q.Abstract = "UPDATE"
					case *pg_query.Node_InsertStmt:
						q.Abstract = "INSERT"
					case *pg_query.Node_DeleteStmt:
						q.Abstract = "DELETE"
					case *pg_query.Node_CreateStmt:
						q.Abstract = "CREATE TABLE"
					case *pg_query.Node_AlterTableStmt:
						q.Abstract = "ALTER TABLE"
					case *pg_query.Node_DropStmt:
						q.Abstract = "DROP TABLE"
					case *pg_query.Node_TruncateStmt:
						q.Abstract = "TRUNCATE TABLE"
					}
					if q.Abstract == "" {
						q, _ = m.usePerl(p.query, q, ErrNotSupported)
					} else {
						tables, extraTables = getTablesFromPgNode(stmt, 0)
						q.Tables = append(q.Tables, tables...)
						q.Tables = append(q.Tables, extraTables...)
						if len(tables) > 0 {
							q.Abstract += " " + protoTables(RemoveDuplicateTables(tables)).String()
						}
					}
				}
			} else {
				switch s := p.s.(type) {
				case sqlparser.SelectStatement:
					q.Abstract = "SELECT"
					if m.Debug {
						fmt.Printf("struct: %#v\n", s)
					}
					tables, whereTables := getTablesFromSelectStmt(s, 0)
					if len(tables) > 0 {
						q.Tables = append(q.Tables, tables...)
						q.Tables = append(q.Tables, whereTables...)
						q.Abstract += " " + tables.String()
					}
				case *sqlparser.Insert:
					// REPLACEs will be recognized by sqlparser as INSERTs and the Action field
					// will have the real command
					if s.Action == sqlparser.InsertAct {
						q.Abstract = "INSERT"
					} else if s.Action == sqlparser.ReplaceAct {
						q.Abstract = "REPLACE"
					}
					if m.Debug {
						fmt.Printf("struct: %#v\n", s)
					}
					table, err := s.Table.TableName()
					if err == nil {
						protoTable := queryProto.Table{
							Db:    table.Qualifier.String(),
							Table: table.Name.String(),
						}
						q.Tables = append(q.Tables, protoTable)
						q.Abstract += " " + protoTable.String()
					}
				case *sqlparser.Update:
					q.Abstract = "UPDATE"
					if m.Debug {
						fmt.Printf("struct: %#v\n", s)
					}
					tables, whereTables := getTablesFromTableExprs(s.TableExprs)
					if len(tables) > 0 {
						q.Abstract += " " + tables.String()
					}
					if s.Where != nil {
						whereTables = append(whereTables, getTablesFromExpr(s.Where.Expr, 0)...)
					}
					q.Tables = append(q.Tables, tables...)
					q.Tables = append(q.Tables, whereTables...)
				case *sqlparser.Delete:
					q.Abstract = "DELETE"
					if m.Debug {
						fmt.Printf("struct: %#v\n", s)
					}
					tables, whereTables := getTablesFromTableExprs(s.TableExprs)
					if len(tables) > 0 {
						q.Abstract += " " + tables.String()
					}
					if s.Where != nil {
						whereTables = append(whereTables, getTablesFromExpr(s.Where.Expr, 0)...)
					}
					q.Tables = append(q.Tables, tables...)
					q.Tables = append(q.Tables, whereTables...)
				case *sqlparser.Use:
					q.Abstract = "USE"
				case *sqlparser.Show:
					sql := sqlparser.NewTrackedBuffer(nil)
					s.Format(sql)
					q.Abstract = strings.ToUpper(sql.String())
				case *sqlparser.CallProc:
					q.Abstract = "CALL"
					q.Procedures = append(q.Procedures, queryProto.Procedure{
						DB:   s.Name.Qualifier.String(),
						Name: s.Name.Name.String(),
					})
				case sqlparser.DDLStatement:
					items := []string{strings.ToUpper(s.GetAction().ToString())}
					t := s.GetTable()
					items = append(items, "TABLE")
					if t.Qualifier.String() != "" {
						items = append(items, fmt.Sprintf("%s.%s", t.Qualifier.String(), t.Name.String()))
					} else {
						items = append(items, t.Name.String())
					}
					q.Abstract = strings.Join(items, " ")
					q.Tables = append(q.Tables, queryProto.Table{
						Db:    t.Qualifier.String(),
						Table: t.Name.String(),
					})
				default:
					if m.Debug {
						fmt.Printf("unsupported type: %#v\n", p.s)
					}
					q, _ = m.usePerl(p.query, q, ErrNotSupported)
				}
			}

			// deduplicate
			q.Tables = RemoveDuplicateTables(q.Tables)
			q.Procedures = RemoveDuplicateProcedures(q.Procedures)

			p.queryChan <- q
		case <-m.stopChan:
			return
		}
	}
}

func (m *Mini) usePerl(query string, q QueryInfo, originalErr error) (QueryInfo, error) {
	if m.onlyTables {
		// Caller wants only tables but we can't get them because sqlparser
		// failed for this query.
		return q, originalErr
	}
	m.queryIn <- query
	abstract := <-m.miniOut
	q.Abstract = strings.Replace(abstract, "\n", "", -1)
	return q, nil
}

func getTablesFromPgNode(node *pg_query.Node, depth uint) (tables, extraTables protoTables) {
	if depth > MAX_EXPR_DEPTH {
		return nil, nil
	}
	depth++

	switch s := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		ts, ets := getTablesFromPgSelectStmt(s.SelectStmt, depth)
		tables = append(tables, ts...)
		extraTables = append(extraTables, ets...)
	case *pg_query.Node_UpdateStmt:
		tables = append(tables, queryProto.Table{Db: s.UpdateStmt.Relation.Schemaname, Table: s.UpdateStmt.Relation.Relname})
		for _, t := range s.UpdateStmt.TargetList {
			ts, ets := getTablesFromPgNode(t, depth)
			extraTables = append(extraTables, ts...)
			extraTables = append(extraTables, ets...)
		}
		for _, f := range s.UpdateStmt.FromClause {
			ts, ets := getTablesFromPgNode(f, depth)
			extraTables = append(extraTables, ts...)
			extraTables = append(extraTables, ets...)
		}
		if s.UpdateStmt.WithClause != nil {
			for _, t := range s.UpdateStmt.WithClause.Ctes {
				ts, ets := getTablesFromPgNode(t, depth)
				extraTables = append(extraTables, ts...)
				extraTables = append(extraTables, ets...)
			}
		}
	case *pg_query.Node_InsertStmt:
		tables = append(tables, queryProto.Table{Db: s.InsertStmt.Relation.Schemaname, Table: s.InsertStmt.Relation.Relname})
		if s.InsertStmt.SelectStmt != nil {
			ts, ets := getTablesFromPgNode(s.InsertStmt.SelectStmt, depth)
			extraTables = append(extraTables, ts...)
			extraTables = append(extraTables, ets...)
		}
		if s.InsertStmt.WithClause != nil {
			for _, t := range s.InsertStmt.WithClause.Ctes {
				ts, ets := getTablesFromPgNode(t, depth)
				extraTables = append(extraTables, ts...)
				extraTables = append(extraTables, ets...)
			}
		}
	case *pg_query.Node_DeleteStmt:
		tables = append(tables, queryProto.Table{Db: s.DeleteStmt.Relation.Schemaname, Table: s.DeleteStmt.Relation.Relname})
		if s.DeleteStmt.WithClause != nil {
			for _, t := range s.DeleteStmt.WithClause.Ctes {
				ts, ets := getTablesFromPgNode(t, depth)
				extraTables = append(extraTables, ts...)
				extraTables = append(extraTables, ets...)
			}
		}
		if s.DeleteStmt.WhereClause != nil {
			ts, ets := getTablesFromPgNode(s.DeleteStmt.WhereClause, depth)
			extraTables = append(extraTables, ts...)
			extraTables = append(extraTables, ets...)
		}
	case *pg_query.Node_CreateStmt:
		if s.CreateStmt.Relation != nil {
			tables = append(tables, queryProto.Table{Db: s.CreateStmt.Relation.Schemaname, Table: s.CreateStmt.Relation.Relname})
		}
	case *pg_query.Node_AlterTableStmt:
		if s.AlterTableStmt.Relation != nil {
			tables = append(tables, queryProto.Table{Db: s.AlterTableStmt.Relation.Schemaname, Table: s.AlterTableStmt.Relation.Relname})
		}
	case *pg_query.Node_DropStmt:
		for _, obj := range s.DropStmt.Objects {
			ts, ets := getTablesFromPgNode(obj, depth)
			tables = append(tables, ts...)
			tables = append(tables, ets...)
		}
	case *pg_query.Node_TruncateStmt:
		for _, r := range s.TruncateStmt.Relations {
			ts, ets := getTablesFromPgNode(r, depth)
			tables = append(tables, ts...)
			tables = append(tables, ets...)
		}
	case *pg_query.Node_TableLikeClause:
		tables = append(tables, queryProto.Table{Db: s.TableLikeClause.Relation.Schemaname, Table: s.TableLikeClause.Relation.Relname})
	case *pg_query.Node_FromExpr:
		for _, fe := range s.FromExpr.Fromlist {
			ts, ets := getTablesFromPgNode(fe, depth)
			tables = append(tables, ts...)
			tables = append(tables, ets...)
		}
	case *pg_query.Node_FuncCall:
		for _, arg := range s.FuncCall.Args {
			ts, ets := getTablesFromPgNode(arg, depth)
			tables = append(tables, ts...)
			tables = append(tables, ets...)
		}
	case *pg_query.Node_FuncExpr:
		for _, arg := range s.FuncExpr.Args {
			ts, ets := getTablesFromPgNode(arg, depth)
			tables = append(tables, ts...)
			tables = append(tables, ets...)
		}
	case *pg_query.Node_ResTarget:
		for _, i := range s.ResTarget.Indirection {
			ts, ets := getTablesFromPgNode(i, depth)
			extraTables = append(extraTables, ts...)
			extraTables = append(extraTables, ets...)
		}
		if s.ResTarget.Val != nil {
			ts, ets := getTablesFromPgNode(s.ResTarget.Val, depth)
			tables = append(tables, ts...)
			tables = append(tables, ets...)
		}
	case *pg_query.Node_JoinExpr:
		if s.JoinExpr.Larg != nil {
			ts, ets := getTablesFromPgNode(s.JoinExpr.Larg, depth)
			tables = append(tables, ts...)
			tables = append(tables, ets...)
		}
		if s.JoinExpr.Rarg != nil {
			ts, ets := getTablesFromPgNode(s.JoinExpr.Rarg, depth)
			tables = append(tables, ts...)
			tables = append(tables, ets...)
		}
	case *pg_query.Node_RangeVar:
		tables = append(tables, queryProto.Table{Db: s.RangeVar.Schemaname, Table: s.RangeVar.Relname})
	case *pg_query.Node_AExpr:
		if s.AExpr.Lexpr != nil {
			ts, ets := getTablesFromPgNode(s.AExpr.Lexpr, depth)
			tables = append(tables, ts...)
			tables = append(tables, ets...)
		}
		if s.AExpr.Rexpr != nil {
			ts, ets := getTablesFromPgNode(s.AExpr.Rexpr, depth)
			tables = append(tables, ts...)
			tables = append(tables, ets...)
		}
	case *pg_query.Node_SubLink:
		if s.SubLink.Subselect != nil {
			ts, ets := getTablesFromPgNode(s.SubLink.Subselect, depth)
			tables = append(tables, ts...)
			tables = append(tables, ets...)
		}
	}

	return
}

func getTablesFromPgSelectStmt(stmt *pg_query.SelectStmt, depth uint) (tables, extraTables protoTables) {
	if depth > MAX_EXPR_DEPTH {
		return nil, nil
	}
	depth++

	for _, t := range stmt.TargetList {
		ts, ets := getTablesFromPgNode(t, depth)
		tables = append(tables, ts...)
		tables = append(tables, ets...)
	}
	for _, f := range stmt.FromClause {
		ts, ets := getTablesFromPgNode(f, depth)
		tables = append(tables, ts...)
		tables = append(tables, ets...)
	}
	if stmt.WithClause != nil {
		for _, t := range stmt.WithClause.Ctes {
			ts, ets := getTablesFromPgNode(t, depth)
			extraTables = append(extraTables, ts...)
			extraTables = append(extraTables, ets...)
		}
	}
	if stmt.WhereClause != nil {
		ts, ets := getTablesFromPgNode(stmt.WhereClause, depth)
		extraTables = append(extraTables, ts...)
		extraTables = append(extraTables, ets...)
	}
	return
}

func getTablesFromTableExprs(tes sqlparser.TableExprs) (tables protoTables, whereTables protoTables) {
	for _, te := range tes {
		ts, wts := getTablesFromTableExpr(te, 0)
		tables = append(tables, ts...)
		whereTables = append(whereTables, wts...)
	}
	return tables, whereTables
}

func getTablesFromSelectStmt(ss sqlparser.SelectStatement, depth uint) (tables protoTables, whereTables protoTables) {
	if depth > MAX_JOIN_DEPTH {
		return nil, nil
	}
	depth++

	switch t := ss.(type) {
	case *sqlparser.Select:
		ts, wts := getTablesFromTableExprs(sqlparser.TableExprs(t.From))
		tables = append(tables, ts...)
		whereTables = append(whereTables, wts...)
		if t.Where != nil {
			whereTables = append(whereTables, getTablesFromExpr(t.Where.Expr, depth)...)
		}
		if t.Having != nil {
			whereTables = append(whereTables, getTablesFromExpr(t.Having.Expr, depth)...)
		}
	case *sqlparser.Union:
		lTables, lWhereTables := getTablesFromSelectStmt(t.Left, depth)
		tables = append(tables, lTables...)
		whereTables = append(whereTables, lWhereTables...)
		rTables, rWhereTables := getTablesFromSelectStmt(t.Right, depth)
		tables = append(tables, rTables...)
		whereTables = append(whereTables, rWhereTables...)
	}

	return tables, whereTables
}

func getTablesFromTableExpr(te sqlparser.TableExpr, depth uint) (tables protoTables, whereTables protoTables) {
	if depth > MAX_JOIN_DEPTH {
		return nil, nil
	}

	depth++
	switch a := te.(type) {
	case *sqlparser.AliasedTableExpr:
		switch a.Expr.(type) {
		case sqlparser.TableName:
			t := a.Expr.(sqlparser.TableName)
			db := t.Qualifier.String()
			tbl := parseTableName(t.Name.String())
			if db != "" || tbl != "" {
				table := queryProto.Table{
					Db:    db,
					Table: tbl,
				}
				tables = append(tables, table)
			}
		case *sqlparser.DerivedTable:
			ts, wts := getTablesFromSelectStmt(a.Expr.(*sqlparser.DerivedTable).Select, depth)
			tables = append(tables, ts...)
			whereTables = append(whereTables, wts...)
		}

	case *sqlparser.JoinTableExpr:
		// This case happens for JOIN clauses. It recurses to the bottom
		// of the tree via the left expressions, then it unwinds. E.g. with
		// "a JOIN b JOIN c" the tree is:
		//
		//  Left			Right
		//  a     b      c	AliasedTableExpr (case above)
		//  |     |      |
		//  +--+--+      |
		//     |         |
		//    t2----+----+	JoinTableExpr
		//          |
		//        var t (t @ depth=1) JoinTableExpr
		//
		// Code will go left twice to arrive at "a". Then it will unwind and
		// store the right-side values: "b" then "c". Because of this, if
		// MAX_JOIN_DEPTH is reached, we lose the whole tree because if we take
		// the existing right-side tables, we'll generate a misleading partial
		// list of tables, e.g. "SELECT b c".
		lTables, lWhereTables := getTablesFromTableExpr(a.LeftExpr, depth)
		tables = append(tables, lTables...)
		whereTables = append(whereTables, lWhereTables...)
		rTables, rWhereTables := getTablesFromTableExpr(a.RightExpr, depth)
		tables = append(tables, rTables...)
		whereTables = append(whereTables, rWhereTables...)
	}

	return tables, whereTables
}

func getTablesFromExpr(expr sqlparser.Expr, depth uint) (tables protoTables) {
	if expr == nil || depth > MAX_EXPR_DEPTH {
		return nil
	}

	depth++

	exprValue := reflect.ValueOf(expr)
	exprValues := []reflect.Value{}
	if exprValue.Kind() == reflect.Slice {
		for i := 0; i < exprValue.Len(); i++ {
			exprValues = append(exprValues, exprValue.Index(i).Elem())
		}
	} else {
		exprValues = append(exprValues, exprValue)
	}
	for _, exprValue := range exprValues {
		exprValue = exprValue.Elem()
		for i := 0; i < exprValue.NumField(); i++ {
			v := exprValue.Field(i)
			if v.Interface() == nil {
				continue
			}
			if v.Type().Implements(reflect.TypeOf((*sqlparser.SelectStatement)(nil)).Elem()) {
				ts, wts := getTablesFromSelectStmt(v.Interface().(sqlparser.SelectStatement), depth)
				tables = append(tables, ts...)
				tables = append(tables, wts...)
				continue
			}
			if v.Type().Implements(reflect.TypeOf((*sqlparser.Expr)(nil)).Elem()) {
				tables = append(tables, getTablesFromExpr(v.Interface().(sqlparser.Expr), depth)...)
				continue
			}
		}
	}

	return tables
}

func parseTableName(tableName string) string {
	// https://dev.mysql.com/doc/refman/5.7/en/select.html#idm140358784149168
	// You are permitted to specify DUAL as a dummy table name in situations where no tables are referenced:
	//
	// ```
	// mysql> SELECT 1 + 1 FROM DUAL;
	//         -> 2
	// ```
	// DUAL is purely for the convenience of people who require that all SELECT statements
	// should have FROM and possibly other clauses. MySQL may ignore the clauses.
	// MySQL does not require FROM DUAL if no tables are referenced.
	if tableName == "dual" {
		tableName = ""
	}
	return tableName
}

// RemoveDuplicateTables removes duplicate tables
// and returns the deduplicate tables
func RemoveDuplicateTables(tables []queryProto.Table) []queryProto.Table {
	if len(tables) == 0 {
		return tables
	}

	newTables := make([]queryProto.Table, 0)
	keysMap := make(map[string]struct{})
	for _, t := range tables {
		if _, ok := keysMap[t.String()]; !ok {
			keysMap[t.String()] = struct{}{}
			newTables = append(newTables, t)
		}
	}

	return newTables
}

// RemoveDuplicateProcedures removes duplicate procedures
// and returns the deduplicate procedures
func RemoveDuplicateProcedures(procedures []queryProto.Procedure) []queryProto.Procedure {
	if len(procedures) == 0 {
		return procedures
	}

	newProcedures := make([]queryProto.Procedure, 0)
	keysMap := make(map[string]struct{})
	for _, p := range procedures {
		if _, ok := keysMap[p.String()]; !ok {
			keysMap[p.String()] = struct{}{}
			newProcedures = append(newProcedures, p)
		}
	}

	return newProcedures
}
