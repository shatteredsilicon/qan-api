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
	"strings"

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
	query     string
	q         QueryInfo
	s         sqlparser.Statement
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

func (m *Mini) Parse(fingerprint, example, defaultDb string) (QueryInfo, error) {
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

	// Internal newlines break everything.
	query = strings.Replace(query, "\n", " ", -1)

	s, err := sqlparser.Parse(query)
	if err != nil {
		if m.Debug {
			fmt.Println("ERROR:", err)
		}
		return m.usePerl(query, q, err)
	}

	// Parse the SQL structure. The sqlparser is rather terrible, incomplete code,
	// so it's prone to crash. If that happens, fall back to using the Perl code
	// which only gets the abstract. Be sure to re-run the parse() goroutine for
	// other callers and queries.
	try := parseTry{
		query:     query,
		q:         q,
		s:         s,
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
			switch s := p.s.(type) {
			case sqlparser.SelectStatement:
				q.Abstract = "SELECT"
				if m.Debug {
					fmt.Printf("struct: %#v\n", s)
				}
				tables := getTablesFromSelectStmt(s, 0)
				if len(tables) > 0 {
					q.Tables = append(q.Tables, tables...)
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
				table := queryProto.Table{
					Db:    s.Table.Qualifier.String(),
					Table: s.Table.Name.String(),
				}
				q.Tables = append(q.Tables, table)
				q.Abstract += " " + table.String()
			case *sqlparser.Update:
				q.Abstract = "UPDATE"
				if m.Debug {
					fmt.Printf("struct: %#v\n", s)
				}
				tables := getTablesFromTableExprs(s.TableExprs)
				if len(tables) > 0 {
					q.Tables = append(q.Tables, tables...)
					q.Abstract += " " + tables.String()
				}
			case *sqlparser.Delete:
				q.Abstract = "DELETE"
				if m.Debug {
					fmt.Printf("struct: %#v\n", s)
				}
				tables := getTablesFromTableExprs(s.TableExprs)
				if len(tables) > 0 {
					q.Tables = append(q.Tables, tables...)
					q.Abstract += " " + tables.String()
				}
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

func getTablesFromTableExprs(tes sqlparser.TableExprs) (tables protoTables) {
	for _, te := range tes {
		tables = append(tables, getTablesFromTableExpr(te, 0)...)
	}
	return tables
}

func getTablesFromSelectStmt(ss sqlparser.SelectStatement, depth uint) (sTables protoTables) {
	if depth > MAX_JOIN_DEPTH {
		return nil
	}
	depth++

	switch t := ss.(type) {
	case *sqlparser.Select:
		sTables = append(sTables, getTablesFromTableExprs(sqlparser.TableExprs(t.From))...)
	case *sqlparser.Union:
		sTables = append(sTables, getTablesFromSelectStmt(t.Left, depth)...)
		sTables = append(sTables, getTablesFromSelectStmt(t.Right, depth)...)
	}

	return sTables
}

func getTablesFromTableExpr(te sqlparser.TableExpr, depth uint) (tables protoTables) {
	if depth > MAX_JOIN_DEPTH {
		return nil
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
			tables = append(tables, getTablesFromSelectStmt(a.Expr.(*sqlparser.DerivedTable).Select, depth)...)
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
		tables = append(tables, getTablesFromTableExpr(a.LeftExpr, depth)...)
		tables = append(tables, getTablesFromTableExpr(a.RightExpr, depth)...)
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
