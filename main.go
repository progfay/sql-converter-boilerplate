package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	_ "github.com/pingcap/tidb/types/parser_driver"
	// _ "github.com/pingcap/parser/test_driver"
)

/* Begin of Customize Area */

const (
	maxCapacity = 1024 * 1024 * 1024
	filename    = "seed.sql"
)

func convertInsertStmt(node *ast.InsertStmt) (string, error) {
	table, err := getInsertStmtTableName(node)
	if err != nil {
		return "", NoChangeErr
	}

	switch table {
	case "users":
		for i := 0; i < len(node.Lists); i++ {
			valueExpr, _ := node.Lists[i][0].(ast.ValueExpr)
			v, _ := valueExpr.GetValue().(int64)
			valueExpr.SetValue(v + 1000)
		}

	case "items":
		for i := 0; i < len(node.Lists); i++ {
			valueExpr, _ := node.Lists[i][2].(ast.ValueExpr)
			v, _ := valueExpr.GetValue().(string)
			valueExpr.SetValue(v[:5])
		}

	case "icon":
		for i := 0; i < len(node.Lists); i++ {
			node.Lists[i] = append(node.Lists[i][:2])
		}

	default:
		return "", NoChangeErr
	}

	var w bytes.Buffer
	restore(&w, node)
	return w.String(), nil
}

/* End of Customize Area */

var (
	NoChangeErr = fmt.Errorf("no change")
	onlyCommentErr = errors.New("comment only")
)

func getInsertStmtTableName(node *ast.InsertStmt) (string, error) {
	tableSource, ok := node.Table.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return "", errors.New("table name is not found in *ast.InsertStmt")
	}

	tableName, ok := tableSource.Source.(*ast.TableName)
	if !ok {
		return "", errors.New("table name is not found in *ast.InsertStmt")
	}

	return tableName.Name.String(), nil
}

func convert(node ast.StmtNode) (string, error) {
	switch node.(type) {
	case *ast.InsertStmt:
		node := node.(*ast.InsertStmt)
		return convertInsertStmt(node)

	default:
		return "", NoChangeErr
	}
}

func restore(w io.Writer, stmt interface {
	Restore(*format.RestoreCtx) error
}) error {
	ctx := format.NewRestoreCtx(format.RestoreStringDoubleQuotes|format.RestoreKeyWordUppercase|format.RestoreNameLowercase|format.RestoreNameBackQuotes, w)
	err := stmt.Restore(ctx)
	return err
}

func parseStatement(statement string) (ast.StmtNode, error) {
	p := parser.New()
	nodes, _, err := p.Parse(statement, "", "")
	if err != nil {
		return nil, fmt.Errorf("error occurred: %w", err)
	}

	if len(nodes) == 0 {
		return nil, onlyCommentErr
	}

	if len(nodes) > 1 {
		return nil, fmt.Errorf("StatementScanner.Text() return SQL Query with multiple statements: %q", statement)
	}

	return nodes[0], nil
}

func newStatementScanner(r io.Reader, bufferSize int) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		var insideSingleQuote, insideDoubleQuote, insideBackQuote, escape bool
		for i := 0; i < len(data); i++ {
			if escape {
				escape = false
				continue
			}

			switch data[i] {
			case ';':
				if insideSingleQuote || insideDoubleQuote || insideBackQuote {
					continue
				}
				return i + 1, data[:i], nil

			case '\\':
				escape = true

			case '\'':
				insideSingleQuote = !insideSingleQuote

			case '"':
				insideDoubleQuote = !insideDoubleQuote

			case '`':
				insideBackQuote = !insideBackQuote
			}
		}

		return 0, data, bufio.ErrFinalToken
	})
	buf := make([]byte, bufferSize)
	scanner.Buffer(buf, bufferSize)
	return scanner
}

func main() {
	src, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	scanner := newStatementScanner(src, maxCapacity)

	for scanner.Scan() {
		statement := scanner.Text()

		node, err := parseStatement(statement)
		if err != nil {
			if !errors.Is(err, onlyCommentErr) {
				log.Println(err)
			}

			fmt.Print(statement + ";")
			continue
		}

		converted, err := convert(node)
		if err != nil {
			if !errors.Is(err, NoChangeErr) {
				log.Println(err)
			}

			fmt.Print(statement + ";")
			continue
		}

		fmt.Println("\n" + converted + ";")
	}
}
