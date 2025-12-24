package sql

import (
	"context"
	"fmt"
	"onql/sql/evaluator"
	"onql/sql/parser"
)

// Execute executes a SQL query.
func Execute(ctx context.Context, query string) (any, error) {
	lex, err := parser.NewLexer(query)
	if err != nil {
		return nil, err
	}
	p := parser.NewParser(lex)
	stmt, err := p.Parse()
	if err != nil {
		return nil, err
	}
	if stmt == nil {
		return nil, fmt.Errorf("empty query")
	}

	return evaluator.Execute(ctx, stmt)
}
