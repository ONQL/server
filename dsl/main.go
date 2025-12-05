package dsl

import (
	"errors"
	"fmt"
	"onql/dsl/evaluator"
	"onql/dsl/parser"
	"runtime/debug"
	"strings"
)

// func Execute(protoPass string, query string, ctxKey string, ctxValues []string) (any, error) {

//	if protoPass == "" {
//		return nil, errors.New("protocol pass required")
//	}
//	if query == "" {
//		return nil, errors.New("query required")
//	}
//	// Implement the execution logic here
//	lexer := parser.NewLexer(query)
//	plan := parser.NewPlan(lexer, protoPass)
//	err := plan.Parse()
//	if err != nil {
//		return nil, err
//	}
//	evaluator := evaluator.NewEvaluator(plan, ctxKey, ctxValues)
//	err = evaluator.Eval()
//	printStatements(evaluator.Plan.Statements)
//	if err != nil {
//		return nil, err
//	}

//	return evaluator.Result, nil
// }

func Execute(protoPass string, query string, ctxKey string, ctxValues []string) (res any, err error) {
	// Catch ANY panic in this goroutine and return it as an error
	defer func() {
		if r := recover(); r != nil {
			// convert recovered value to error and include stack for debugging
			var perr error
			switch x := r.(type) {
			case error:
				perr = x
			default:
				perr = fmt.Errorf("%v", x)
			}
			// err = fmt.Errorf("execute recovered panic: %w", perr)
			err = fmt.Errorf("execute recovered panic: %w\n%s", perr, debug.Stack())
			res = nil
		}
	}()

	if protoPass == "" {
		return nil, errors.New("protocol pass required")
	}
	if query == "" {
		return nil, errors.New("query required")
	}

	lexer := parser.NewLexer(query)
	plan := parser.NewPlan(lexer, protoPass)
	if err = plan.Parse(); err != nil {
		return nil, err
	}

	ev := evaluator.NewEvaluator(plan, ctxKey, ctxValues)
	if err = ev.Eval(); err != nil {
		return nil, err
	}

	printStatements(ev.Plan.Statements)
	return ev.Result, nil
}

func ExecuteByOnqlAssembly(ev *evaluator.Evaluator) (res any, err error) {
	// Catch ANY panic in this goroutine and return it as an error
	defer func() {
		if r := recover(); r != nil {
			// convert recovered value to error and include stack for debugging
			var perr error
			switch x := r.(type) {
			case error:
				perr = x
			default:
				perr = fmt.Errorf("%v", x)
			}
			// err = fmt.Errorf("execute recovered panic: %w", perr)
			err = fmt.Errorf("execute recovered panic: %w\n%s", perr, debug.Stack())
			res = nil
		}
	}()

	if err = ev.Eval(); err != nil {
		return nil, err
	}
	// printStatements(ev.Plan.Statements)
	return ev.Result, nil
}

func printStatements(stmts []*parser.Statement) {
	fmt.Printf("%-8s %-8s %-30s %-20s\n", "Name", "Operation", "Sources", "Expressions")
	fmt.Println(strings.Repeat("-", 80))
	for _, stmt := range stmts {
		// Collect sources as a comma-separated string
		var sources []string
		for _, source := range stmt.Sources {
			if source.SourceType == "" {
				continue
			}
			sources = append(sources, fmt.Sprintf("%s:%s", source.SourceType, source.SourceValue))
		}
		// Print statement info
		fmt.Printf("%-8s %-8s %-30s %-20v\n",
			stmt.Name,
			stmt.Operation,
			strings.Join(sources, ", "),
			stmt.Expressions,
		)
	}
}
