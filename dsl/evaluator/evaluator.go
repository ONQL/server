package evaluator

import (
	"errors"
	"fmt"
	"onql/dsl/parser"
)

func (e *Evaluator) Eval() error {
	for {
		stmt := e.Plan.NextStatement(false)
		if stmt == nil {
			break
		}
		if err := e.EvalStatement(); err != nil {
			return err
		}
	}
	e.Result = e.Memory[e.Plan.Statements[len(e.Plan.Statements)-1].Name]
	return nil
}

func (e *Evaluator) EvalStatement() error {
	stmt := e.Plan.NextStatement(false)
	fmt.Println("Evaluating statement:", stmt.Name, "Operation:", stmt.Operation)
	if stmt == nil {
		return errors.New("no statement found")
	}
	switch stmt.Operation {
	case parser.OpAccessTable:
		if err := e.EvalTableWithContext(); err != nil {
			return err
		}
	case parser.OpAccessRelatedTable:
		if err := e.EvalRelatedTable(); err != nil {
			return err
		}
	case parser.OpAccessList:
		if err := e.EvalTableList(); err != nil {
			return err
		}
	case parser.OpAccessRow:
		if err := e.EvalTableRow(); err != nil {
			return err
		}
	case parser.OpSlice:
		if err := e.EvalSlice(); err != nil {
			return err
		}
	case parser.OpAccessField:
		if err := e.EvalTableField(); err != nil {
			return err
		}
	case parser.OpLiteral:
		if err := e.EvalLiteral(); err != nil {
			return err
		}
	case parser.OpNormalOperation:
		if err := e.EvalOperator(); err != nil {
			return err
		}
	case parser.OpStartFilter:
		if err := e.EvalFilter(); err != nil {
			return err
		}
	case parser.OpStartProjection:
		if err := e.EvalProjection(); err != nil {
			return err
		}
	case parser.OpAggregateReduce:
		if err := e.EvalAggr(); err != nil {
			return err
		}
	case parser.OpAccessJsonProperty:
		if err := e.EvalJsonProperty(); err != nil {
			return err
		}
	case parser.OpUnknownIdentifier:
		// Unknown identifier - not validated as JSON
		if err := e.EvalUnknownIdentifier(); err != nil {
			return err
		}
	default:
		return errors.New("unknown operation " + string(stmt.Operation))
	}
	return nil
}
