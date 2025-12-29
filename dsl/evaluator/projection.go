package evaluator

import (
	"errors"
	"fmt"
	"onql/dsl/parser"
)

func (e *Evaluator) EvalProjection() error {
	// Implement projection logic here
	endProjectionName := ""
	pStmt := e.Plan.NextStatement(true)
	if pStmt.Operation != parser.OpStartProjection {
		return errors.New("expected start projection operation")
	}
	result := make([]map[string]any, 0)
	// stmt = e.Plan.NextStatement(true)
	pos := e.Plan.Pos
	var endProjectionPos int
	//handle unwanted datatypes
	// _,ok := e.Memory[pStmt.Sources[0].SourceValue].([]map[string]any)
	// Continue with projection logic

	// Handle potentially empty slice from previous operations
	var tableData []map[string]any
	sourceValue := e.Memory[pStmt.Sources[0].SourceValue]

	if val, ok := sourceValue.([]map[string]any); ok {
		tableData = val
	} else if val, ok := sourceValue.([]any); ok && len(val) == 0 {
		tableData = make([]map[string]any, 0)
	} else {
		return errors.New("expect table data in projection but got " + fmt.Sprintf("%T", sourceValue))
	}
	
	if len(tableData) == 0 {
		nested := 0
		for {
			stmt := e.Plan.NextStatement(true)
			if stmt == nil {
				return fmt.Errorf("expect } but got empty in projection")
			}
			if stmt.Operation == parser.OpEndProjection {
				if nested > 0 {
					nested -= 1
					continue
				}
				endProjectionName = stmt.Name
				endProjectionPos = e.Plan.Pos
				break
			}
			if stmt.Operation == parser.OpStartProjection {
				nested += 1
			}
		}
	} else {
		for _, row := range tableData {
			// e.Memory[pStmt.Name] = row
			e.SetMemoryValue(pStmt.Name, row)
			obj := make(map[string]any)
			// Apply projection conditions
			for {
				stmt := e.Plan.NextStatement(false)
				fmt.Println(stmt)
				// print(stmt.Name, "Operation:", stmt.Operation)
				if stmt.Operation == parser.OpStartProjectionKey {
					e.Memory[stmt.Name] = row
					obj[stmt.Expressions.(string)] = ""
					e.Plan.NextStatement(true) // move to next statement
				} else if stmt.Operation == parser.OpEndProjectionKey {
					e.Plan.NextStatement(true) // move to next statement
					prevStmt := e.Plan.PrevStatement(false)
					obj[stmt.Expressions.(string)] = e.Memory[prevStmt.Name]
				} else if stmt.Operation == parser.OpEndProjection {
					endProjectionName = stmt.Name
					endProjectionPos = e.Plan.Pos
					break
				} else {
					err := e.EvalStatement()
					if err != nil {
						return err
					}
				}
			}
			result = append(result, obj)
			e.Plan.Pos = pos
		}
	}
	e.Plan.Pos = endProjectionPos
	e.Plan.NextStatement(true) // Move past OpEndProjection
	// e.Memory[endProjectionName] = result
	e.SetMemoryValue(endProjectionName, result)
	// e.Memory[pStmt.Name] = result
	return nil
}
