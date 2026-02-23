package evaluator

import (
	"errors"
	"fmt"
	"onql/dsl/parser"
)

func (e *Evaluator) EvalProjection() error {
	endProjectionName := ""
	pStmt := e.Plan.NextStatement(true)
	if pStmt.Operation != parser.OpStartProjection {
		return errors.New("expected start projection operation")
	}

	var endProjectionPos int

	// Resolve source data — accept []map[string]any or []any (e.g. from EvalSlice)
	var tableData []map[string]any
	sourceValue := e.Memory[pStmt.Sources[0].SourceValue]

	switch sv := sourceValue.(type) {
	case []map[string]any:
		tableData = sv
	case nil:
		tableData = make([]map[string]any, 0)
	case []any:
		// EvalSlice always emits []any; convert each element assuming map[string]any rows
		tableData = make([]map[string]any, 0, len(sv))
		for _, item := range sv {
			row, ok := item.(map[string]any)
			if !ok {
				return fmt.Errorf("projection: expected map row in []any, got %T", item)
			}
			tableData = append(tableData, row)
		}
	case map[string]any:
		// Single row from row-access or context sub-query result
		tableData = []map[string]any{sv}
	case []string:
		if len(sv) > 0 {
			return fmt.Errorf("projection: cannot project string list as map rows")
		}
		tableData = make([]map[string]any, 0)
	case []float64:
		if len(sv) > 0 {
			return fmt.Errorf("projection: cannot project float list as map rows")
		}
		tableData = make([]map[string]any, 0)
	case []int64:
		if len(sv) > 0 {
			return fmt.Errorf("projection: cannot project int list as map rows")
		}
		tableData = make([]map[string]any, 0)
	case bool:
		// Boolean left in memory by a filter expression — treat as no rows
		tableData = make([]map[string]any, 0)
	default:
		return fmt.Errorf("projection: expected table data, got %T", sourceValue)
	}

	pos := e.Plan.Pos
	result := make([]map[string]any, 0, len(tableData))

	if len(tableData) == 0 {
		// Fast-skip: consume all statements until the matching OpEndProjection.
		// Track nesting only for nested projections (OpStartProjection/OpEndProjection pairs).
		nested := 0
		found := false
		for !found {
			stmt := e.Plan.NextStatement(true)
			if stmt == nil {
				return fmt.Errorf("projection: unexpected end of plan, expected '}'")
			}
			switch stmt.Operation {
			case parser.OpStartProjection:
				nested++
			case parser.OpEndProjection:
				if nested > 0 {
					nested--
				} else {
					endProjectionName = stmt.Name
					// NextStatement(true) already advanced past EPJ; subtract 1 so
					// the final NextStatement(true) below lands on EPJ and moves past it.
					endProjectionPos = e.Plan.Pos - 1
					found = true
				}
			}
		}
	} else {
		for _, row := range tableData {
			e.SetMemoryValue(pStmt.Name, row)
			obj := make(map[string]any)

			for {
				stmt := e.Plan.NextStatement(false)
				switch stmt.Operation {
				case parser.OpStartProjectionKey:
					e.Memory[stmt.Name] = row
					obj[stmt.Expressions.(string)] = ""
					e.Plan.NextStatement(true)
				case parser.OpEndProjectionKey:
					e.Plan.NextStatement(true)
					prevStmt := e.Plan.PrevStatement(false)
					obj[stmt.Expressions.(string)] = e.Memory[prevStmt.Name]
				case parser.OpEndProjection:
					endProjectionName = stmt.Name
					endProjectionPos = e.Plan.Pos
				default:
					if err := e.EvalStatement(); err != nil {
						return err
					}
				}
				if endProjectionPos > 0 {
					break
				}
			}

			result = append(result, obj)
			e.Plan.Pos = pos
			endProjectionPos = 0 // reset so next row's inner loop works
		}
	}

	e.Plan.Pos = endProjectionPos
	e.Plan.NextStatement(true) // move past OpEndProjection
	e.SetMemoryValue(endProjectionName, result)
	return nil
}
