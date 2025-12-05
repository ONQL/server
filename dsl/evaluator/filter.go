package evaluator

import (
	"errors"
	"fmt"
	"onql/dsl/parser"
	"strconv"
	"strings"
)

func (e *Evaluator) EvalFilter() error {
	// Implement filtering logic here
	filterStmt := e.Plan.NextStatement(true)
	if filterStmt.Operation != parser.OpStartFilter {
		return errors.New("expected start filter operation")
	}
	result := make([]map[string]any, 0)
	// stmt := e.Plan.NextStatement(true)
	pos := e.Plan.Pos
	var endFilterPos int
	var endFilterName string
	// Continue with filtering logic
	// fmt.Println(e.Memory)
	// fmt.Println(filterStmt.Sources[0].SourceValue)
	//get data from start filter table
	tableData, ok := e.Memory[filterStmt.Sources[0].SourceValue].([]map[string]any)
	if !ok {
		return errors.New("expect table data for filter but got " + fmt.Sprintf("%T", e.Memory[filterStmt.Sources[0].SourceValue]))
	}
	if len(tableData) == 0 {
		nested := 0
		for {
			stmt := e.Plan.NextStatement(true)
			if stmt == nil {
				return fmt.Errorf("expect ] but got empty in projection")
			}
			if stmt.Operation == parser.OpEndFilter {
				if nested > 0 {
					nested -= 1
					continue
				}
				endFilterPos = e.Plan.Pos
				endFilterName = stmt.Name
				break
			}
			if stmt.Operation == parser.OpStartFilter {
				nested += 1
			}
		}
	} else {
		for _, row := range tableData {
			// Apply filter conditions
			// e.Memory[filterStmt.Name] = row
			e.SetMemoryValue(filterStmt.Name, row)
			for {
				stmt := e.Plan.NextStatement(false)
				if stmt == nil || stmt.Operation == parser.OpEndFilter {
					endFilterPos = e.Plan.Pos
					endFilterName = stmt.Name
					e.Plan.NextStatement(true) // move to next statement
					break
				}
				err := e.EvalStatement()
				if err != nil {
					return err
				}
			}
			prevStmt := e.Plan.PrevStatement(false)
			conditionResult := e.Memory[prevStmt.Name].(bool)
			if conditionResult {
				result = append(result, row)
			}
			// Restore original position for next filter
			e.Plan.Pos = pos
		}
	}
	e.Plan.Pos = endFilterPos
	e.Plan.NextStatement(true) // Move past OpEndFilter
	// e.Memory[endFilterName] = result
	e.SetMemoryValue(endFilterName, result)
	fmt.Println(result)
	return nil
}

func (e *Evaluator) isUnderFilter(varName string) bool {

	stmt := e.Plan.StatementMap[varName]
	if stmt.Operation == parser.OpAccessRelatedTable {
		stmt = e.Plan.StatementMap[e.Plan.StatementMap[varName].Sources[1].SourceValue]
	} else {
		stmt = e.Plan.StatementMap[e.Plan.StatementMap[varName].Sources[0].SourceValue]
	}
	return stmt.Operation == parser.OpStartFilter
}

func (e *Evaluator) IsUnderProjection(varName string) bool {
	// stmt := e.Plan.StatementMap[e.Plan.StatementMap[varName].Sources[0].SourceValue]
	stmt := e.Plan.StatementMap[varName]
	if stmt.Operation == parser.OpAccessRelatedTable {
		stmt = e.Plan.StatementMap[e.Plan.StatementMap[varName].Sources[1].SourceValue]
	} else {
		stmt = e.Plan.StatementMap[e.Plan.StatementMap[varName].Sources[0].SourceValue]
	}
	return stmt.Operation == parser.OpStartProjectionKey || stmt.Operation == parser.OpStartProjection
}

func (e *Evaluator) EvalSlice() error {
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpSlice {
		return fmt.Errorf("expected slice operation")
	}
	sliceParts := strings.Split(stmt.Expressions.(string), ":")
	data := e.Memory[stmt.Sources[0].SourceValue]

	var arr []any

	switch s := data.(type) {
	case []map[string]any:
		arr = make([]any, len(s))
		for i, v := range s {
			arr[i] = v
		}
	case []string:
		arr = make([]any, len(s))
		for i, v := range s {
			arr[i] = v
		}
	case []float64:
		arr = make([]any, len(s))
		for i, v := range s {
			arr[i] = v
		}
	case []int64:
		arr = make([]any, len(s))
		for i, v := range s {
			arr[i] = v // or float64(v)
		}
	case []any:
		arr = s
	default:
		return errors.New("expected array for row access for table row access")
	}

	// Parse slice indices
	start, end, step := 0, len(arr), 1
	var err error

	if len(sliceParts) > 0 && sliceParts[0] != "" {
		start, err = strconv.Atoi(sliceParts[0])
		if err != nil {
			return fmt.Errorf("invalid start index: %v", err)
		}
		if start < 0 {
			start = len(arr) + start
		}
	}
	if len(sliceParts) > 1 && sliceParts[1] != "" {
		end, err = strconv.Atoi(sliceParts[1])
		if err != nil {
			return fmt.Errorf("invalid end index: %v", err)
		}
		if end < 0 {
			end = len(arr) + end
		}
	}
	if len(sliceParts) > 2 && sliceParts[2] != "" {
		step, err = strconv.Atoi(sliceParts[2])
		if err != nil {
			return fmt.Errorf("invalid step: %v", err)
		}
		if step == 0 {
			step = 1
		}
	}

	// Clamp indices
	if start < 0 {
		start = 0
	}
	if start > len(arr) {
		start = len(arr)
	}
	if end > len(arr) {
		end = len(arr)
	}
	if end < 0 {
		end = 0
	}
	// INFO: We can support backward slicing with negative step in future
	if start > end {
		return errors.ErrUnsupported
	}
	// Build the sliced result
	result := make([]any, 0)
	for i := start; i < end; i += step {
		result = append(result, arr[i])
	}

	// result2,ok := result.([]map[string]any)

	// e.Memory[stmt.Name] = result
	e.SetMemoryValue(stmt.Name, result)
	return nil
}
