package evaluator

import (
	"errors"
	"fmt"
	"onql/database"
	"onql/dsl/parser"
	"onql/storemanager"
	"strconv"
	"strings"
)

// func (e *Evaluator) GenFilters() []string {
// 	stmt := e.Plan.NextStatement(true)
// 	if stmt == nil {
// 		return nil
// 	}
// 	if stmt.Operation != parser.OpStartFilter {
// 		return nil
// 	}
// 	filters := make([]string, 0)
// 	col := [2]string{}
// 	for {

// 		stmt = e.Plan.NextStatement(true)
// 		if stmt.Operation == parser.OpEndFilter {
// 			break
// 		}
// 		if stmt.Operation != parser.OpNormalOperation && stmt.Operation != parser.OpLiteral && stmt.Operation != parser.OpAccessList {
// 			return nil
// 		}
// 		if stmt.Operation == parser.OpNormalOperation {
// 			op := strings.ToLower(strings.Split(stmt.Expressions.(string), " ")[1])
// 			if op != "and" && op != "or" && op != "=" {
// 				return nil
// 			}
// 			if op == "=" {
// 				continue
// 			}
// 			filters = append(filters, op)
// 		}
// 		if len(col) == 2 {
// 			filters = append(filters, col[0]+":"+col[1])
// 			col = [2]string{}
// 		}
// 		if stmt.Operation == parser.OpAccessList {
// 			col[0] = stmt.Meta["name"]
// 		}
// 		if stmt.Operation == parser.OpLiteral {
// 			col[1] = stmt.Expressions.(string)
// 		}
// 	}
// 	return filters
// }

func (e *Evaluator) GenFilters() []string {
	stmt := e.Plan.NextStatement(true)
	if stmt == nil || stmt.Operation != parser.OpStartFilter {
		return nil
	}

	filters := make([]string, 0, 8)
	var colName, colVal string

	flush := func() {
		if colName == "" || colVal == "" {
			return
		}
		v := strings.TrimSpace(colVal)
		// strip quotes if present
		if n := len(v); n >= 2 && ((v[0] == '"' && v[n-1] == '"') || (v[0] == '\'' && v[n-1] == '\'')) {
			v = v[1 : n-1]
		}
		filters = append(filters, colName+":"+v)
		colName, colVal = "", ""
	}

	for {
		stmt = e.Plan.NextStatement(true)
		if stmt == nil {
			break
		}
		if stmt.Operation == parser.OpEndFilter {
			flush()
			break
		}

		switch stmt.Operation {
		case parser.OpAccessList:
			// left side (column)
			colName = stmt.Meta["name"]

		case parser.OpLiteral:
			// right side (value) for '='
			colVal = stmt.Expressions.(string)

		case parser.OpNormalOperation:
			// expecting one of: "=", "==", "and", "or"
			op := strings.ToLower(strings.TrimSpace(stmt.Expressions.(string)))
			switch op {
			case "=", "==":
				// do nothing here; we'll flush when literal arrives
			case "and", "or":
				flush() // ensure previous expr emitted
				filters = append(filters, op)
			default:
				return nil
				// ignore anything else (e.g., "!=" not supported)
			}
		default:
			return nil
		}
		if colName != "" && colVal != "" {
			flush()
		}
	}
	return filters
}

func (e *Evaluator) EvalTableWithContext() error {
	// Implement table evaluation logic here
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpAccessTable {
		return errors.New("expected access table operation")
	}
	// if e.ContextKey == "" {
	// 	e.Plan.PrevStatement(true) //move back
	// 	return e.EvalTable()
	// }
	sources := strings.Split(stmt.Sources[0].SourceValue, ".")
	cntxQuery, err := database.GetProtoContext(e.Plan.ProtocolPass, sources[1], e.ContextKey)
	if err != nil {
		return err
	}
	if cntxQuery == "" {
		// e.Plan.PrevStatement(true) //move back
		//moveback forcefully
		e.Plan.Pos = e.Plan.Pos - 1
		return e.EvalTable()
	}
	for i, v := range e.ContextValues {
		replacement := v
		if !(strings.HasPrefix(v, "\"") && strings.HasSuffix(v, "\"")) {
			replacement = "\"" + v + "\""
		}
		cntxQuery = strings.Replace(cntxQuery, "$"+strconv.Itoa(i+1), replacement, 1)
	}
	lexer := parser.NewLexer(cntxQuery)
	plan := parser.NewPlan(lexer, e.Plan.ProtocolPass)
	err = plan.Parse()
	if err != nil {
		return err
	}
	eval := NewEvaluator(e.Ctx, plan, "", []string{cntxQuery})
	err = eval.Eval()
	if err != nil {
		return err
	}
	e.SetMemoryValue(stmt.Name, eval.Result)
	// e.Memory[stmt.Name] = eval.Result
	// e.Memory[stmt.Name+"_meta_structure_type"] = getStructureType(eval.Result)
	return nil
}

func (e *Evaluator) EvalTable() error {
	// Implement table evaluation logic here
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpAccessTable {
		return errors.New("expected access table operation")
	}
	// if next statement is start filteration and have
	pos := e.Plan.Pos
	filters := e.GenFilters()
	var data []map[string]any
	var err error
	if filters != nil {
		data, err = GetTableWithDataWithFilters(stmt.Meta["db"], stmt.Meta["table"], filters)
	} else {
		// Continue with table evaluation logic
		data, err = GetTableData(stmt.Meta["db"], stmt.Meta["table"])
	}
	e.Plan.Pos = pos
	if err != nil {
		return err
	}
	// e.Memory[stmt.Name] = data
	e.SetMemoryValue(stmt.Name, data)
	// fmt.Println(data)
	// e.Memory[stmt.Name] = data
	return nil
}

func (e *Evaluator) EvalRelatedTable() error {
	// Implement related table evaluation logic here
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpAccessRelatedTable {
		return errors.New("expected access table operation")
	}

	fkKey := strings.Split(stmt.Expressions.(*storemanager.Relation).FKField, ":")[0]
	result := make([]map[string]any, 0)
	if e.isUnderFilter(stmt.Name) || e.IsUnderProjection(stmt.Name) {
		val, ok := e.Memory[stmt.Sources[1].SourceValue].(map[string]any)
		if !ok {
			return fmt.Errorf("host table data not found for getting related table data")
		}
		// data, err := GetRelatedTableData(stmt.Meta["db"], *stmt.Expressions.(*storemanager.Relation), val[fkKey].(string))
		// handle non string types
		var fkValue string
		switch v := val[fkKey].(type) {
		case string:
			fkValue = v
		case float64:
			fkValue = strconv.FormatFloat(v, 'f', -1, 64)
		case int:
			fkValue = strconv.Itoa(v)
		default:
			fkValue = fmt.Sprintf("%v", v)
		}
		data, err := GetRelatedTableData(stmt.Meta["db"], *stmt.Expressions.(*storemanager.Relation), fkValue)
		if err != nil {
			return err
		}
		result = append(result, data...)
	} else {
		tabledata, ok := e.Memory[stmt.Sources[1].SourceValue].([]map[string]any)
		if !ok {
			return fmt.Errorf("host table data not found for getting related table data")
		}
		for _, val := range tabledata {
			// data, err := GetRelatedTableData(stmt.Meta["db"], *stmt.Expressions.(*storemanager.Relation), val[fkKey].(string))
			var fkValue string
			switch v := val[fkKey].(type) {
			case string:
				fkValue = v
			case float64:
				fkValue = strconv.FormatFloat(v, 'f', -1, 64)
			case int:
				fkValue = strconv.Itoa(v)
			default:
				fkValue = fmt.Sprintf("%v", v)
			}
			data, err := GetRelatedTableData(stmt.Meta["db"], *stmt.Expressions.(*storemanager.Relation), fkValue)
			if err != nil {
				return err
			}
			result = append(result, data...)
		}
	}

	// e.Memory[stmt.Name] = data
	e.SetMemoryValue(stmt.Name, result)
	return nil
}

func (e *Evaluator) EvalTableList() error {
	// Implement table list evaluation logic here
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpAccessList {
		return errors.New("expected access table list operation")
	}
	if e.isUnderFilter(stmt.Name) || e.IsUnderProjection(stmt.Name) {
		e.Plan.PrevStatement(true)
		return e.EvalTableField()
	}
	sourceName := stmt.Sources[0].SourceValue
	list := make([]string, 0)
	listNum := make([]float64, 0)
	listOther := make([]any, 0)
	stmtMetadataType := strings.ToUpper(stmt.Meta["type"])
	for _, item := range e.Memory[sourceName].([]map[string]any) {
		if val, ok := item[stmt.Meta["name"]]; ok {
			if stmtMetadataType == "NUMBER" || stmtMetadataType == "TIMESTAMP" {
				// num, err := strconv.ParseFloat(val.(string), 64)
				// if err != nil {
				// return err
				// }
				num := val.(float64)
				listNum = append(listNum, num)
			} else if stmtMetadataType == "STRING" {
				list = append(list, val.(string))
			} else {
				listOther = append(listOther, val)
			}
		}
	}
	if stmtMetadataType == "NUMBER" || stmtMetadataType == "TIMESTAMP" {
		// e.Memory[stmt.Name] = listNum
		e.SetMemoryValue(stmt.Name, listNum)
	} else if stmtMetadataType == "STRING" {
		// e.Memory[stmt.Name] = list
		e.SetMemoryValue(stmt.Name, list)
	} else {
		e.SetMemoryValue(stmt.Name, listOther)
	}
	// e.Memory[stmt.Name+"_meta_structure_type"] = "LIST"
	// e.Memory[stmt.Name+"_meta_type"] = stmt.Meta["type"]
	return nil
}

func (e *Evaluator) EvalTableRow() error {
	// Implement table row evaluation logic here
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpAccessRow {
		return errors.New("expected access table row operation")
	}
	sourceName := stmt.Sources[0].SourceValue
	// row := make(map[string]any)
	// if e.Plan.StatementMap[stmt.Expressions.(string)].Operation == parser.OpUnknownIdentifier {

	// } else {

	// }
	switch e.Memory[sourceName].(type) {
	case []map[string]any:
		row2 := e.Memory[sourceName].([]map[string]any)[stmt.Expressions.(int64)]
		// e.Memory[stmt.Name] = row2
		e.SetMemoryValue(stmt.Name, row2)
	case []any:
		row2 := e.Memory[sourceName].([]any)[stmt.Expressions.(int64)]
		e.SetMemoryValue(stmt.Name, row2)
	case []string:
		e.SetMemoryValue(stmt.Name, e.Memory[sourceName].([]string)[stmt.Expressions.(int64)])
		// e.Memory[stmt.Name+"_meta_type"] = "STRING"
	case []float64, []int64:
		e.SetMemoryValue(stmt.Name, e.Memory[sourceName].([]float64)[stmt.Expressions.(int64)])
		// e.Memory[stmt.Name+"_meta_type"] = "NUMBER"
	default:
		return errors.New("expected array for row access for table row access")
	}
	return nil
}

func (e *Evaluator) EvalTableField() error {
	// Implement table field evaluation logic here
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpAccessField && (stmt.Operation != parser.OpAccessList || (!e.isUnderFilter(stmt.Name) && !e.IsUnderProjection(stmt.Name))) {
		return errors.New("expected access table field operation")
	}
	sourceName := stmt.Sources[0].SourceValue

	field := e.Memory[sourceName].(map[string]any)[stmt.Meta["name"]]
	// e.Memory[stmt.Name] = field
	// e.Memory[stmt.Name+"_meta_type"] = stmt.Meta["type"]
	e.SetMemoryValue(stmt.Name, field)
	return nil
}

func (e *Evaluator) EvalLiteral() error {
	// Implement literal evaluation logic here
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpLiteral {
		return errors.New("expected literal operation")
	}
	if stmt.Meta["type"] == "NUMBER" {
		num, err := strconv.ParseFloat(stmt.Expressions.(string), 64)
		if err != nil {
			return err
		}
		// e.Memory[stmt.Name] = num
		e.SetMemoryValue(stmt.Name, num)
	} else {
		e.SetMemoryValue(stmt.Name, stmt.Expressions)
	}
	// e.Memory[stmt.Name+"_meta_type"] = stmt.Meta["type"]
	return nil
}
