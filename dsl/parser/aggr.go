package parser

import (
	"fmt"
)

var AggrRegistry = map[string]map[string]string{
	"_sum":    {"LIST": "NUMBER", "TABLE": "NUMBER", "JSON": "NUMBER"},
	"_count":  {"LIST": "NUMBER", "TABLE": "NUMBER", "JSON": "NUMBER"},
	"_avg":    {"LIST": "NUMBER", "JSON": "NUMBER"},
	"_min":    {"LIST": "NUMBER", "JSON": "NUMBER"},
	"_max":    {"LIST": "NUMBER", "JSON": "NUMBER"},
	"_unique": {"LIST": "LIST", "TABLE": "TABLE", "JSON": "LIST"},
	"_asc":    {"LIST": "LIST", "TABLE": "TABLE", "JSON": "LIST"},
	"_desc":   {"LIST": "LIST", "TABLE": "TABLE", "JSON": "LIST"},
	"_date":   {"LIST": "STRING", "FIELD": "STRING", "NUMBER": "STRING", "TABLE": "STRING", "JSON": "STRING"},
	"_like":   {"FIELD": "NUMBER", "LIST": "NUMBER", "TABLE": "NUMBER", "JSON": "NUMBER"},
}

func (plan *Plan) ParseAggr(stmt *Statement, dependency string) error {
	token := plan.lexer.Next(true)
	if token.Type != TOKEN_IDENTIFIER {
		return fmt.Errorf("expect identifier but got %s", token.Value)
	}
	if _, ok := AggrRegistry[token.Value]; !ok {
		return fmt.Errorf("unknown aggregate function %s", token.Value)
	}

	returnType, err := plan.GetAggrReturnType(token.Value, dependency)
	if err != nil {
		return err
	}
	inputStmt := plan.StatementMap[dependency]
	stmt.Operation = OpAggregateReduce
	stmt.Sources[0] = NewSource("var", dependency)
	stmt.Meta = map[string]string{
		"input_type":  plan.GetAggrInputTypeFromOperationType(inputStmt.Operation),
		"return_type": returnType,
	}
	aggrObj := Aggr{Name: token.Value, Args: []string{}}
	if plan.lexer.Next(false) != nil && plan.lexer.Next(false).Type == TOKEN_LPAREN {
		plan.lexer.Next(true) // consume the (
		for {
			token = plan.lexer.Next(true)
			if token.Type == TOKEN_RPAREN {
				break
			}
			if token.Type == TOKEN_COMMA {
				continue
			}
			if token.Type != TOKEN_IDENTIFIER && token.Type != TOKEN_NUMBER && token.Type != TOKEN_STRING {
				return fmt.Errorf("expect identifier | number | string but got %s", token.Value)
			}
			aggrObj.Args = append(aggrObj.Args, token.Value)
		}
	}
	stmt.Expressions = aggrObj
	return nil
}

func (plan *Plan) GetAggrReturnType(aggrName, inputStmtName string) (string, error) {
	inputStmt := plan.StatementMap[inputStmtName]
	inpType := ""
	switch inputStmt.Operation {
	case OpAccessTable, OpAccessRelatedTable, OpEndFilter:
		inpType = "TABLE"
	case OpAccessList:
		inpType = "LIST"
	case OpAccessField:
		inpType = "FIELD"
	case OpAccessRow:
		inpType = "ROW"
	case OpAccessJsonProperty:
		inpType = "JSON"
	case OpUnknownIdentifier:
		// Unknown identifiers cannot be aggregated
		return "", fmt.Errorf(
			"cannot apply aggregate function '%s' on unknown identifier. "+
				"Ensure the property exists and is of a supported type (TABLE, LIST, FIELD, or JSON)",
			aggrName,
		)
	case OpAggregateReduce:
		inpType = inputStmt.Meta["return_type"]
	}
	returnType, ok := AggrRegistry[aggrName][inpType]
	if !ok {
		return "", fmt.Errorf(
			"aggregate function '%s' does not support input type '%s'. "+
				"Supported types: %v",
			aggrName, inpType, getAggrSupportedTypes(aggrName),
		)
	}
	return returnType, nil
}

// getAggrSupportedTypes returns supported input types for an aggregate function
func getAggrSupportedTypes(aggrName string) []string {
	types := []string{}
	if mapping, ok := AggrRegistry[aggrName]; ok {
		for t := range mapping {
			types = append(types, t)
		}
	}
	return types
}

func (plan *Plan) GetOperationTypeFromAggrReturnType(returnType string) OperationType {
	switch returnType {
	case "NUMBER", "STRING":
		return OpLiteral
	case "TABLE":
		return OpAccessTable
	case "FIELD":
		return OpAccessField
	case "ROW":
		return OpAccessRow
	case "LIST":
		return OpAccessList
	}
	return OpUnknownIdentifier
}

func (plan *Plan) GetAggrInputTypeFromOperationType(ot OperationType) string {
	switch ot {
	case OpLiteral:
		return "NUMBER"
	case OpAccessTable, OpAccessRelatedTable, OpEndFilter:
		return "TABLE"
	case OpAccessField:
		return "FIELD"
	case OpAccessRow:
		return "ROW"
	case OpAccessList:
		return "LIST"
	case OpAccessJsonProperty:
		return "JSON"
	case OpUnknownIdentifier:
		return "UNKNOWN"
	}
	return "unknown"
}

func (plan *Plan) IsAggr(name string) bool {
	if _, ok := AggrRegistry[name]; ok {
		return true
	}
	return false
}

// func (plan *Plan) GetAggrReturnType(stmt *Statement, aggr ) string {
// 	if aggr, ok := AggrRegistry[aggrName]; ok {
// 		return aggr["return"]
// 	}
// 	return "unknown"
// }
