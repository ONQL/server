package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseFilter parses a filter block [ ... ]
func (plan *Plan) ParseFilter() error {
	token := plan.lexer.Next(true)
	if token.Type != TOKEN_LBRACKET {
		return fmt.Errorf("expect [ but got %s", token.Value)
	}

	// Enter start filter
	prevStmtVar := plan.Statements[len(plan.Statements)-1].Name
	stmt := &Statement{
		Operation: OpStartFilter,
		Sources:   []Source{NewSource("var", prevStmtVar)},
	}
	name, err := NumberToColumn(len(plan.Statements) + 1)
	if err != nil {
		return err
	}
	stmt.Name = name
	plan.AddStatement(stmt)

	// Parse filter internal statements
	for {
		token = plan.lexer.Next(false)
		if token.Type == TOKEN_RBRACKET {
			plan.lexer.Next(true) // consume the closing bracket
			break
		}
		if err := plan.ParseStatement(); err != nil {
			return err
		}
	}

	// Enter end filter
	stmt = &Statement{
		Operation: OpEndFilter,
		Sources:   []Source{NewSource("var", prevStmtVar)},
	}
	name, err = NumberToColumn(len(plan.Statements) + 1)
	if err != nil {
		return err
	}
	stmt.Name = name
	plan.AddStatement(stmt)
	return nil
}

func (plan *Plan) ParseSlice(stmt *Statement) error {
	// Expect slice operation
	token := plan.lexer.Next(true)
	if token.Type != TOKEN_SLICE {
		return fmt.Errorf("expect slice but got %s", token.Value)
	}
	slice := strings.TrimLeft(token.Value, "[")
	slice = strings.TrimRight(slice, "]")
	//set slice operation
	// for {
	// 	token = plan.lexer.Next(true)
	// 	if token.Type == TOKEN_RBRACKET {
	// 		break
	// 	} else if token.Type == TOKEN_COLON || token.Type == TOKEN_NUMBER {
	// 		slice += token.Value
	// 	} else {
	// 		return fmt.Errorf("expect number or : but got %s", token.Value)
	// 	}
	// }
	stmt.Operation = OpSlice
	stmt.Expressions = slice
	stmt.Sources = []Source{NewSource("var", plan.Statements[len(plan.Statements)-1].Name)}
	return nil
}

func (plan *Plan) ParseRow(stmt *Statement) error {
	token := plan.lexer.Next(true)
	if token.Type != TOKEN_ROW_ACCESS {
		return fmt.Errorf("expect [digits] in access row but got %s", token.Value)
	}
	rowNum := strings.TrimLeft(token.Value, "[")
	rowNum = strings.TrimRight(rowNum, "]")
	// prevStmt := plan.Statements[len(plan.Statements)-1]
	// if prevStmt.Operation == OpUnknownIdentifier or Op
	stmt.Operation = OpAccessRow
	rowNumCon, err := strconv.ParseInt(rowNum, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid row number: %s", rowNum)
	}
	stmt.Expressions = rowNumCon
	stmt.Sources = []Source{NewSource("var", plan.Statements[len(plan.Statements)-1].Name)}
	return nil
}
