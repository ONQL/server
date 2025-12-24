package parser

import (
	"fmt"
)

func (plan *Plan) Parse() error {
	for plan.lexer.Next(false) != nil {
		if err := plan.ParseStatement(); err != nil {
			return err
		}
	}
	return nil
}

// ParseStatement parses a single statement (identifier, filter, or projection)
func (plan *Plan) ParseStatement() error {
	stmt := &Statement{Sources: make([]Source, 5)}
	token := plan.lexer.Next(false)
	fmt.Println(token)

	if token == nil {
		return nil
	}
	if token.Type == TOKEN_DOT {
		plan.lexer.Next(true) // consume the dot
		return nil
	}
	// if token == nil{
	// 	return nil
	// }
	// print("in:-" + token.Value)
	if token.Type == TOKEN_STRING || token.Type == TOKEN_NUMBER {
		err := plan.ParseLiteral(stmt)
		if err != nil {
			return err
		}
	} else if token.Type == TOKEN_IDENTIFIER {
		// print("in:-" + token.Value)
		// Parse identifier
		if err := plan.ParseIdentifier(stmt); err != nil {
			return err
		}
		//parse row access [0]
	} else if token.Type == TOKEN_ROW_ACCESS {
		if err := plan.ParseRow(stmt); err != nil {
			return err
		}
		//parse slice [0:4]
	} else if token.Type == TOKEN_SLICE {
		if err := plan.ParseSlice(stmt); err != nil {
			return err
		}
	} else if token.Type == TOKEN_LBRACKET {
		// print("parse filter started")
		// Parse filter
		if err := plan.ParseFilter(); err != nil {
			return err
		}
		return nil
	} else if token.Type == TOKEN_LCURLEY {
		// Parse projection
		if err := plan.ParseProjection(); err != nil {
			return err
		}
		return nil
	} else if token.Type == TOKEN_LPAREN {
		if err := plan.ParseParenthesis(); err != nil {
			return err
		}
		return nil
	} else if GetOperatorPrecedence(token.Type) > 0 {
		if err := plan.ParseOperator(); err != nil {
			return err
		}
		return nil
	} else {
		return fmt.Errorf("unexpected token '%s' at position %d", token.Value, token.Pos)
	}

	name, err := NumberToColumn(len(plan.Statements) + 1)
	if err != nil {
		return err
	}
	stmt.Name = name
	plan.AddStatement(stmt)

	return nil
}

// func (plan *Plan) ParseOperator(stmt *Statement) error {
