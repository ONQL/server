package parser

import "fmt"

// ParseProjection parses a projection block { ... }
func (plan *Plan) ParseProjection() error {
	token := plan.lexer.Next(true)
	if token.Type != TOKEN_LCURLEY {
		return fmt.Errorf("expect { but got %s", token.Value)
	}

	// Enter start projection
	prevStmtVar := plan.Statements[len(plan.Statements)-1].Name
	stmt := &Statement{
		Operation: OpStartProjection,
		Sources:   []Source{NewSource("var", prevStmtVar)},
	}
	name, err := NumberToColumn(len(plan.Statements) + 1)
	if err != nil {
		return err
	}
	projectionName := name
	stmt.Name = name
	plan.AddStatement(stmt)
	// Parse projection internal statements
	for {
		token = plan.lexer.Next(true)
		// fmt.Println(token.Value)
		if token == nil || token.Type == TOKEN_RCURLEY {
			break
		}
		key := ""
		// Add project key start
		if token.Type == TOKEN_STRING {
			key = token.Value
			colonToken := plan.lexer.Next(true) // consume :
			if colonToken.Type != TOKEN_COLON {
				return fmt.Errorf("expect : but got %s", colonToken.Value)
			}
		} else if token.Type == TOKEN_IDENTIFIER {
			key = token.Value
			plan.lexer.Prev(true) // move back to the token
		} else {
			return fmt.Errorf("expect string or identifier but got %s", token.Value)
		}

		stmt := &Statement{
			Operation:   OpStartProjectionKey,
			Sources:     []Source{NewSource("var", projectionName)},
			Expressions: key,
		}
		name, err := NumberToColumn(len(plan.Statements) + 1)
		if err != nil {
			return err
		}
		stmt.Name = name
		plan.AddStatement(stmt)

		// Parse key internal statements
		for {
			token = plan.lexer.Next(false)
			if token == nil || token.Type == TOKEN_RCURLEY || token.Type == TOKEN_COMMA {
				plan.lexer.Next(true) // consume the comma or closing brace
				break
			} 
			if err := plan.ParseStatement(); err != nil {
				return err
			}
		}
		// print("=================")
		// i := 0
		// for i < len(plan.Statements) {
		// 	stmt := plan.Statements[i]
		// 	fmt.Println("Statement:", stmt.Name, "Operation:", stmt.Operation)
		// 	i++
		// }
		// print("==================")
		// End projection key
		stmt = &Statement{
			Operation:   OpEndProjectionKey,
			Sources:     []Source{NewSource("var", projectionName)},
			Expressions: key,
		}
		name, err = NumberToColumn(len(plan.Statements) + 1)
		if err != nil {
			return err
		}
		stmt.Name = name
		plan.AddStatement(stmt)
		// print("=================")
		// i = 0
		// for i < len(plan.Statements) {
		// 	stmt := plan.Statements[i]
		// 	fmt.Println("Statement:", stmt.Name, "Operation:", stmt.Operation)
		// 	i++
		// }
		// print("==================")
	}

	// Enter end projection
	stmt = &Statement{
		Operation: OpEndProjection,
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
