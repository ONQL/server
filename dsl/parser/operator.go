package parser

import (
	"fmt"
)

func (plan *Plan) ParseOperator() error {
	left := ""
	right := ""
	leftType := ""
	rightType := ""
	isLeftVar := false
	isRightVar := false
	//get previous statement

	token := plan.lexer.Next(true)
	//if literal on left side pass it
	// if token.Type == TOKEN_STRING || token.Type == TOKEN_NUMBER {
	// 	left = token.Value
	// 	leftType = TokenNames[token.Type]
	// 	token = plan.lexer.Next(true) // move back to the token
	// } else
	if token.Type != TOKEN_NOT {
		prevStmt := plan.Statements[len(plan.Statements)-1]
		left = prevStmt.Name
		leftType = "var"
		isLeftVar = true
	}

	pr1 := GetOperatorPrecedence(token.Type)
	if pr1 == 0 {
		return fmt.Errorf("expect operator but got %s", token.Value)
	}
	op := token.Value

	for {
		token = plan.lexer.Next(false)
		if token == nil {
			break
		}
		if GetOperatorPrecedence(token.Type) == 0 {
			//check is time to return or not
			if token.Type == TOKEN_RPAREN || token.Type == TOKEN_RBRACKET || token.Type == TOKEN_COMMA || token.Type == TOKEN_RCURLEY {
				//get last staement here
				break
			}
		} else {
			pr2 := GetOperatorPrecedence(token.Type)
			if pr1 > pr2 {
				break
			} else {
				//break after parse next statement mean its end of this operator
				err := plan.ParseStatement()
				if err != nil {
					return err
				}
				break
			}
		}
		err := plan.ParseStatement()
		if err != nil {
			return err
		}
	}

	right = plan.Statements[len(plan.Statements)-1].Name
	isRightVar = true
	rightType = "var"
	//this op just maintain order of statements but its not a statement
	// if op == "(" {
	// 	return nil // skip parentheses
	// }
	//add operator statement here
	stmt := &Statement{
		Operation: OpNormalOperation,
		Sources:   make([]Source, 5),
		// Sources:   ,
		Expressions: left + " " + op + " " + right}

	if isLeftVar {
		stmt.Sources[0] = NewSource("var", left)
	}

	if isRightVar {
		stmt.Sources[1] = NewSource("var", right)
	}

	name, err := NumberToColumn(len(plan.Statements) + 1)
	if err != nil {
		return err
	}
	stmt.Name = name
	stmt.Meta = make(map[string]string)
	stmt.Meta["left_type"] = leftType
	stmt.Meta["right_type"] = rightType
	plan.AddStatement(stmt)
	return nil
}

func (plan *Plan) ParseParenthesis() error {
	token := plan.lexer.Next(true)
	if token.Type != TOKEN_LPAREN {
		return fmt.Errorf("expect ( but got %s", token.Value)
	}
	for {
		token = plan.lexer.Next(false)
		if token.Type == TOKEN_RPAREN {
			plan.lexer.Next(true) // consume the closing parenthesis
			break
		}
		if err := plan.ParseStatement(); err != nil {
			return err
		}
	}
	return nil
}

func GetOperatorPrecedence(op int) int {
	switch op {
	// case TOKEN_LPAREN:
	// 	return 5

	case TOKEN_MUL, TOKEN_DIV, TOKEN_MOD:
		return 4
	case TOKEN_PLUS, TOKEN_MINUS:
		return 3
	case TOKEN_GT, TOKEN_LT, TOKEN_GE, TOKEN_LE, TOKEN_NE, TOKEN_IN, TOKEN_EQUAL:
		return 2
	case TOKEN_AND, TOKEN_OR, TOKEN_NOT:
		return 1
	default:
		return 0
	}
}

//parseArithmatic 3
//parse Logical 1
//Parse Comparison 2
//ParseParanthesis 4

// if other operator found check if that operator priority is < then me then  return else parseStatement
// if found literal then add it in expression and return

// if (),23,"",operators
//statements if found any other operator or if found any other ] or ) if found ( start parse statement
// users[name == "paras" and transactions.amount.sum > 1000]
// at end of statement we foud literal, operator,
// tm.users[2:3].orders[2:3]{"q":qty}
// }Name     Operation Sources                        Expressions
// Name     Operation Sources                        Expressions
// --------------------------------------------------------------------------------
// A        AT       db:tm.users                    <nil>
// B        SLT      var:A                          2:3
// C        ART      db:tm.orders, var:B            &{otm                  orders                                    balance:qty         }
// D        SLT      var:C                          2:3
// E        SPJ      var:D                          <nil>
// F        SPK      var:E                          "q"
// G        ATL      var:F                          qty
// H        EPK      var:E                          "q"
// I        EPJ      var:D                          <nil>
