package parser

import "errors"

func (plan *Plan) ParseLiteral(stmt *Statement) error {
	// Implement literal parsing logic here
	token := plan.lexer.Next(true)
	if token.Type != TOKEN_STRING && token.Type != TOKEN_NUMBER {
		return errors.New("invalid literal")
	}
	stmt.Name = token.Value
	stmt.Operation = OpLiteral
	stmt.Expressions = token.Value
	stmt.Meta = map[string]string{"type": TokenNames[token.Type]}
	return nil
}
