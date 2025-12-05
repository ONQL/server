package parser

import (
	"fmt"
	"onql/database"
)

func (plan *Plan) ParseTableList(stmt *Statement, db string, table string, column string, dependencyName string) error {
	//expect column here
	token := plan.lexer.Next(true)
	if !database.IsColumn(plan.ProtocolPass, db, table, token.Value) {
		return fmt.Errorf("expect column but got %s", token.Value)
	}
	stmt.Operation = OpAccessList
	stmt.Sources[0] = NewSource("var", dependencyName)
	stmt.Expressions = token.Value
	dbColSchema, err := database.GetColSchemaFromProtoName(plan.ProtocolPass, db, table, token.Value)
	if err != nil {
		return err
	}
	stmt.Meta = dbColSchema
	return nil
}

func (plan *Plan) ParseRowField(stmt *Statement, db string, table string, column string, dependencyName string) error {
	//expect column here
	token := plan.lexer.Next(true)
	if !database.IsColumn(plan.ProtocolPass, db, table, token.Value) {
		return fmt.Errorf("expect column but got %s", token.Value)
	}
	stmt.Operation = OpAccessField
	stmt.Sources[0] = NewSource("var", dependencyName)
	stmt.Expressions = token.Value
	dbColSchema, err := database.GetColSchemaFromProtoName(plan.ProtocolPass, db, table, token.Value)
	if err != nil {
		return err
	}
	stmt.Meta = dbColSchema
	return nil
}
