package parser

import (
	"fmt"
	"onql/database"
	"strings"
)

// parse table will declare later
func (plan *Plan) ParseTable(stmt *Statement) error {
	var prevStmt *Statement
	if len(plan.Statements) > 0 {
		prevStmt = plan.Statements[len(plan.Statements)-1]
	}
	if prevStmt == nil || (prevStmt.Operation != OpAccessTable && prevStmt.Operation != OpAccessRelatedTable) {
		err := plan.ParseAccessTable(stmt)
		if err != nil {
			return err
		}
		return nil
	} else {
		if prevStmt.Operation != OpAccessTable && prevStmt.Operation != OpAccessRelatedTable && prevStmt.Operation != OpEndFilter && prevStmt.Operation != OpSlice {
			return fmt.Errorf("previous statement is not a table access: %v", prevStmt)
		}
		prevSource := strings.Split(prevStmt.Sources[0].SourceValue, ".")
		if database.IsRelatedTableByRelationName(plan.ProtocolPass, prevSource[0], prevSource[1], plan.lexer.Next(false).Value) {
			// if database.IsTable(plan.ProtocolPass, prevSource[0], plan.lexer.Next(false).Value) {
			// ParseAccessRelatedTable
			err := plan.ParseAccessRelatedTable(stmt, prevSource[0], prevSource[1], prevStmt.Name)
			if err != nil {
				return err
			}
			return nil
		}
	}
	return nil
}

// ParseAccessTable expects the next token to be a database name followed by a dot and then a table name.
func (plan *Plan) ParseAccessTable(stmt *Statement) error {
	//expect database here
	token := plan.lexer.Next(true)
	if !database.IsDatabase(plan.ProtocolPass, token.Value) {
		return fmt.Errorf("expect database but got %s", token.Value)
	}
	db := token.Value
	//expect table here
	token = plan.lexer.Next(true)
	if token.Type != TOKEN_DOT {
		return fmt.Errorf("expect . but got %s", token.Value)
	}
	token = plan.lexer.Next(true)
	if !database.IsTable(plan.ProtocolPass, db, token.Value) {
		return fmt.Errorf("expect table but got %s", token.Value)
	}
	table := token.Value
	stmt.Operation = OpAccessTable
	stmt.Sources[0] = NewSource("db", fmt.Sprintf("%s.%s", db, table))
	stmt.Meta = make(map[string]string)
	//save original database names
	name, err := database.GetDbNameFromProtoName(plan.ProtocolPass, db)
	if err != nil {
		return err
	}
	stmt.Meta["db"] = name
	name, err = database.GetTableNameFromProtoName(plan.ProtocolPass, db, table)
	if err != nil {
		return err
	}
	stmt.Meta["table"] = name
	return nil
}

func (plan *Plan) ParseAccessRelatedTable(stmt *Statement, db string, parentTableDependency string, varDependency string) error {
	//expect table here
	token := plan.lexer.Next(true)
	fmt.Println(plan.ProtocolPass,db,parentTableDependency,token.Value,varDependency)
	if !database.IsRelatedTableByRelationName(plan.ProtocolPass, db, parentTableDependency, token.Value) {
		return fmt.Errorf("not relation found on table %s by name %s", parentTableDependency, token.Value)
	}
	relation, err := database.GetRelationByRelationName(plan.ProtocolPass, db, parentTableDependency, token.Value)
	// if !database.IsTable(plan.ProtocolPass, db, token.Value) {
	// 	return fmt.Errorf("expect table but got %s", token.Value)
	// }
	//fetch relation between tables
	// relation, err := database.GetProtoRelation(plan.ProtocolPass, db, parentTableDependency, token.Value)
	if err != nil {
		return fmt.Errorf("no relation found between %s and %s in database %s: %v", parentTableDependency, token.Value, db, err)
	}
	stmt.Operation = OpAccessRelatedTable
	stmt.Sources[0] = NewSource("db", fmt.Sprintf("%s.%s", db, relation.ProtoTable))
	stmt.Sources[1] = NewSource("var", varDependency)
	stmt.Expressions = relation
	//save original database names
	stmt.Meta = make(map[string]string)
	name, err := database.GetDbNameFromProtoName(plan.ProtocolPass, db)
	if err != nil {
		return err
	}
	stmt.Meta["db"] = name
	// name, err = database.GetTableNameFromProtoName(plan.ProtocolPass, db, token.Value)
	// if err != nil {
	// 	return err
	// }
	stmt.Meta["table"] = relation.Entity
	return nil
}
