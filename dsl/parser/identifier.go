package parser

import (
	"fmt"
	"onql/database"

	// "onql/dsl/core"
	"strings"
)

// ParseIdentifier parses an identifier in the query and dispatches to the appropriate handler.
// It uses helper functions for each logical branch for clarity and maintainability.
// The token is passed as *Token to avoid repeated lexer calls and to allow mutation if needed.
func (plan *Plan) ParseIdentifier(stmt *Statement) error {
	token := plan.lexer.Next(false)
	if token.Type != TOKEN_IDENTIFIER {
		return fmt.Errorf("expect identifier but got %s", token.Value)
	}
	// No previous statements: treat as table
	if len(plan.Statements) == 0 {
		return plan.parseIdentifierNoPrev(stmt, token)
	}

	// If token is a database, treat as access table
	if database.IsDatabase(plan.ProtocolPass, token.Value) {
		return plan.parseIdentifierIsDatabase(stmt, token)
	}

	// prevStmt := plan.Statements[len(plan.Statements)-1]
	prevStmt, err := plan.GetPrevStatement()
	if err != nil {
		return err
	}
	if token.Value == "parent" {
		return plan.ParseParentKeyword(stmt, prevStmt)
	}

	// If previous op is table/related/filter/slice, handle as table/column/aggr/unknown
	if (prevStmt.Meta != nil && prevStmt.Meta["return_type"] == "TABLE") || prevStmt.Operation == OpAccessTable || prevStmt.Operation == OpAccessRelatedTable || prevStmt.Operation == OpStartFilter || prevStmt.Operation == OpEndFilter || prevStmt.Operation == OpSlice || prevStmt.Operation == OpStartProjectionKey || prevStmt.Operation == OpEndProjectionKey {
		return plan.parseIdentifierTableOrRelated(stmt, prevStmt, token)
	}
	// If previous op is list/row/field/json/unknown, handle as aggr/field/json
	if prevStmt.Operation == OpAccessList || prevStmt.Operation == OpAccessRow || prevStmt.Operation == OpAccessField || prevStmt.Operation == OpAccessJsonProperty || prevStmt.Operation == OpUnknownIdentifier || prevStmt.Operation == OpAggregateReduce {
		return plan.parseIdentifierListOrRowOrFieldOrAggr(stmt, prevStmt, token)
	}

	//here need to find parent statement mean parent filter or projection

	return fmt.Errorf("unexpected identifier %s after %s", token.Value, prevStmt.Operation)
}

// parseIdentifierNoPrev handles the case where there are no previous statements.
// It treats the identifier as a table.
func (plan *Plan) parseIdentifierNoPrev(stmt *Statement, token *Token) error {
	return plan.ParseTable(stmt)
}

// parseIdentifierIsDatabase handles the case where the identifier is a database name.
// It treats the identifier as an access table operation.
func (plan *Plan) parseIdentifierIsDatabase(stmt *Statement, token *Token) error {
	return plan.ParseAccessTable(stmt)
}

// parseIdentifierTableOrRelated handles the case where the previous statement is a table, related table, filter, or slice.
// It checks if the identifier is a table, column, aggregate, or unknown, and dispatches accordingly.
func (plan *Plan) parseIdentifierTableOrRelated(stmt *Statement, prevStmt *Statement, token *Token) error {
	prevSourceStmt, err := plan.GetAncestorTable(prevStmt)
	if err != nil {
		return err
	}
	prevSource := strings.Split(prevSourceStmt.Sources[0].SourceValue, ".")
	fmt.Println(prevSource[0], token.Value)
	// if database.IsTable(plan.ProtocolPass, prevSource[0], token.Value){
	if database.IsRelatedTableByRelationName(plan.ProtocolPass, prevSource[0], prevSource[1], token.Value) {
		switch prevStmt.Operation {
		case OpAccessTable, OpAccessRelatedTable, OpStartFilter, OpEndFilter, OpSlice, OpStartProjectionKey, OpEndProjectionKey:
			return plan.ParseAccessRelatedTable(stmt, prevSource[0], prevSource[1], prevStmt.Name)
		}
		return fmt.Errorf("invalid table access %s on %s", token.Value, prevStmt.Operation)
	} else if database.IsColumn(plan.ProtocolPass, prevSource[0], prevSource[1], token.Value) {
		return plan.ParseTableList(stmt, prevSource[0], prevSource[1], token.Value, prevStmt.Name)
	} else if plan.IsAggr(token.Value) {
		return plan.ParseAggr(stmt, prevStmt.Name)
	} else {
		return fmt.Errorf("unknown identifier %s after table data", token.Value)
	}
}

// parseIdentifierListOrRowOrField handles the case where the previous statement is a list, row, field, or unknown identifier.
// It checks if the identifier is a column (field), aggregate, or json field (not implemented).
func (plan *Plan) parseIdentifierListOrRowOrFieldOrAggr(stmt *Statement, prevStmt *Statement, token *Token) error {
	prevSourceStmt, err := plan.GetAncestorTable(prevStmt)
	if err != nil {
		return err
	}
	prevSource := strings.Split(prevSourceStmt.Sources[0].SourceValue, ".")
	var operation OperationType
	if prevStmt.Operation == OpAggregateReduce {
		operation = plan.GetOperationTypeFromAggrReturnType(prevStmt.Meta["return_type"])
	} else {
		operation = prevStmt.Operation
	}

	switch operation {
	case OpAccessList:
		// Only aggregate is valid here (not implemented)
		if plan.IsAggr(token.Value) {
			err := plan.ParseAggr(stmt, prevStmt.Name)
			if err != nil {
				return err
			}
		} else {
			// Parse as JSON property access
			err := plan.ParseJsonProperty(stmt, prevStmt)
			if err != nil {
				return err
			}
		}
		return nil
	case OpAccessRow:
		// Can be aggregate or field
		// if database.IsTable(plan.ProtocolPass, prevSource[0], token.Value) {
		// return plan.ParseAccessRelatedTable(stmt, prevSource[0], pre, prevStmt.Name)
		if database.IsColumn(plan.ProtocolPass, prevSource[0], prevSource[1], token.Value) {
			return plan.ParseRowField(stmt, prevSource[0], prevSource[1], token.Value, prevStmt.Name)

		} else if database.IsRelatedTableByRelationName(plan.ProtocolPass, prevSource[0], prevSource[1], token.Value) {
			// parse related table
			return plan.ParseAccessRelatedTable(stmt, prevSource[0], prevSource[1], prevStmt.Name)
		} else if plan.IsAggr(token.Value) {
			err := plan.ParseAggr(stmt, prevStmt.Name)
			if err != nil {
				return err
			}
			return nil
			// Aggregate or json field (not implemented)
		} else {
			return fmt.Errorf("expect field or aggregate but got %s", token.Value)
		}
	case OpAccessField:
		// Aggregate or JSON property access
		if plan.IsAggr(token.Value) {
			err := plan.ParseAggr(stmt, prevStmt.Name)
			if err != nil {
				return err
			}
			return nil
		} else {
			// Parse as JSON property access
			err := plan.ParseJsonProperty(stmt, prevStmt)
			if err != nil {
				return err
			}
			return nil
		}
	case OpAccessJsonProperty, OpUnknownIdentifier:
		// Check if it's an aggregate function first
		if plan.IsAggr(token.Value) {
			err := plan.ParseAggr(stmt, prevStmt.Name)
			if err != nil {
				return err
			}
			return nil
		} else {
			// Parse as JSON property access
			err := plan.ParseJsonProperty(stmt, prevStmt)
			if err != nil {
				return err
			}
			return nil
		}
	}
	return nil
}

// ParseJsonProperty parses a JSON property access on validated JSON data
func (plan *Plan) ParseJsonProperty(stmt *Statement, prevStmt *Statement) error {
	token := plan.lexer.Next(true)
	if token.Type != TOKEN_IDENTIFIER {
		return fmt.Errorf("expect identifier but got %s", token.Value)
	}

	// Validate that parent is actually JSON type
	isJsonParent := plan.isJsonType(prevStmt)

	if isJsonParent {
		stmt.Operation = OpAccessJsonProperty
		stmt.Meta = map[string]string{
			"property_name": token.Value,
			"type":          "json",
		}
	} else {
		// Not JSON - treat as unknown identifier
		stmt.Operation = OpUnknownIdentifier
		stmt.Meta = map[string]string{
			"property_name": token.Value,
			"type":          "unknown",
		}
	}

	stmt.Sources[0] = NewSource("var", prevStmt.Name)
	stmt.Expressions = token.Value
	return nil
}

// isJsonType checks if a statement represents JSON data
func (plan *Plan) isJsonType(stmt *Statement) bool {
	if stmt == nil {
		return false
	}

	// Check statement metadata for JSON type
	if stmt.Meta != nil {
		stmtType, ok := stmt.Meta["type"]
		if ok && stmtType == "json" {
			return true
		}
	}

	// Check based on operation type
	switch stmt.Operation {
	case OpAccessJsonProperty:
		// Chained JSON property access
		return true
	case OpAccessList:
		// Check if the column is JSON type
		return stmt.Meta != nil && stmt.Meta["type"] == "json"
	case OpAccessField:
		// Check if the field is JSON type
		return stmt.Meta != nil && stmt.Meta["type"] == "json"
	case OpAggregateReduce:
		// Check if aggregate returns JSON
		return stmt.Meta != nil && stmt.Meta["return_type"] == "JSON"
	default:
		return false
	}
}

// ParseUnknownIdentifier is deprecated - use ParseJsonProperty instead
// Kept for backward compatibility
func (plan *Plan) ParseUnknownIdentifier(stmt *Statement, prevStmt *Statement) error {
	return plan.ParseJsonProperty(stmt, prevStmt)
}

// ParseJsonField is deprecated - use ParseJsonProperty instead
// Kept for backward compatibility
func (plan *Plan) ParseJsonField(stmt *Statement, prevStmt *Statement) error {
	return plan.ParseJsonProperty(stmt, prevStmt)
}

func (plan *Plan) ParseParentKeyword(stmt *Statement, prevStmt *Statement) error {
	token := plan.lexer.Next(true)
	if token.Value != "parent" {
		return fmt.Errorf("expect parent keyword but got %s", token.Value)
	}
	parentStmt := plan.Parents[len(plan.Parents)-2]
	annceStmt, err := plan.GetAncestorTable(parentStmt)
	if err != nil {
		return err
	}
	sources := strings.Split(annceStmt.Sources[0].SourceValue, ".")

	if parentStmt == nil {
		return fmt.Errorf("no parent statement found")
	}

	plan.lexer.Next(true) //parse dot
	token = plan.lexer.Next(true)
	if token == nil || token.Type != TOKEN_IDENTIFIER {
		return fmt.Errorf("expect identifier but got %s", token.Value)
	}
	dbColSchema, err := database.GetColSchemaFromProtoName(plan.ProtocolPass, sources[0], sources[1], token.Value)
	if err != nil {
		return err
	}
	stmt.Expressions = token.Value
	stmt.Operation = OpAccessList
	stmt.Sources[0] = NewSource("var", parentStmt.Name)
	stmt.Meta = dbColSchema
	return nil
}
