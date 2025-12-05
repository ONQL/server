package parser

import (
	"errors"
	"fmt"
)

type Expression any

type Literal struct {
	Value string
	Type  string // e.g., "string", "number", "boolean"
}

type Aggr struct {
	Name string
	Args []string
	Rtrn string //table, list, row, field
}

type Source struct {
	SourceType  string //database or variable
	SourceValue string
}

func NewSource(sourceType, sourceValue string) Source {
	return Source{
		SourceType:  sourceType,
		SourceValue: sourceValue,
	}
}

type Statement struct {
	Name        string
	Operation   OperationType
	Sources     []Source
	Expressions Expression
	Meta        map[string]string
}

type Plan struct {
	Statements   []*Statement
	StatementMap map[string]*Statement
	Parents      []*Statement
	lexer        *Lexer
	ProtocolPass string
	Pos          int
	// Context      string
}

// OperationType defines the possible ONQL operation codes
type OperationType string

const (
	OpAccessTable        OperationType = "AT"  // Access Table
	OpAccessRelatedTable OperationType = "ART" // Access Related Table
	// OpAccessRelatedData  OperationType = "ARD" // Access Related Data
	OpAccessJoiningTable OperationType = "AJT" // Access Joining Data
	OpAccessList         OperationType = "ATL" // Access List
	OpAccessRow          OperationType = "ATR" // Access Row
	OpAccessField        OperationType = "ARF" // Access Field
	OpAccessUnknownRow   OperationType = "AUR" // Access Unknown Row
	OpAccessJsonProperty OperationType = "AJP" // Access JSON Property (dynamic property on JSON objects)
	OpSlice              OperationType = "SLT" // Slice
	OpAggregateReduce    OperationType = "AGR" // Aggregate
	OpNormalOperation    OperationType = "NO"  // Normal Operation
	OpUnknownIdentifier  OperationType = "UNI" // Unknown Identifier (non-JSON property access)
	OpSliceUnknown       OperationType = "SLU" // Slice Unknown
	OpStartFilter        OperationType = "SFT" // Start Filter
	OpEndFilter          OperationType = "EFT" // End Filter
	OpStartProjection    OperationType = "SPJ" // Start Projection
	OpEndProjection      OperationType = "EPJ" // End Projection
	OpStartProjectionKey OperationType = "SPK" // Start Projection Key
	OpEndProjectionKey   OperationType = "EPK" // End Projection Key
	OpLiteral            OperationType = "LIT" // Literal
	// OpStartJoin          OperationType = "SJO" // Start Join
)

func NewPlan(lexer *Lexer, protocolPass string) *Plan {
	return &Plan{
		Statements:   make([]*Statement, 0),
		StatementMap: make(map[string]*Statement), // Initialize the map
		Pos:          -1,
		lexer:        lexer,
		ProtocolPass: protocolPass,
		// Context:      context,
	}
}

func (plan *Plan) AddStatement(stmt *Statement) {
	fmt.Println("Adding statement:", stmt.Name, "Operation:", stmt.Operation)
	plan.Statements = append(plan.Statements, stmt)
	plan.StatementMap[stmt.Name] = stmt
	if stmt.Operation == OpStartFilter {
		plan.Parents = append(plan.Parents, stmt)
	} else if stmt.Operation == OpStartProjection {
		plan.Parents = append(plan.Parents, stmt)
	} else if stmt.Operation == OpEndFilter || stmt.Operation == OpEndProjection {
		plan.Parents = plan.Parents[:len(plan.Parents)-1]
	}
}

// Peek without advancing
func (plan *Plan) GetStatement(pos int, advance bool) *Statement {
	if pos < 0 || pos >= len(plan.Statements) {
		return nil
	}
	if advance {
		plan.Pos = pos
	}
	return plan.Statements[pos]
}

func (plan *Plan) NextStatement(advance bool) *Statement {
	next := plan.Pos + 1
	if next < 0 || next >= len(plan.Statements) {
		return nil
	}
	if advance {
		plan.Pos = next
	}
	return plan.Statements[next]
}

func (plan *Plan) PrevStatement(advance bool) *Statement {
	prev := plan.Pos - 1
	if prev < 0 || prev >= len(plan.Statements) {
		return nil
	}
	if advance {
		plan.Pos = prev
	}
	return plan.Statements[prev]
}

// func (plan *Plan) StatementByName(name string) *Statement {
// 	return plan.StatementMap[name]
// }

// NumberToColumn converts n (1-based) into spreadsheet-style columns.
// e.g. 1→"A", 26→"Z", 27→"AA", 28→"AB", 703→"AAA".
func NumberToColumn(n int) (string, error) {
	if n < 1 {
		return "", errors.New("number must be ≥ 1")
	}
	var col []rune
	for n > 0 {
		n-- // make it 0-based
		rem := n % 26
		col = append([]rune{rune('A' + rem)}, col...)
		n /= 26
	}
	return string(col), nil
}

func (plan *Plan) GetAncestorTable(stmt *Statement) (*Statement, error) {
	for {
		if stmt.Operation == OpAccessTable || stmt.Operation == OpAccessRelatedTable {
			return stmt, nil
		} else if len(stmt.Sources) > 0 {
			stmt = plan.StatementMap[stmt.Sources[0].SourceValue]
		} else {
			return nil, fmt.Errorf("no ancestor table found for statement: %v", stmt)
		}
	}
}

func (plan *Plan) GetPrevStatement() (*Statement, error) {
	prevStmt := plan.Statements[len(plan.Statements)-1]
	if prevStmt.Operation == OpAccessTable || prevStmt.Operation == OpAccessRelatedTable || prevStmt.Operation == OpStartFilter || prevStmt.Operation == OpEndFilter || prevStmt.Operation == OpSlice || prevStmt.Operation == OpStartProjectionKey || prevStmt.Operation == OpEndProjectionKey || prevStmt.Operation == OpAccessList || prevStmt.Operation == OpAccessRow || prevStmt.Operation == OpAccessField || prevStmt.Operation == OpAccessJsonProperty || prevStmt.Operation == OpUnknownIdentifier || prevStmt.Operation == OpAggregateReduce {
		return prevStmt, nil
	}
	if len(plan.Parents) > 0 {
		return plan.Parents[len(plan.Parents)-1], nil
	}
	return nil, fmt.Errorf("no previous statement found")
}
