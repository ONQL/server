package parser

import (
	"fmt"
	"strings"
)

// AST Nodes
type Statement interface {
	stmt()
}

type SelectStmt struct {
	Columns []string
	Table   string
	DB      string
	Where   map[string]interface{}
	Joins   []JoinDef
}

type JoinDef struct {
	Type  string // INNER, LEFT, RIGHT, FULL
	DB    string
	Table string
	On    map[string]string // LeftCol -> RightCol
}

func (s *SelectStmt) stmt() {}

type InsertStmt struct {
	DB      string
	Table   string
	Columns []string
	Values  []interface{}
}

func (s *InsertStmt) stmt() {}

type UpdateStmt struct {
	DB    string
	Table string
	Set   map[string]interface{}
	Where map[string]interface{}
}

func (s *UpdateStmt) stmt() {}

type DeleteStmt struct {
	DB    string
	Table string
	Where map[string]interface{}
}

func (s *DeleteStmt) stmt() {}

type CreateStmt struct {
	Type    string // DATABASE or TABLE
	DB      string
	Table   string
	Columns map[string]interface{} // For TABLE
}

func (s *CreateStmt) stmt() {}

type DropStmt struct {
	Type  string // DATABASE or TABLE
	DB    string
	Table string
}

func (s *DropStmt) stmt() {}

type AlterStmt struct {
	DB      string
	Table   string
	Action  string // ADD, DROP, MODIFY
	ColName string
	ColDef  map[string]interface{}
}

func (s *AlterStmt) stmt() {}

type RenameStmt struct {
	Type    string // DATABASE or TABLE
	DB      string
	OldName string
	NewName string
}

func (s *RenameStmt) stmt() {}

// Parser
type Parser struct {
	lexer *Lexer
	cur   TokenItem
	peek  TokenItem
}

func NewParser(l *Lexer) *Parser {
	p := &Parser{lexer: l}
	p.nextToken() // Load cur
	p.nextToken() // Load peek
	return p
}

func (p *Parser) nextToken() {
	p.cur = p.peek
	p.peek = p.lexer.Scan()
}

func (p *Parser) Parse() (Statement, error) {
	if p.cur.Type == EOF {
		return nil, nil
	}

	switch p.cur.Type {
	case SELECT:
		return p.parseSelect()
	case INSERT:
		return p.parseInsert()
	case UPDATE:
		return p.parseUpdate()
	case DELETE:
		return p.parseDelete()
	case CREATE:
		return p.parseCreate()
	case DROP:
		return p.parseDrop()
	case ALTER:
		return p.parseAlter()
	case RENAME:
		return p.parseRename()
	default:
		return nil, fmt.Errorf("unexpected token %s", p.cur.Value)
	}
}

// Helper to parse "db.table" or just "table" (assuming handle context later or error)
// Returns db, table
func (p *Parser) parseTableIdentifier() (string, string, error) {
	// Expect IDENTIFIER
	if p.cur.Type != IDENTIFIER {
		return "", "", fmt.Errorf("expected identifier, got %s", p.cur.Value)
	}
	part1 := p.cur.Value
	p.nextToken()

	if p.cur.Type == DOT {
		p.nextToken() // consume dot
		if p.cur.Type != IDENTIFIER {
			return "", "", fmt.Errorf("expected table name after dot, got %s", p.cur.Value)
		}
		part2 := p.cur.Value
		p.nextToken()
		return part1, part2, nil
	}

	// Just table? we usually need db.table
	// Return empty db to signal missing db
	return "", part1, nil
}

func (p *Parser) parseSelect() (*SelectStmt, error) {
	p.nextToken() // skip SELECT

	cols := []string{}
	if p.cur.Type == ASTERISK {
		cols = append(cols, "*")
		p.nextToken()
	} else {
		// Parse column list
		for {
			if p.cur.Type != IDENTIFIER {
				return nil, fmt.Errorf("expected column name, got %s", p.cur.Value)
			}
			colName := p.cur.Value
			p.nextToken()
			if p.cur.Type == DOT {
				p.nextToken() // skip .
				if p.cur.Type != IDENTIFIER {
					return nil, fmt.Errorf("expected column name after dot")
				}
				colName += "." + p.cur.Value
				p.nextToken()
			}
			cols = append(cols, colName)
			if p.cur.Type == COMMA {
				p.nextToken()
			} else {
				break
			}
		}
	}

	if p.cur.Type != FROM {
		return nil, fmt.Errorf("expected FROM, got %s", p.cur.Value)
	}
	p.nextToken() // skip FROM

	db, table, err := p.parseTableIdentifier()
	if err != nil {
		return nil, err
	}
	if db == "" {
		// HACK: Support 'table' only if context allows?
		// For now, assume db is required or strict matching
		return nil, fmt.Errorf("expected db.table format")
	}

	joins := []JoinDef{}
	for {
		if p.cur.Type == JOIN || p.cur.Type == INNER || p.cur.Type == LEFT || p.cur.Type == RIGHT || p.cur.Type == FULL {
			joinType := "INNER"
			if p.cur.Type == INNER {
				p.nextToken() // skip INNER
			} else if p.cur.Type == LEFT {
				joinType = "LEFT"
				p.nextToken() // skip LEFT
				if p.cur.Type == OUTER {
					p.nextToken()
				} // skip optional OUTER
			} else if p.cur.Type == RIGHT {
				joinType = "RIGHT"
				p.nextToken()
				if p.cur.Type == OUTER {
					p.nextToken()
				}
			} else if p.cur.Type == FULL {
				joinType = "FULL"
				p.nextToken()
				if p.cur.Type == OUTER {
					p.nextToken()
				}
			}

			if p.cur.Type != JOIN {
				return nil, fmt.Errorf("expected JOIN")
			}
			p.nextToken() // skip JOIN

			jDB, jTable, err := p.parseTableIdentifier()
			if err != nil {
				return nil, err
			}

			onMap := make(map[string]string)
			if p.cur.Type == ON {
				p.nextToken() // skip ON
				// Parsing ON t1.col = t2.col
				// Simplified: left = right
				leftRef := p.cur.Value
				p.nextToken()
				if p.cur.Type == DOT {
					p.nextToken()
					leftRef += "." + p.cur.Value
					p.nextToken()
				}

				if p.cur.Type != EQUAL {
					return nil, fmt.Errorf("expected = in ON")
				}
				p.nextToken()

				rightRef := p.cur.Value
				p.nextToken()
				if p.cur.Type == DOT {
					p.nextToken()
					rightRef += "." + p.cur.Value
					p.nextToken()
				}

				onMap[leftRef] = rightRef
			}
			joins = append(joins, JoinDef{Type: joinType, DB: jDB, Table: jTable, On: onMap})
		} else {
			break
		}
	}

	where := make(map[string]interface{})
	if p.cur.Type == WHERE {
		// Basic WHERE col = val parser
		// Only supporting simple equality for now to map to underlying API easily
		p.nextToken()

		// Loop parser for AND/OR could be added here
		col := p.cur.Value
		if p.cur.Type != IDENTIFIER {
			return nil, fmt.Errorf("expected column in WHERE, got %s", p.cur.Value)
		}
		p.nextToken()

		// Handle table.col in where
		if p.cur.Type == DOT {
			p.nextToken()
			col += "." + p.cur.Value
			p.nextToken()
		}

		if p.cur.Type != EQUAL {
			return nil, fmt.Errorf("expected = in WHERE, got %s", p.cur.Value)
		}
		p.nextToken()

		val := p.cur.Value
		// Handle literals
		where[col] = val
		p.nextToken()
	}

	return &SelectStmt{Columns: cols, Table: table, DB: db, Where: where, Joins: joins}, nil
}

func (p *Parser) parseInsert() (*InsertStmt, error) {
	p.nextToken() // skip INSERT
	if p.cur.Type != INTO {
		return nil, fmt.Errorf("expected INTO, got %s", p.cur.Value)
	}
	p.nextToken()

	db, table, err := p.parseTableIdentifier()
	if err != nil {
		return nil, err
	}

	if p.cur.Type != LPAREN {
		return nil, fmt.Errorf("expected (, got %s", p.cur.Value)
	}
	p.nextToken()

	cols := []string{}
	for {
		if p.cur.Type != IDENTIFIER {
			return nil, fmt.Errorf("expected column, got %s", p.cur.Value)
		}
		cols = append(cols, p.cur.Value)
		p.nextToken()
		if p.cur.Type == COMMA {
			p.nextToken()
		} else {
			break
		}
	}
	if p.cur.Type != RPAREN {
		return nil, fmt.Errorf("expected ), got %s", p.cur.Value)
	}
	p.nextToken()

	if p.cur.Type != VALUES {
		return nil, fmt.Errorf("expected VALUES, got %s", p.cur.Value)
	}
	p.nextToken()

	if p.cur.Type != LPAREN {
		return nil, fmt.Errorf("expected (, got %s", p.cur.Value)
	}
	p.nextToken()

	vals := []interface{}{}
	for {
		vals = append(vals, p.cur.Value) // Treat valid tokens as values
		p.nextToken()
		if p.cur.Type == COMMA {
			p.nextToken()
		} else {
			break
		}
	}
	if p.cur.Type != RPAREN {
		return nil, fmt.Errorf("expected ), got %s", p.cur.Value)
	}
	p.nextToken()

	return &InsertStmt{DB: db, Table: table, Columns: cols, Values: vals}, nil
}

func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	p.nextToken() // skip UPDATE
	db, table, err := p.parseTableIdentifier()
	if err != nil {
		return nil, err
	}

	if p.cur.Type != SET {
		return nil, fmt.Errorf("expected SET, got %s", p.cur.Value)
	}
	p.nextToken()

	set := make(map[string]interface{})
	// Only supporting single SET for now or loop
	col := p.cur.Value
	if p.cur.Type != IDENTIFIER {
		return nil, fmt.Errorf("expected col, got %s", p.cur.Value)
	}
	p.nextToken()
	if p.cur.Type != EQUAL {
		return nil, fmt.Errorf("expected =, got %s", p.cur.Value)
	}
	p.nextToken()
	val := p.cur.Value
	set[col] = val
	p.nextToken()

	where := make(map[string]interface{})
	if p.cur.Type == WHERE {
		p.nextToken()
		wCol := p.cur.Value
		p.nextToken()
		if p.cur.Type != EQUAL {
			return nil, fmt.Errorf("expected = in WHERE")
		}
		p.nextToken()
		wVal := p.cur.Value
		where[wCol] = wVal
		p.nextToken()
	}
	return &UpdateStmt{DB: db, Table: table, Set: set, Where: where}, nil
}

func (p *Parser) parseDelete() (*DeleteStmt, error) {
	p.nextToken() // skip DELETE
	if p.cur.Type != FROM {
		return nil, fmt.Errorf("expected FROM")
	}
	p.nextToken()
	db, table, err := p.parseTableIdentifier()
	if err != nil {
		return nil, err
	}

	where := make(map[string]interface{})
	if p.cur.Type == WHERE {
		p.nextToken()
		wCol := p.cur.Value
		p.nextToken()
		if p.cur.Type != EQUAL {
			return nil, fmt.Errorf("expected = in WHERE")
		}
		p.nextToken()
		wVal := p.cur.Value
		where[wCol] = wVal
		p.nextToken()
	}

	return &DeleteStmt{DB: db, Table: table, Where: where}, nil
}

func (p *Parser) parseCreate() (*CreateStmt, error) {
	p.nextToken() // skip CREATE

	if p.cur.Type == DATABASE {
		p.nextToken()
		db := p.cur.Value
		p.nextToken()
		return &CreateStmt{Type: "DATABASE", DB: db}, nil
	} else if p.cur.Type == TABLE {
		p.nextToken()
		db, table, err := p.parseTableIdentifier()
		if err != nil {
			return nil, err
		}

		if p.cur.Type != LPAREN {
			return nil, fmt.Errorf("expected ( definition")
		}
		p.nextToken()

		cols := make(map[string]interface{}) // complex defs usually
		// Simple parser: name type, name type
		for {
			if p.cur.Type != IDENTIFIER {
				break
			}
			name := p.cur.Value
			p.nextToken()
			typ := p.cur.Value
			p.nextToken()

			// basic translation to map expected by schema API
			cols[name] = map[string]interface{}{"type": typ}

			if p.cur.Type == COMMA {
				p.nextToken()
			} else {
				break
			}
		}
		if p.cur.Type != RPAREN {
			return nil, fmt.Errorf("expected )")
		}
		p.nextToken()
		return &CreateStmt{Type: "TABLE", DB: db, Table: table, Columns: cols}, nil
	}

	return nil, fmt.Errorf("expected DATABASE or TABLE")
}

func (p *Parser) parseDrop() (*DropStmt, error) {
	p.nextToken()
	if p.cur.Type == DATABASE {
		p.nextToken()
		db := p.cur.Value
		p.nextToken()
		return &DropStmt{Type: "DATABASE", DB: db}, nil
	} else if p.cur.Type == TABLE {
		p.nextToken()
		db, table, err := p.parseTableIdentifier()
		if err != nil {
			return nil, err
		}
		return &DropStmt{Type: "TABLE", DB: db, Table: table}, nil
	}
	return nil, fmt.Errorf("expected DATABASE or TABLE")
}

func (p *Parser) parseAlter() (*AlterStmt, error) {
	p.nextToken() // skip ALTER
	if p.cur.Type != TABLE {
		return nil, fmt.Errorf("expected TABLE")
	}
	p.nextToken()

	db, table, err := p.parseTableIdentifier()
	if err != nil {
		return nil, err
	}

	// ALTER TABLE db.table ADD col type
	// ALTER TABLE db.table DROP col
	// ALTER TABLE db.table MODIFY col type

	action := strings.ToUpper(p.cur.Value)
	if action == "ADD" || action == "MODIFY" {
		p.nextToken()
		col := p.cur.Value
		p.nextToken()
		typ := p.cur.Value
		p.nextToken()
		return &AlterStmt{
			DB: db, Table: table, Action: action,
			ColName: col,
			ColDef:  map[string]interface{}{"type": typ},
		}, nil
	} else if action == "DROP" {
		p.nextToken()
		col := p.cur.Value
		p.nextToken()
		return &AlterStmt{
			DB: db, Table: table, Action: action,
			ColName: col,
		}, nil
	}

	return nil, fmt.Errorf("unknown alter action %s", action)
}

func (p *Parser) parseRename() (*RenameStmt, error) {
	p.nextToken() // skip RENAME
	if p.cur.Type == DATABASE {
		p.nextToken()
		oldName := p.cur.Value
		p.nextToken()
		if strings.ToUpper(p.cur.Value) != "TO" {
			return nil, fmt.Errorf("expected TO")
		}
		p.nextToken()
		newName := p.cur.Value
		p.nextToken()
		return &RenameStmt{Type: "DATABASE", OldName: oldName, NewName: newName}, nil

	} else if p.cur.Type == TABLE {
		p.nextToken()
		// rename table db.old to new
		// OR rename table db.old to db.new (usually just new name is enough if within same db)
		// Schema API supports separate args for db, old, new

		db, oldName, err := p.parseTableIdentifier()
		if err != nil {
			return nil, err
		}

		if strings.ToUpper(p.cur.Value) != "TO" {
			return nil, fmt.Errorf("expected TO")
		}
		p.nextToken()

		newName := p.cur.Value // Assume just table name
		p.nextToken()

		return &RenameStmt{Type: "TABLE", DB: db, OldName: oldName, NewName: newName}, nil
	}
	return nil, fmt.Errorf("expected DATABASE or TABLE")
}
