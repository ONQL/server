package parser

import (
	"strings"
	"text/scanner"
)

type Token int

const (
	// Keywords
	SELECT Token = iota
	INSERT
	UPDATE
	DELETE
	CREATE
	DROP
	ALTER
	FROM
	WHERE
	INTO
	VALUES
	SET
	TABLE
	DATABASE
	AND
	OR
	RENAME
	JOIN
	LEFT
	RIGHT
	FULL
	INNER
	OUTER
	ON

	// Literals
	IDENTIFIER
	STRING
	NUMBER

	// Symbols
	ASTERISK // *
	COMMA    // ,
	EQUAL    // =
	LPAREN   // (
	RPAREN   // )
	DOT      // .

	// Misc
	EOF
	ILLEGAL
)

var tokens = []string{
	SELECT:   "SELECT",
	INSERT:   "INSERT",
	UPDATE:   "UPDATE",
	DELETE:   "DELETE",
	CREATE:   "CREATE",
	DROP:     "DROP",
	ALTER:    "ALTER",
	FROM:     "FROM",
	WHERE:    "WHERE",
	INTO:     "INTO",
	VALUES:   "VALUES",
	SET:      "SET",
	TABLE:    "TABLE",
	DATABASE: "DATABASE",
	AND:      "AND",
	OR:       "OR",
	RENAME:   "RENAME",
	JOIN:     "JOIN",
	LEFT:     "LEFT",
	RIGHT:    "RIGHT",
	FULL:     "FULL",
	INNER:    "INNER",
	OUTER:    "OUTER",
	ON:       "ON",

	IDENTIFIER: "IDENTIFIER",
	STRING:     "STRING",
	NUMBER:     "NUMBER",

	ASTERISK: "*",
	COMMA:    ",",
	EQUAL:    "=",
	LPAREN:   "(",
	RPAREN:   ")",
	DOT:      ".",

	EOF:     "EOF",
	ILLEGAL: "ILLEGAL",
}

func (t Token) String() string {
	return tokens[t]
}

type TokenItem struct {
	Type  Token
	Value string
	Pos   int
}

type Lexer struct {
	scanner scanner.Scanner
	buf     strings.Builder
}

func NewLexer(input string) (*Lexer, error) {
	var s scanner.Scanner
	s.Init(strings.NewReader(input))
	s.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanStrings
	return &Lexer{scanner: s}, nil
}

func (l *Lexer) Scan() TokenItem {
	tok := l.scanner.Scan()
	pos := l.scanner.Position.Offset
	txt := l.scanner.TokenText()

	switch tok {
	case scanner.EOF:
		return TokenItem{Type: EOF, Value: "", Pos: pos}
	case scanner.Ident:
		switch strings.ToUpper(txt) {
		case "SELECT":
			return TokenItem{Type: SELECT, Value: txt, Pos: pos}
		case "INSERT":
			return TokenItem{Type: INSERT, Value: txt, Pos: pos}
		case "UPDATE":
			return TokenItem{Type: UPDATE, Value: txt, Pos: pos}
		case "DELETE":
			return TokenItem{Type: DELETE, Value: txt, Pos: pos}
		case "CREATE":
			return TokenItem{Type: CREATE, Value: txt, Pos: pos}
		case "DROP":
			return TokenItem{Type: DROP, Value: txt, Pos: pos}
		case "ALTER":
			return TokenItem{Type: ALTER, Value: txt, Pos: pos}
		case "FROM":
			return TokenItem{Type: FROM, Value: txt, Pos: pos}
		case "WHERE":
			return TokenItem{Type: WHERE, Value: txt, Pos: pos}
		case "INTO":
			return TokenItem{Type: INTO, Value: txt, Pos: pos}
		case "VALUES":
			return TokenItem{Type: VALUES, Value: txt, Pos: pos}
		case "SET":
			return TokenItem{Type: SET, Value: txt, Pos: pos}
		case "TABLE":
			return TokenItem{Type: TABLE, Value: txt, Pos: pos}
		case "DATABASE":
			return TokenItem{Type: DATABASE, Value: txt, Pos: pos}
		case "AND":
			return TokenItem{Type: AND, Value: txt, Pos: pos}
		case "OR":
			return TokenItem{Type: OR, Value: txt, Pos: pos}
		case "RENAME":
			return TokenItem{Type: RENAME, Value: txt, Pos: pos}
		case "JOIN":
			return TokenItem{Type: JOIN, Value: txt, Pos: pos}
		case "LEFT":
			return TokenItem{Type: LEFT, Value: txt, Pos: pos}
		case "RIGHT":
			return TokenItem{Type: RIGHT, Value: txt, Pos: pos}
		case "FULL":
			return TokenItem{Type: FULL, Value: txt, Pos: pos}
		case "INNER":
			return TokenItem{Type: INNER, Value: txt, Pos: pos}
		case "OUTER":
			return TokenItem{Type: OUTER, Value: txt, Pos: pos}
		case "ON":
			return TokenItem{Type: ON, Value: txt, Pos: pos}
		default:
			return TokenItem{Type: IDENTIFIER, Value: txt, Pos: pos}
		}
	case scanner.Float, scanner.Int:
		return TokenItem{Type: NUMBER, Value: txt, Pos: pos}
	case scanner.String:
		// Remove quotes
		return TokenItem{Type: STRING, Value: strings.Trim(txt, "\"`"), Pos: pos}
	default:
		switch txt {
		case "*":
			return TokenItem{Type: ASTERISK, Value: txt, Pos: pos}
		case ",":
			return TokenItem{Type: COMMA, Value: txt, Pos: pos}
		case "=":
			return TokenItem{Type: EQUAL, Value: txt, Pos: pos}
		case "(":
			return TokenItem{Type: LPAREN, Value: txt, Pos: pos}
		case ")":
			return TokenItem{Type: RPAREN, Value: txt, Pos: pos}
		case ".":
			return TokenItem{Type: DOT, Value: txt, Pos: pos}
		default:
			return TokenItem{Type: ILLEGAL, Value: txt, Pos: pos}
		}
	}
}
