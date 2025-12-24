// package parser

// import (
// 	"github.com/timtadh/lexmachine"
// 	"github.com/timtadh/lexmachine/machines"
// )

// const (
// 	// Basic types
// 	TOKEN_STRING = iota
// 	TOKEN_NUMBER
// 	TOKEN_IDENTIFIER

// 	// Symbols
// 	TOKEN_LBRACKET // [
// 	TOKEN_RBRACKET // ]
// 	TOKEN_LPAREN   // (
// 	TOKEN_RPAREN   // )
// 	TOKEN_LCURLEY  // {
// 	TOKEN_RCURLEY  // }
// 	TOKEN_DOT      // .
// 	TOKEN_JOIN     // ::
// 	TOKEN_COMMA    // ,
// 	TOKEN_COLON    // :
// 	TOKEN_EQUAL    // =
// 	TOKEN_GT       // >
// 	TOKEN_LT       // <
// 	TOKEN_NE       // !=
// 	TOKEN_GE       // >=
// 	TOKEN_LE       // <=

// 	// Arithmetic
// 	TOKEN_PLUS  // +
// 	TOKEN_MINUS // -
// 	TOKEN_MUL   // *
// 	TOKEN_DIV   // /
// 	TOKEN_MOD   // %

// 	// Keywords
// 	TOKEN_AND
// 	TOKEN_OR
// 	TOKEN_IN
// 	TOKEN_NOT
// 	TOKEN_AS
// 	TOKEN_DOLLAR // $ for variables

// 	// New Tokens
// 	TOKEN_ROW_ACCESS // [10]
// 	TOKEN_SLICE      // [1:5] or [1:10:2]

// 	// Ignored
// 	TOKEN_WHITESPACE
// )

// type Token struct {
// 	Type  int
// 	Value string
// 	Pos   int // position in the input string
// }

// var TokenNames = map[int]string{
// 	TOKEN_STRING:     "STRING",
// 	TOKEN_NUMBER:     "NUMBER",
// 	TOKEN_IDENTIFIER: "IDENTIFIER",
// 	TOKEN_LBRACKET:   "[",
// 	TOKEN_RBRACKET:   "]",
// 	TOKEN_LPAREN:     "(",
// 	TOKEN_RPAREN:     ")",
// 	TOKEN_LCURLEY:    "{",
// 	TOKEN_RCURLEY:    "}",
// 	TOKEN_DOT:        ".",
// 	TOKEN_JOIN:       "::",
// 	TOKEN_COMMA:      ",",
// 	TOKEN_COLON:      ":",
// 	TOKEN_EQUAL:      "=",
// 	TOKEN_GT:         ">",
// 	TOKEN_LT:         "<",
// 	TOKEN_NE:         "!=",
// 	TOKEN_GE:         ">=",
// 	TOKEN_LE:         "<=",
// 	TOKEN_PLUS:       "+",
// 	TOKEN_MINUS:      "-",
// 	TOKEN_MUL:        "*",
// 	TOKEN_DIV:        "/",
// 	TOKEN_MOD:        "%",
// 	TOKEN_AND:        "AND",
// 	TOKEN_OR:         "OR",
// 	TOKEN_IN:         "IN",
// 	TOKEN_NOT:        "NOT",
// 	TOKEN_AS:         "AS",
// 	TOKEN_DOLLAR:     "$",
// 	TOKEN_ROW_ACCESS: "ROW_ACCESS",
// 	TOKEN_SLICE:      "SLICE",
// }

// var TokenRules = []struct {
// 	Type  int
// 	Regex string
// }{
// 	// Longer operators first
// 	{TOKEN_NE, `!=`},
// 	{TOKEN_GE, `>=`},
// 	{TOKEN_LE, `<=`},
// 	{TOKEN_JOIN, `::`},
// 	{TOKEN_EQUAL, `=`},
// 	{TOKEN_GT, `>`},
// 	{TOKEN_LT, `<`},

// 	// Literals and identifiers
// 	{TOKEN_STRING, `"([^"\\]|\\.)*"`},
// 	{TOKEN_NUMBER, `\d+(\.\d+)?`},
// 	{TOKEN_IDENTIFIER, `[a-zA-Z_][a-zA-Z0-9_]*`},

// 	// --- UPDATED TOKEN RULE ---
// 	// Must be defined before the general LBRACKET rule
// 	// {TOKEN_SLICE, `\[\d*:\d*(?::\d*)?\]`}, // Matches [start:stop] and [start:stop:step]
// 	// {TOKEN_SLICE, `\[\s*(-?\d+)?\s*:\s*(-?\d+)?\s*(?::\s*(-?\d+)?)?\s*\]`}, // [:], [10:], [:20], [10:20], [::3], [2:3:5]
// 	{TOKEN_SLICE, `\[\s*-?\d*\s*:\s*-?\d*\s*(:\s*-?\d*)?\s*\]`},
// 	{TOKEN_ROW_ACCESS, `\[\d+\]`}, // Matches row access like [5]

// 	// Symbols
// 	{TOKEN_LBRACKET, `\[`},
// 	{TOKEN_RBRACKET, `\]`},
// 	{TOKEN_LPAREN, `\(`},
// 	{TOKEN_RPAREN, `\)`},
// 	{TOKEN_LCURLEY, `\{`},
// 	{TOKEN_RCURLEY, `\}`},
// 	{TOKEN_DOT, `\.`},
// 	{TOKEN_COMMA, `,`},
// 	{TOKEN_COLON, `:`},
// 	{TOKEN_DOLLAR, `\$`}, // Dollar sign for variables

// 	// Arithmetic operators
// 	{TOKEN_PLUS, `\+`},
// 	{TOKEN_MINUS, `-`},
// 	{TOKEN_MUL, `\*`},
// 	{TOKEN_DIV, `/`},
// 	{TOKEN_MOD, `%`},

// 	// Keywords
// 	{TOKEN_AND, `\band\b`},
// 	{TOKEN_OR, `\bor\b`},
// 	{TOKEN_NOT, `\bnot\b`},
// 	{TOKEN_IN, `\bin\b`},
// 	{TOKEN_AS, `\bas\b`},

// 	// Whitespace
// 	{TOKEN_WHITESPACE, `\s+`},
// }

// var LexMach = lexmachine.NewLexer()

// type Lexer struct {
// 	tokens []Token
// 	pos    int
// }

// func init() {
// 	for _, rule := range TokenRules {
// 		rule := rule // shadow loop var
// 		LexMach.Add([]byte(rule.Regex), func(s *lexmachine.Scanner, m *machines.Match) (any, error) {
// 			if rule.Type == TOKEN_WHITESPACE {
// 				return nil, nil // skip whitespace
// 			}

// 			return Token{Type: rule.Type, Value: string(m.Bytes)}, nil
// 		})
// 	}
// 	if err := LexMach.Compile(); err != nil {
// 		panic(err)
// 	}

// }

// func NewLexer(query string) *Lexer {
// 	scanner, _ := LexMach.Scanner([]byte(query))
// 	tokens := []Token{}
// 	i := 0
// 	for {
// 		tok, err, eof := scanner.Next()
// 		if eof {
// 			break
// 		}
// 		if err != nil {
// 			panic(err)
// 		}
// 		if tok == nil {
// 			continue // skip whitespace or nil tokens
// 		}
// 		token := tok.(Token)
// 		token.Pos = i
// 		tokens = append(tokens, token)
// 		i++
// 	}

// 	return &Lexer{tokens: tokens, pos: 0}
// }

// // Next returns the current token. If advance is true, it moves to the next token.
// func (l *Lexer) Next(advance bool) *Token {
// 	if l.pos >= len(l.tokens) {
// 		return nil
// 	}
// 	tok := &l.tokens[l.pos]
// 	if advance {
// 		l.pos++
// 	}
// 	return tok
// }

// // Prev returns the previous token. If advance is true, it moves the position backward.
// func (l *Lexer) Prev(advance bool) *Token {
// 	if l.pos <= 0 {
// 		return nil
// 	}
// 	if advance {
// 		l.pos--
// 	}
// 	return &l.tokens[l.pos-1]
// }

// // Seek moves to a specific index. If advance is true, updates internal position.
// func (l *Lexer) Seek(pos int, advance bool) *Token {
// 	if pos < 0 || pos >= len(l.tokens) {
// 		return nil
// 	}
// 	if advance {
// 		l.pos = pos
// 	}
// 	return &l.tokens[pos]
// }

// func (L *Lexer) HasNext() bool {
// 	return L.pos < len(L.tokens)-1
// }

package parser

import (
	"strings"

	"github.com/timtadh/lexmachine"
	"github.com/timtadh/lexmachine/machines"
)

// ----- Token Kinds -----

const (
	// Basic types
	TOKEN_STRING = iota
	TOKEN_NUMBER
	TOKEN_IDENTIFIER

	// Symbols
	TOKEN_LBRACKET // [
	TOKEN_RBRACKET // ]
	TOKEN_LPAREN   // (
	TOKEN_RPAREN   // )
	TOKEN_LCURLEY  // {
	TOKEN_RCURLEY  // }
	TOKEN_DOT      // .
	TOKEN_JOIN     // ::
	TOKEN_COMMA    // ,
	TOKEN_COLON    // :
	TOKEN_EQUAL    // =
	TOKEN_GT       // >
	TOKEN_LT       // <
	TOKEN_NE       // !=
	TOKEN_GE       // >=
	TOKEN_LE       // <=

	// Arithmetic
	TOKEN_PLUS  // +
	TOKEN_MINUS // -
	TOKEN_MUL   // *
	TOKEN_DIV   // /
	TOKEN_MOD   // %

	// Keywords (mapped case-insensitively from IDENTIFIER)
	TOKEN_AND
	TOKEN_OR
	TOKEN_IN
	TOKEN_NOT
	TOKEN_AS
	TOKEN_DOLLAR // $ for variables

	// New Tokens
	TOKEN_ROW_ACCESS // [10]
	TOKEN_SLICE      // [1:5] or [1:10:2]

	// Ignored
	TOKEN_WHITESPACE
)

// ----- Token & Names -----

type Token struct {
	Type  int
	Value string
	Pos   int // index in the token stream
}

var TokenNames = map[int]string{
	TOKEN_STRING:     "STRING",
	TOKEN_NUMBER:     "NUMBER",
	TOKEN_IDENTIFIER: "IDENTIFIER",
	TOKEN_LBRACKET:   "[",
	TOKEN_RBRACKET:   "]",
	TOKEN_LPAREN:     "(",
	TOKEN_RPAREN:     ")",
	TOKEN_LCURLEY:    "{",
	TOKEN_RCURLEY:    "}",
	TOKEN_DOT:        ".",
	TOKEN_JOIN:       "::",
	TOKEN_COMMA:      ",",
	TOKEN_COLON:      ":",
	TOKEN_EQUAL:      "=",
	TOKEN_GT:         ">",
	TOKEN_LT:         "<",
	TOKEN_NE:         "!=",
	TOKEN_GE:         ">=",
	TOKEN_LE:         "<=",
	TOKEN_PLUS:       "+",
	TOKEN_MINUS:      "-",
	TOKEN_MUL:        "*",
	TOKEN_DIV:        "/",
	TOKEN_MOD:        "%",
	TOKEN_AND:        "AND",
	TOKEN_OR:         "OR",
	TOKEN_IN:         "IN",
	TOKEN_NOT:        "NOT",
	TOKEN_AS:         "AS",
	TOKEN_DOLLAR:     "$",
	TOKEN_ROW_ACCESS: "ROW_ACCESS",
	TOKEN_SLICE:      "SLICE",
}

// ----- Rule Table -----
//
// IMPORTANT: Order matters. We keep SLICE and ROW_ACCESS before bare '['.

var TokenRules = []struct {
	Type  int
	Regex string
}{
	// Multi-char operators first
	{TOKEN_NE, `!=`},
	{TOKEN_GE, `>=`},
	{TOKEN_LE, `<=`},
	{TOKEN_JOIN, `::`},
	{TOKEN_EQUAL, `=`},
	{TOKEN_GT, `>`},
	{TOKEN_LT, `<`},

	// Literals and identifiers
	{TOKEN_STRING, `"([^"\\]|\\.)*"`},
	{TOKEN_NUMBER, `\d+(\.\d+)?`},
	{TOKEN_IDENTIFIER, `[a-zA-Z_][a-zA-Z0-9_]*`},

	// Slices and row access (keep before bare '[')
	{TOKEN_SLICE, `\[\s*-?\d*\s*:\s*-?\d*\s*(:\s*-?\d*)?\s*\]`}, // [:], [10:], [:20], [10:20], [::3], [2:3:5], negatives
	{TOKEN_ROW_ACCESS, `\[\s*\d+\s*\]`},                         // [5] (spaces allowed)

	// Symbols
	{TOKEN_LBRACKET, `\[`},
	{TOKEN_RBRACKET, `\]`},
	{TOKEN_LPAREN, `\(`},
	{TOKEN_RPAREN, `\)`},
	{TOKEN_LCURLEY, `\{`},
	{TOKEN_RCURLEY, `\}`},
	{TOKEN_DOT, `\.`},
	{TOKEN_COMMA, `,`},
	{TOKEN_COLON, `:`},
	{TOKEN_DOLLAR, `\$`}, // Dollar sign for variables

	// Arithmetic operators
	{TOKEN_PLUS, `\+`},
	{TOKEN_MINUS, `-`},
	{TOKEN_MUL, `\*`},
	{TOKEN_DIV, `/`},
	{TOKEN_MOD, `%`},

	// Whitespace (ignored)
	{TOKEN_WHITESPACE, `\s+`},
}

// ----- Lexer Impl -----

var LexMach = lexmachine.NewLexer()

type Lexer struct {
	tokens []Token
	pos    int
}

func init() {
	for _, rule := range TokenRules {
		rule := rule // capture loop var
		LexMach.Add([]byte(rule.Regex), func(s *lexmachine.Scanner, m *machines.Match) (any, error) {
			// Skip ignored tokens
			if rule.Type == TOKEN_WHITESPACE {
				return nil, nil
			}

			val := string(m.Bytes)
			tokType := rule.Type
			// Remove double quotes for string tokens
			if rule.Type == TOKEN_STRING {
				val = strings.Trim(val, `"`)
			}

			// Post-map IDENTIFIER -> keyword (case-insensitive)
			if rule.Type == TOKEN_IDENTIFIER {
				switch strings.ToLower(val) {
				case "and":
					tokType = TOKEN_AND
				case "or":
					tokType = TOKEN_OR
				case "not":
					tokType = TOKEN_NOT
				case "in":
					tokType = TOKEN_IN
				case "as":
					tokType = TOKEN_AS
				}
			}

			return Token{Type: tokType, Value: val}, nil
		})
	}
	if err := LexMach.Compile(); err != nil {
		panic(err)
	}
}

// NewLexer tokenizes the input string into a simple slice-backed stream.
// NewLexer tokenizes the input string into a simple slice-backed stream.
func NewLexer(query string) (*Lexer, error) {
	scanner, err := LexMach.Scanner([]byte(query))
	if err != nil {
		return nil, err
	}
	tokens := []Token{}
	i := 0
	for {
		tok, err, eof := scanner.Next()
		if eof {
			break
		}
		if err != nil {
			return nil, err
		}
		if tok == nil {
			continue // whitespace or skipped
		}
		token := tok.(Token)
		token.Pos = i
		tokens = append(tokens, token)
		i++
	}
	return &Lexer{tokens: tokens, pos: 0}, nil
}

// Next returns the current token; if advance is true, it moves to the next.
func (l *Lexer) Next(advance bool) *Token {
	if l.pos >= len(l.tokens) {
		return nil
	}
	tok := &l.tokens[l.pos]
	if advance {
		l.pos++
	}
	return tok
}

// Prev returns the previous token; if advance is true, it moves back one.
func (l *Lexer) Prev(advance bool) *Token {
	if l.pos <= 0 {
		return nil
	}
	if advance {
		l.pos--
	}
	return &l.tokens[l.pos-1]
}

// Seek moves to an absolute token index and returns that token (without moving unless advance is true).
func (l *Lexer) Seek(pos int, advance bool) *Token {
	if pos < 0 || pos >= len(l.tokens) {
		return nil
	}
	if advance {
		l.pos = pos
	}
	return &l.tokens[pos]
}

// HasNext reports whether there is at least one more token after the current position.
func (l *Lexer) HasNext() bool {
	return l.pos < len(l.tokens)-1
}
