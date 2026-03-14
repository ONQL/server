package optimizer

import (
	"onql/dsl/parser"
	"strings"
)

// ParseFilters extracts equality-based filter conditions from the plan as an RPN token list,
// suitable for index-based pushdown via GetTableWithDataWithFilters.
//
// Supports:
//   - Simple equality:  col = val
//   - Compound AND/OR:  col1=v1 and col2=v2
//   - Grouped OR:       col1=v1 and (col2=v2 or col3=v3)
//   - Multi-group:      col1=v1 and (col2=v2 or col3=v3) and col4=v4
//
// Returns nil when any condition uses a non-equality operator (!=, <, >, etc.)
// or when the column reference is a relational/nested access (e.g. category[0].name),
// so the caller falls back to in-memory filter evaluation.
//
// Strategy: walk every statement inside the filter block. For each NO statement:
//   - op "=" → resolve left/right via StatementMap, emit "col:val"
//   - op "and"/"or" → emit the operator
//   ATL/LIT/other statements are skipped (they are referenced by NO statements).
func ParseFilters(plan *parser.Plan) []string {
	stmt := plan.NextStatement(true)
	if stmt == nil || stmt.Operation != parser.OpStartFilter {
		return nil
	}

	filters := make([]string, 0, 8)

	for {
		stmt = plan.NextStatement(true)
		if stmt == nil {
			break
		}
		if stmt.Operation == parser.OpEndFilter {
			break
		}

		// Only NormalOperation statements carry actionable information.
		// ATL / LIT / other statements are sub-expressions referenced by NO; skip them.
		if stmt.Operation != parser.OpNormalOperation {
			continue
		}

		parts := strings.Split(stmt.Expressions.(string), " ")
		if len(parts) < 3 {
			return nil
		}
		leftName := parts[0]
		op := strings.ToLower(strings.TrimSpace(parts[1]))
		rightName := parts[2]

		switch op {
		case "=", "==":
			// Resolve the two operands to find which is the column and which is the literal.
			leftStmt := plan.StatementMap[leftName]
			rightStmt := plan.StatementMap[rightName]
			if leftStmt == nil || rightStmt == nil {
				return nil
			}

			var colName, colVal string

			// isDirectColOp checks that s is a direct column access on the filtered
			// table — its source must be the StartFilter statement itself.
			// Relational accesses like category[0].name have an intermediate ATR/ART
			// in the source chain (ARF → ATR → ART → SFT), so their source is NOT
			// SFT and they correctly fall through to the default case, which returns
			// nil and forces in-memory evaluation.
			isDirectColOp := func(s *parser.Statement) bool {
				if s.Operation != parser.OpAccessList && s.Operation != parser.OpAccessField {
					return false
				}
				if len(s.Sources) == 0 {
					return false
				}
				srcStmt := plan.StatementMap[s.Sources[0].SourceValue]
				return srcStmt != nil && srcStmt.Operation == parser.OpStartFilter
			}

			switch {
			case isDirectColOp(leftStmt) && rightStmt.Operation == parser.OpLiteral:
				colName = leftStmt.Meta["name"]
				colVal = rightStmt.Expressions.(string)
			case isDirectColOp(rightStmt) && leftStmt.Operation == parser.OpLiteral:
				// val = col  (reversed — treat same as col = val)
				colName = rightStmt.Meta["name"]
				colVal = leftStmt.Expressions.(string)
			default:
				// Operands are not a simple col/literal pair (e.g. col = col,
				// relational access like category[0].name, or a sub-expression).
				// We cannot push this down via index.
				return nil
			}

			if colName == "" {
				return nil
			}

			// Strip surrounding quotes added by the lexer context-value substitution.
			v := strings.TrimSpace(colVal)
			if n := len(v); n >= 2 &&
				((v[0] == '"' && v[n-1] == '"') || (v[0] == '\'' && v[n-1] == '\'')) {
				v = v[1 : n-1]
			}
			filters = append(filters, colName+":"+v)

		case "and", "or":
			// Logical combinator — emit as RPN operator.
			// Both operands must themselves be NO statements (equality or combinator results)
			// to be valid for pushdown. If either is an unknown/complex type, bail.
			leftStmt := plan.StatementMap[leftName]
			rightStmt := plan.StatementMap[rightName]
			if leftStmt == nil || rightStmt == nil {
				return nil
			}
			if leftStmt.Operation != parser.OpNormalOperation ||
				rightStmt.Operation != parser.OpNormalOperation {
				return nil
			}
			filters = append(filters, op)

		default:
			// Any other operator (!=, <, >, >=, <=, in, …) cannot be satisfied
			// by a simple index lookup — abort pushdown for the whole filter.
			return nil
		}
	}

	if len(filters) == 0 {
		return nil
	}
	return filters
}
