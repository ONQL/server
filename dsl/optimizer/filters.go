package optimizer

import (
	"onql/dsl/parser"
	"strings"
)

// ParseFilters extracts filter conditions from the plan starting at the current position.
// It consumes statements until OpEndFilter is reached.
// It returns a list of filter strings and the new position in the plan.
// Note: This function advances the plan position during parsing but returns the final position.
// The caller might need to manage plan position carefully if using this for analysis only.
// However, typically Evaluator advances.
// The user asked to "move evaluator genfilter function in optomizer".
// This implies it might be used by Evaluator via Optimizer package.
func ParseFilters(plan *parser.Plan) []string {
	stmt := plan.NextStatement(true)
	if stmt == nil || stmt.Operation != parser.OpStartFilter {
		return nil
	}

	filters := make([]string, 0, 8)
	var colName, colVal string

	flush := func() {
		if colName == "" || colVal == "" {
			return
		}
		v := strings.TrimSpace(colVal)
		// strip quotes if present
		if n := len(v); n >= 2 && ((v[0] == '"' && v[n-1] == '"') || (v[0] == '\'' && v[n-1] == '\'')) {
			v = v[1 : n-1]
		}
		filters = append(filters, colName+":"+v)
		colName, colVal = "", ""
	}

	for {
		stmt = plan.NextStatement(true)
		if stmt == nil {
			break
		}
		if stmt.Operation == parser.OpEndFilter {
			flush()
			break
		}

		switch stmt.Operation {
		case parser.OpAccessList:
			// left side (column)
			colName = stmt.Meta["name"]

		case parser.OpLiteral:
			// right side (value) for '='
			colVal = stmt.Expressions.(string)

		case parser.OpNormalOperation:
			// expecting one of: "=", "==", "and", "or"
			// Expression is "Left OP Right", e.g. "C = D"
			parts := strings.Split(stmt.Expressions.(string), " ")
			if len(parts) < 2 {
				return nil
			}
			op := strings.ToLower(strings.TrimSpace(parts[1]))
			switch op {
			case "=", "==":
				// do nothing here; we'll flush when literal arrives
			case "and", "or":
				flush() // ensure previous expr emitted
				filters = append(filters, op)
			default:
				return nil
				// ignore anything else (e.g., "!=" not supported)
			}
		default:
			return nil
		}
		if colName != "" && colVal != "" {
			flush()
		}
	}
	return filters
}
