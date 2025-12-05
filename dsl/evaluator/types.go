package evaluator

import (
	"encoding/json"
	"fmt"
	"onql/dsl/parser"
	"strings"
)

type Evaluator struct {
	Plan           *parser.Plan
	Memory         map[string]any
	Result         any
	ContextKey     string
	ContextValues  []string
	ProjectionPath []string
}

func NewEvaluator(plan *parser.Plan, ContextKey string, contextValues []string) *Evaluator {
	return &Evaluator{
		Plan:          plan,
		Memory:        make(map[string]any),
		Result:        make([]any, 0),
		ContextKey:    ContextKey,
		ContextValues: contextValues,
	}
}

// func (e *Evaluator) SetMemoryValue(key string, value any) {
// 	e.Memory[key] = value
// 	e.Memory[key+"_meta_type"] = getStructureType(value)
// }

// func getStructureType(val any) string {
// 	switch val.(type) {
// 	case map[string]any:
// 		return "ROW"
// 	case []map[string]any:
// 		return "TABLE"
// 	case []string:
// 		return "ARRAY_OF_STRING"
// 	case []float64:
// 		return "ARRAY_OF_NUMBER"
// 	case []any:
// 		return "ARRAY_OF_UNKNOWN"
// 	case string:
// 		return "STRING"
// 	case float64:
// 		return "NUMBER"
// 	case bool:
// 		return "BOOL"
// 	default:
// 		return strings.ToUpper(fmt.Sprintf("%T", val))
// 	}
// }

// ---- NEW: canonicalize value before saving
func (e *Evaluator) SetMemoryValue(key string, value any) {
	v := narrowTypes(value) // <— convert []any → []string / []float64 / []map[string]any where possible
	e.Memory[key] = v
	e.Memory[key+"_meta_type"] = getStructureType(v)
}

// ---- Helper: recursively tighten JSON-like structures (no reflection)
func narrowTypes(v any) any {
	switch x := v.(type) {
	case nil, string, bool, float64, float32,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		json.Number:
		// Primitives: normalize numbers to float64
		if f, ok := asFloat64(v); ok {
			return f
		}
		return v

	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = narrowTypes(val)
		}
		return out

	case []map[string]any:
		// Already a TABLE; just narrow each row
		for i := range x {
			row := x[i]
			for k, val := range row {
				row[k] = narrowTypes(val)
			}
		}
		return x

	case []any:
		if len(x) == 0 {
			return []any{}
		}
		// First narrow each element
		tmp := make([]any, len(x))
		for i, e := range x {
			tmp[i] = narrowTypes(e)
		}

		// Then try to tighten the slice
		if allString(tmp) {
			out := make([]string, len(tmp))
			for i, e := range tmp {
				out[i] = e.(string)
			}
			return out
		}
		if allNumber(tmp) {
			out := make([]float64, len(tmp))
			for i, e := range tmp {
				f, _ := asFloat64(e)
				out[i] = f
			}
			return out
		}
		if allBool(tmp) {
			out := make([]bool, len(tmp))
			for i, e := range tmp {
				out[i] = e.(bool)
			}
			return out
		}
		if allMap(tmp) {
			out := make([]map[string]any, len(tmp))
			for i, e := range tmp {
				out[i] = e.(map[string]any)
			}
			return out
		}
		// Mixed → keep []any
		return tmp

	default:
		// Unknown concrete type—leave as-is
		return v
	}
}

// ---- Type tests (no reflection)
func asFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case json.Number:
		if f, err := n.Float64(); err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func allString(vs []any) bool {
	for _, v := range vs {
		if _, ok := v.(string); !ok {
			return false
		}
	}
	return true
}
func allBool(vs []any) bool {
	for _, v := range vs {
		if _, ok := v.(bool); !ok {
			return false
		}
	}
	return true
}
func allNumber(vs []any) bool {
	for _, v := range vs {
		if _, ok := asFloat64(v); !ok {
			return false
		}
	}
	return true
}
func allMap(vs []any) bool {
	for _, v := range vs {
		if _, ok := v.(map[string]any); !ok {
			return false
		}
	}
	return true
}

// ---- Improved type labeler
func getStructureType(val any) string {
	if val == nil {
		return "NULL"
	}
	switch val.(type) {
	case map[string]any:
		return "ROW"
	case []map[string]any:
		return "TABLE"
	case []string:
		return "ARRAY_OF_STRING"
	case []float64:
		return "ARRAY_OF_NUMBER"
	case []bool:
		return "ARRAY_OF_BOOL"
	case []any:
		return "ARRAY_OF_UNKNOWN"
	case string:
		return "STRING"
	case bool:
		return "BOOL"
	case float64, float32,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		json.Number:
		return "NUMBER"
	default:
		return strings.ToUpper(fmt.Sprintf("%T", val))
	}
}
