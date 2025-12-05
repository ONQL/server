package evaluator

import (
	"fmt"
	"onql/dsl/parser"
)

// EvalJsonProperty evaluates access to JSON properties on validated JSON data
func (e *Evaluator) EvalJsonProperty() error {
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpAccessJsonProperty && stmt.Operation != parser.OpUnknownIdentifier {
		return fmt.Errorf("expected JSON property access operation, got %s", stmt.Operation)
	}

	data := e.Memory[stmt.Sources[0].SourceValue]

	// Validate data is JSON-compatible
	if !isJsonCompatible(data) {
		if stmt.Operation == parser.OpAccessJsonProperty {
			// This should have been OpUnknownIdentifier
			return fmt.Errorf(
				"cannot access JSON property '%s' on non-JSON data (type: %T). "+
					"Parent data is not a JSON object or array",
				stmt.Expressions, data,
			)
		}
		// OpUnknownIdentifier - this is expected to potentially fail
		return fmt.Errorf(
			"cannot access property '%s' on type %T. "+
				"Property access is only supported on JSON objects and arrays",
			stmt.Expressions, data,
		)
	}

	// Data is JSON-compatible, proceed with access
	switch data := data.(type) {
	case map[string]any:
		e.SetMemoryValue(stmt.Name, data[stmt.Expressions.(string)])
		// e.Memory[stmt.Name] = data[stmt.Expressions.(string)]
		// e.Memory[stmt.Name+"_meta_type"] = getStructureType(data[stmt.Expressions.(string)])
	case []map[string]any:
		result := make([]any, 0)
		for _, item := range data {
			result = append(result, item[stmt.Expressions.(string)])
		}
		e.SetMemoryValue(stmt.Name, result)
		// e.Memory[stmt.Name] = result
		// e.Memory[stmt.Name+"_meta_type"] = getStructureType(result)
	case []any:
		result := make([]any, 0)
		for _, item := range data {
			// Handle nil values gracefully
			if item == nil {
				result = append(result, nil)
				continue
			}

			// Try to access property on map
			if m, ok := item.(map[string]any); ok {
				result = append(result, m[stmt.Expressions.(string)])
			} else {
				// Not a map, append nil
				result = append(result, nil)
			}
		}
		e.SetMemoryValue(stmt.Name, result)
	// cases any

	default:
		return fmt.Errorf("unsupported data type %T for JSON field access", data)
	}
	return nil
}

// isJsonCompatible checks if data is JSON-compatible (can have properties accessed)
func isJsonCompatible(data any) bool {
	if data == nil {
		return false
	}

	switch data.(type) {
	case map[string]any:
		// JSON object
		return true
	case []map[string]any:
		// Array of JSON objects
		return true
	case []any:
		// Generic array (could be JSON)
		return true
	default:
		// Primitives, typed arrays, etc. - not JSON objects
		return false
	}
}

// EvalUnknownIdentifier handles property access on non-JSON data
// This typically indicates an error in the query (accessing non-existent properties)
func (e *Evaluator) EvalUnknownIdentifier() error {
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpUnknownIdentifier {
		return fmt.Errorf("expected unknown identifier operation, got %s", stmt.Operation)
	}

	data := e.Memory[stmt.Sources[0].SourceValue]
	fieldName := stmt.Expressions.(string)

	// Try to access the property anyway (might work for some Go types)
	// But provide clear error messages when it fails
	switch data := data.(type) {
	case map[string]any:
		// It's actually JSON - access it
		e.SetMemoryValue(stmt.Name, data[fieldName])
		return nil

	case []map[string]any:
		// Array of maps - extract field
		result := make([]any, 0)
		for _, item := range data {
			result = append(result, item[fieldName])
		}
		e.SetMemoryValue(stmt.Name, result)
		return nil

	case []any:
		// Generic array - try to extract field
		result := make([]any, 0)
		for _, item := range data {
			// Handle nil values gracefully
			if item == nil {
				result = append(result, nil)
				continue
			}

			if m, ok := item.(map[string]any); ok {
				result = append(result, m[fieldName])
			} else {
				// Can't access property on non-object
				return fmt.Errorf(
					"cannot access property '%s' on non-object type %T in array. "+
						"Property access requires JSON objects",
					fieldName, item,
				)
			}
		}
		e.SetMemoryValue(stmt.Name, result)
		return nil

	default:
		// Unsupported type for property access
		return fmt.Errorf(
			"cannot access property '%s' on type %T. "+
				"Property access is only supported on JSON objects (map[string]any). "+
				"Available operations: %s",
			fieldName, data, getSupportedOperations(data),
		)
	}
}

// getSupportedOperations suggests what operations are valid for a given data type
func getSupportedOperations(data any) string {
	switch data.(type) {
	case string:
		return "string operations (future: .length, .upper, .lower)"
	case float64, int, int64:
		return "numeric operations (future: .abs, .round)"
	case []string:
		return "array operations (._count, ._unique, ._asc, ._desc)"
	case []float64:
		return "array operations (._sum, ._avg, ._min, ._max, ._count)"
	case []any:
		return "array operations (._count, ._unique)"
	default:
		return "no operations currently supported"
	}
}

// EvalUnknown is deprecated - use EvalJsonProperty or EvalUnknownIdentifier
// Kept for backward compatibility
func (e *Evaluator) EvalUnknown() error {
	stmt := e.Plan.NextStatement(false) // peek without advancing
	if stmt.Operation == parser.OpAccessJsonProperty {
		return e.EvalJsonProperty()
	}
	return e.EvalUnknownIdentifier()
}

// EvalJsonField is deprecated - use EvalJsonProperty instead
// Kept for backward compatibility
func (e *Evaluator) EvalJsonField() error {
	return e.EvalJsonProperty()
}
