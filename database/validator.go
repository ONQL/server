/*
Business Source License 1.1

Parameters
Licensor:             Autobit Software Services Private Limited
Licensed Work:        ONQL (Database Engine)
The Licensed Work is (c) 2025 Autobit Software Services Private Limited.
Change Date:          2028-01-01
Change License:       GNU General Public License, version 3 or later

Terms
The Business Source License (this “License”) grants you the right to copy,
modify, and redistribute the Licensed Work, provided that you do not use the
Licensed Work for a Commercial Use.

“Commercial Use” means offering the Licensed Work to third parties as a
paid service, product, or part of a service or product for which you or a
third party receives payment or other consideration.

You may make use of the Licensed Work for internal use, research, evaluation,
education, and non-commercial purposes, and you may contribute modifications
back to the Licensor under the same License.

Before the Change Date, use of the Licensed Work in violation of this License
automatically terminates your rights.  After the Change Date, the Licensed Work
will be governed by the Change License.

The Licensor may make an Additional Use Grant allowing specific commercial
uses by prior written permission.

THE LICENSED WORK IS PROVIDED “AS IS” AND WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT.

This License does not grant trademark rights.  The ONQL name and logo are
trademarks of Autobit Software Services Private Limited and may not be used
without written permission.

For more details see: https://mariadb.com/bsl11/
*/

package database

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Validate checks if a value satisfies a set of validation rules.
// Supported rules:
// - required: The value must not be nil or empty.
// - min:<value>: The value must be at least the specified minimum (length for strings, value for numbers).
// - numeric: The value must be a number or a string representing a number.
func Validate(value interface{}, rules []string) error {
	if len(rules) == 0 {
		return nil
	}

	for _, rule := range rules {
		ruleParts := strings.Split(rule, ":")
		ruleName := ruleParts[0]

		switch ruleName {
		case "required":
			if value == nil || value == "" {
				return fmt.Errorf("field is required")
			}
		case "min":
			if len(ruleParts) < 2 {
				return fmt.Errorf("min rule requires value")
			}
			minVal, err := strconv.Atoi(ruleParts[1])
			if err != nil {
				return err
			}
			switch v := value.(type) {
			case string:
				if len(v) < minVal {
					return fmt.Errorf("length must be at least %d", minVal)
				}
			case float64:
				if v < float64(minVal) {
					return fmt.Errorf("value must be at least %d", minVal)
				}
			case int:
				if v < minVal {
					return fmt.Errorf("value must be at least %d", minVal)
				}
			}
		case "numeric":
			switch value.(type) {
			case int, float64:
				// ok
			case string:
				if _, err := strconv.ParseFloat(value.(string), 64); err != nil {
					return fmt.Errorf("must be numeric")
				}
			default:
				return fmt.Errorf("must be numeric")
			}
		}
	}
	return nil
}

// ValidateType checks if a value matches the expected DataType.
// Supported types: string, number, timestamp, json.
func ValidateType(value interface{}, dataType string) error {
	switch dataType {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string")
		}
	case "number":
		switch value.(type) {
		case int, float64:
			// ok
		default:
			return fmt.Errorf("expected number")
		}
	case "timestamp":
		// Expecting time.Time or string that can be parsed?
		// Let's assume int64 (unix) or string.
		// For now, strict check.
		switch value.(type) {
		case int64, float64: // Unix timestamp
		case string:
			// Try parse?
			if _, err := time.Parse(time.RFC3339, value.(string)); err != nil {
				return fmt.Errorf("expected timestamp (RFC3339)")
			}
		default:
			return fmt.Errorf("expected timestamp")
		}
	case "json":
		// Check if it can be marshaled or is a map/slice
		if _, err := json.Marshal(value); err != nil {
			return fmt.Errorf("invalid json")
		}
	}
	return nil
}
