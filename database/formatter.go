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
	"fmt"
	"strconv"
	"strings"
)

// Format applies a series of formatting rules to a value.
// Supported rules:
// - trim: Trims whitespace from strings.
// - lower: Converts strings to lowercase.
// - upper: Converts strings to uppercase.
// - decimal:<precision>: Rounds numbers to the specified precision.
func Format(value interface{}, rules []string) (interface{}, error) {
	if len(rules) == 0 {
		return value, nil
	}

	for _, rule := range rules {
		ruleParts := strings.Split(rule, ":")
		ruleName := ruleParts[0]

		switch ruleName {
		case "trim":
			if str, ok := value.(string); ok {
				value = strings.TrimSpace(str)
			}
		case "lower":
			if str, ok := value.(string); ok {
				value = strings.ToLower(str)
			}
		case "upper":
			if str, ok := value.(string); ok {
				value = strings.ToUpper(str)
			}
		case "prefix":
			if len(ruleParts) < 2 {
				continue
			}
			if str, ok := value.(string); ok {
				value = ruleParts[1] + str
			}
		case "suffix":
			if len(ruleParts) < 2 {
				continue
			}
			if str, ok := value.(string); ok {
				value = str + ruleParts[1]
			}
		case "decimal":
			if len(ruleParts) < 2 {
				return nil, fmt.Errorf("decimal rule requires precision")
			}
			precision, err := strconv.Atoi(ruleParts[1])
			if err != nil {
				return nil, err
			}
			// Handle float or string
			var f float64
			switch v := value.(type) {
			case float64:
				f = v
			case int:
				f = float64(v)
			case string:
				f, err = strconv.ParseFloat(v, 64)
				if err != nil {
					return nil, err
				}
			default:
				// Skip if not number
				continue
			}
			// Format to specific precision
			// Return as float or string? User said "decimal:2", usually implies rounding.
			// Let's return float rounded.
			formatStr := fmt.Sprintf("%%.%df", precision)
			valStr := fmt.Sprintf(formatStr, f)
			value, _ = strconv.ParseFloat(valStr, 64)
		}
	}
	return value, nil
}
