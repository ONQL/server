package optimizer

import (
	"fmt"
	"onql/dsl/parser"
	"strconv"
	"strings"
)

// Optimizer is responsible for optimizing the query plan
type Optimizer struct {
	Plan *parser.Plan
}

func NewOptimizer(plan *parser.Plan) *Optimizer {
	return &Optimizer{Plan: plan}
}

// Optimize applies optimization rules to the plan
func (opt *Optimizer) Optimize() error {
	var newStatements []*parser.Statement
	stmts := opt.Plan.Statements
	skipIndices := make(map[int]bool)

	for i := 0; i < len(stmts); i++ {
		if skipIndices[i] {
			continue
		}
		stmt := stmts[i]

		// We currently only optimize Table Access patterns
		if stmt.Operation == parser.OpAccessTable {
			if i+1 < len(stmts) && !skipIndices[i+1] {
				nextStmt := stmts[i+1]

				// Try to match and apply optimization rules based on the next statement
				if nextStmt.Operation == parser.OpSlice {
					if opt.OptimizeSlice(stmt, nextStmt) {
						skipIndices[i+1] = true
					}
				} else if nextStmt.Operation == parser.OpStartFilter {
					if skipped := opt.OptimizeFilterSlice(stmts, i); skipped > 0 {
						skipIndices[i+skipped] = true
					}
				} else if nextStmt.Operation == parser.OpAggregateReduce {
					if skipped := opt.OptimizeSortSlice(stmts, i); skipped > 0 {
						for k := 1; k <= skipped; k++ {
							skipIndices[i+k] = true
						}
					}
				}
			}
		}
		newStatements = append(newStatements, stmt)
	}

	opt.Plan.Statements = newStatements
	return nil
}

// OptimizeSlice handles the pattern: AccessTable -> Slice
// This optimization merges the Slice operation into the AccessTable operation
// by pushing down the offset and limit.
//
// Pattern: TABLE[Start:End]
// Action:  Set TABLE.Meta["offset"] = Start, TABLE.Meta["limit"] = End-Start
// Returns: true if optimization was applied
func (opt *Optimizer) OptimizeSlice(stmt *parser.Statement, sliceStmt *parser.Statement) bool {
	offset, limit, ok := parseSliceExpression(sliceStmt.Expressions)
	if ok {
		applyOptimization(stmt, offset, limit)
		return true
	}
	return false
}

// OptimizeFilterSlice handles the pattern: AccessTable -> Filter -> Slice
// This optimization allows pushing down the Limit/Offset to the Table Access even when filtered.
// NOTE: This assumes the storage engine/evaluator can handle "limit matching items",
// i.e., it scans until it finds 'Limit' items that satisfy the filter.
//
// Pattern: TABLE[Filter][Start:End]
// Action:  Set TABLE.Meta["offset"] = Start, TABLE.Meta["limit"] = End-Start
// Returns: index of the Slice statement relative to AccessTable (to be skipped), or 0 if failed.
func (opt *Optimizer) OptimizeFilterSlice(stmts []*parser.Statement, index int) int {
	nesting := 0
	endFilterIdx := -1
	for j := index + 1; j < len(stmts); j++ {
		if stmts[j].Operation == parser.OpStartFilter {
			nesting++
		} else if stmts[j].Operation == parser.OpEndFilter {
			nesting--
			if nesting == 0 {
				endFilterIdx = j
				break
			}
		}
	}

	if endFilterIdx != -1 && endFilterIdx+1 < len(stmts) {
		sliceStmt := stmts[endFilterIdx+1]
		if sliceStmt.Operation == parser.OpSlice {
			stmt := stmts[index]
			offset, limit, ok := parseSliceExpression(sliceStmt.Expressions)
			if ok {
				applyOptimization(stmt, offset, limit)
				return endFilterIdx + 1 - index // Return relative index of Slice
			}
		}
	}
	return 0
}

// OptimizeSortSlice handles the pattern: AccessTable -> Sort -> Slice
// This optimization utilizes the database index to retrieve data in sorted order,
// avoiding loading all data into memory for sorting.
//
// Pattern: TABLE._desc(Col)[Start:End]
// Action:  Set TABLE.Meta["sort_col"] = Col, TABLE.Meta["sort_dir"] = _desc
//
//	Set TABLE.Meta["offset"] = Start, TABLE.Meta["limit"] = End-Start
//
// Returns: number of statements to skip (Scan + Sort + Slice = skip 2 next stmts)
func (opt *Optimizer) OptimizeSortSlice(stmts []*parser.Statement, index int) int {
	// check if next is Sort
	sortStmt := stmts[index+1]
	aggr, ok := sortStmt.Expressions.(parser.Aggr)
	if ok && (aggr.Name == "_asc" || aggr.Name == "_desc") {
		// Found Sort operation
		if index+2 < len(stmts) {
			sliceStmt := stmts[index+2]
			if sliceStmt.Operation == parser.OpSlice {
				stmt := stmts[index]
				offset, limit, okSlice := parseSliceExpression(sliceStmt.Expressions)
				if okSlice {
					// Apply Sort and Slice optimization
					if stmt.Meta == nil {
						stmt.Meta = make(map[string]string)
					}
					stmt.Meta["sort_col"] = aggr.Args[0]
					stmt.Meta["sort_dir"] = aggr.Name
					applyOptimization(stmt, offset, limit)

					return 2 // Skip Sort (idx+1) and Slice (idx+2)
				}
			}
		}
	}
	return 0
}

func parseSliceExpression(expr any) (offset int64, limit int64, ok bool) {
	sliceStr, isStr := expr.(string)
	if !isStr {
		// Try generic slice of expressions if parser changes back
		if exprs, okList := expr.([]parser.Expression); okList && len(exprs) >= 2 {
			// Legacy support or if parser assumption was right initially
			// ... skipping for now as string seems to be the one
			fmt.Printf("Optimizer: Expressions type mismatch. Got %T\n", expr)
			return 0, 0, false
		}
		// fmt.Printf("Optimizer: Expressions type mismatch. Got %T\n", expr)
		return 0, 0, false
	}

	parts := strings.Split(sliceStr, ":")
	if len(parts) >= 1 {
		// Parse Start (Offset)
		if parts[0] != "" {
			var err error
			offset, err = strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
			if err != nil {
				return 0, 0, false
			}
		} else {
			offset = 0 // Default start
		}

		ok = true

		// Parse End (Limit)
		if len(parts) >= 2 && parts[1] != "" {
			end, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err == nil {
				if end > offset {
					limit = end - offset
				} else {
					// Invalid range
					limit = 0
					ok = false
				}
			} else {
				ok = false
			}
		} else {
			limit = 0 // Meaning all
		}
	} else {
		ok = false
	}
	return
}

func applyOptimization(stmt *parser.Statement, offset, limit int64) {
	if stmt.Meta == nil {
		stmt.Meta = make(map[string]string)
	}
	stmt.Meta["offset"] = strconv.FormatInt(offset, 10)
	stmt.Meta["limit"] = strconv.FormatInt(limit, 10)
}
