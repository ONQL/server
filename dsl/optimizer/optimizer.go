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
	stmts := opt.Plan.Statements

	for i := 0; i < len(stmts); i++ {
		stmt := stmts[i]

		// We currently only optimize Table Access patterns
		if stmt.Operation == parser.OpAccessTable {
			if i+1 < len(stmts) {
				nextStmt := stmts[i+1]

				// Try to match and apply optimization rules based on the next statement
				if nextStmt.Operation == parser.OpSlice {
					if opt.OptimizeSlice(stmt, nextStmt) {
						markOptimized(nextStmt)
					}
				} else if nextStmt.Operation == parser.OpStartFilter {
					// Check for Filter -> Slice
					if skipped := opt.OptimizeFilterSlice(stmts, i); skipped > 0 {
						markOptimized(stmts[i+skipped])
					} else if skippedIndices := opt.OptimizeFilterSortSlice(stmts, i); len(skippedIndices) > 0 {
						// Check for Filter -> Sort -> Slice
						for _, idx := range skippedIndices {
							markOptimized(stmts[i+idx])
						}
					}
				} else if nextStmt.Operation == parser.OpAggregateReduce {
					if skipped := opt.OptimizeSortSlice(stmts, i); skipped > 0 {
						for k := 1; k <= skipped; k++ {
							markOptimized(stmts[i+k])
						}
					}
				}
			}
		}
	}

	return nil
}

func markOptimized(stmt *parser.Statement) {
	if stmt.Meta == nil {
		stmt.Meta = make(map[string]string)
	}
	stmt.Meta["optimized"] = "true"
}

// OptimizeSlice handles the pattern: AccessTable -> Slice
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
// Returns: relative index of Slice statement to skip (0 if failed)
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
// Returns: number of statements to skip (2 statements: Sort + Slice)
func (opt *Optimizer) OptimizeSortSlice(stmts []*parser.Statement, index int) int {
	sortStmt := stmts[index+1]
	aggr, ok := sortStmt.Expressions.(parser.Aggr)
	if ok && (aggr.Name == "_asc" || aggr.Name == "_desc") {
		if index+2 < len(stmts) {
			sliceStmt := stmts[index+2]
			if sliceStmt.Operation == parser.OpSlice {
				stmt := stmts[index]
				offset, limit, okSlice := parseSliceExpression(sliceStmt.Expressions)
				if okSlice {
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

// OptimizeFilterSortSlice handles the pattern: AccessTable -> Filter -> Sort -> Slice
// Returns: list of relative indices to skip (Sort + Slice)
func (opt *Optimizer) OptimizeFilterSortSlice(stmts []*parser.Statement, index int) []int {
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
		// Check for Sort after Filter
		sortStmt := stmts[endFilterIdx+1]
		if sortStmt.Operation == parser.OpAggregateReduce {
			aggr, ok := sortStmt.Expressions.(parser.Aggr)
			if ok && (aggr.Name == "_asc" || aggr.Name == "_desc") {
				// Check for Slice after Sort
				if endFilterIdx+2 < len(stmts) {
					sliceStmt := stmts[endFilterIdx+2]
					if sliceStmt.Operation == parser.OpSlice {
						stmt := stmts[index]
						offset, limit, okSlice := parseSliceExpression(sliceStmt.Expressions)
						if okSlice {
							// Apply Optimization
							if stmt.Meta == nil {
								stmt.Meta = make(map[string]string)
							}
							stmt.Meta["sort_col"] = aggr.Args[0]
							stmt.Meta["sort_dir"] = aggr.Name
							applyOptimization(stmt, offset, limit)

							// Return indices of Sort and Slice
							// Sort is at endFilterIdx+1 -> rel: endFilterIdx + 1 - index
							// Slice is at endFilterIdx+2 -> rel: endFilterIdx + 2 - index
							sortRelIdx := endFilterIdx + 1 - index
							sliceRelIdx := endFilterIdx + 2 - index
							return []int{sortRelIdx, sliceRelIdx}
						}
					}
				}
			}
		}
	}
	return nil
}

func parseSliceExpression(expr any) (offset int64, limit int64, ok bool) {
	sliceStr, isStr := expr.(string)
	if !isStr {
		if exprs, okList := expr.([]parser.Expression); okList && len(exprs) >= 2 {
			fmt.Printf("Optimizer: Expressions type mismatch. Got %T\n", expr)
			return 0, 0, false
		}
		return 0, 0, false
	}

	parts := strings.Split(sliceStr, ":")
	if len(parts) >= 1 {
		if parts[0] != "" {
			var err error
			offset, err = strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
			if err != nil {
				return 0, 0, false
			}
		} else {
			offset = 0
		}

		ok = true

		if len(parts) >= 2 && parts[1] != "" {
			end, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err == nil {
				if end > offset {
					limit = end - offset
				} else {
					limit = 0
					ok = false
				}
			} else {
				ok = false
			}
		} else {
			limit = 0
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
