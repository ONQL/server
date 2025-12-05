package evaluator

import (
	"fmt"
	"onql/dsl/parser"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// var AggrRegistry = map[string]map[string]fin{
// 	"_sum":      ,
// 	"_count":    {"LIST": "NUMBER"},
// 	"_avg":      {"LIST": "NUMBER"},
// 	"_min":      {"LIST": "NUMBER"},
// 	"_max":      {"LIST": "NUMBER"},
// 	"_distinct": {"LIST": "NUMBER"},
// }

var AggrRegistry = map[string]func(stmt *parser.Statement, data any, aggrObj parser.Aggr, e *Evaluator) error{
	// "_sum":  _sum,
	// "_asc":  _asc,
	// "_desc": _desc,

	"_sum":    _sum,
	"_count":  _count,
	"_avg":    _avg,
	"_min":    _min,
	"_max":    _max,
	"_unique": _unique,
	"_date":   _date,
	"_asc":    _asc,
	"_desc":   _desc,
	"_like":   _like,

	// "_date"
}

func (e *Evaluator) EvalAggr() error {
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpAggregateReduce {
		return fmt.Errorf("expect aggregate reduce but got %s", stmt.Operation)
	}
	// prevStmt := e.Plan.PrevStatement(false)
	data := e.Memory[stmt.Sources[0].SourceValue]

	aggrObj := stmt.Expressions.(parser.Aggr)
	err := AggrRegistry[aggrObj.Name](stmt, data, aggrObj, e)
	if err != nil {
		return err
	}
	// switch aggrObj.Name {
	// case "_sum":
	// 	return _sum(stmt, data, aggrObj, e)
	// }
	return nil
}

func _sum(stmt *parser.Statement, data any, aggrObj parser.Aggr, e *Evaluator) error {
	total := 0.0

	switch t := data.(type) {
	case []float64:
		for _, v := range t {
			total += v
		}
	case []map[string]any:
		if len(aggrObj.Args) == 0 {
			return fmt.Errorf("_sum: missing column name")
		}
		col := aggrObj.Args[0]
		for _, row := range t {
			if f, ok := asFloat64(row[col]); ok {
				total += f
			}
		}
	case []any:
		// Handle JSON data from unknown identifiers
		for _, item := range t {
			if f, ok := asFloat64(item); ok {
				total += f
			}
		}
	default:
		return fmt.Errorf("_sum: unsupported input %T", data)
	}

	e.SetMemoryValue(stmt.Name, total)
	return nil
}

// _asc orders data ASC. For TABLE, it sorts by args left→right: if the first key ties,
// it falls through to the next, like SQL ORDER BY col1, col2, ...
func _asc(stmt *parser.Statement, data any, aggrObj parser.Aggr, e *Evaluator) error {
	switch v := data.(type) {

	// ---------- TABLE ----------
	case []map[string]any:
		if len(aggrObj.Args) == 0 {
			return fmt.Errorf("sort: missing sort key(s)")
		}
		keys := make([]string, len(aggrObj.Args))
		for i, a := range aggrObj.Args {
			// s, ok := a.(string)
			if a == "" {
				return fmt.Errorf("sort: key %d must be non-empty string, got %T", i, a)
			}
			keys[i] = a
		}

		sort.SliceStable(v, func(i, j int) bool {
			ri, rj := v[i], v[j]
			for _, k := range keys {
				vi, vj := ri[k], rj[k]

				// nils last
				if vi == nil && vj == nil {
					continue
				}
				if vi == nil {
					return false
				}
				if vj == nil {
					return true
				}

				// numeric compare if both numeric, else string compare
				if fi, ok := asFloat64(vi); ok {
					if fj, ok := asFloat64(vj); ok {
						if fi < fj {
							return true
						}
						if fi > fj {
							return false
						}
						continue
					}
				}
				si := fmt.Sprint(vi)
				sj := fmt.Sprint(vj)
				if si < sj {
					return true
				}
				if si > sj {
					return false
				}
				// equal on this key → check next key
			}
			return false // completely equal
		})

		e.SetMemoryValue(stmt.Name, v)
		return nil

	// ---------- LIST (numbers) ----------
	case []float64:
		sort.Float64s(v)
		e.SetMemoryValue(stmt.Name, v)
		return nil

	// ---------- LIST (strings) ----------
	case []string:
		sort.Strings(v)
		e.SetMemoryValue(stmt.Name, v)
		return nil

	// ---------- LIST (interface) - JSON data ----------
	case []any:
		sort.SliceStable(v, func(i, j int) bool {
			vi, vj := v[i], v[j]

			// nils last
			if vi == nil && vj == nil {
				return false
			}
			if vi == nil {
				return false
			}
			if vj == nil {
				return true
			}

			// numeric compare if both numeric, else string compare
			if fi, ok := asFloat64(vi); ok {
				if fj, ok := asFloat64(vj); ok {
					return fi < fj
				}
			}
			si := fmt.Sprint(vi)
			sj := fmt.Sprint(vj)
			return si < sj
		})
		e.SetMemoryValue(stmt.Name, v)
		return nil

	default:
		return fmt.Errorf("sort: expected TABLE ([]map[string]any) or LIST ([]string/[]float64/[]any), got %T", data)
	}
}

// _sortDesc orders data in DESC order.
// TABLE: sorts by args left→right (col1 DESC, then col2 DESC, ...).
// LIST: sorts []float64, []string, or []any in descending order.
func _desc(stmt *parser.Statement, data any, aggrObj parser.Aggr, e *Evaluator) error {
	switch v := data.(type) {

	// ---------- TABLE ----------
	case []map[string]any:
		if len(aggrObj.Args) == 0 {
			return fmt.Errorf("sortDesc: missing sort key(s)")
		}
		keys := make([]string, len(aggrObj.Args))
		for i, a := range aggrObj.Args {
			// s, ok := a.(string)
			if a == "" {
				return fmt.Errorf("sortDesc: key %d must be non-empty string, got %T", i, a)
			}
			keys[i] = a
		}

		sort.SliceStable(v, func(i, j int) bool {
			ri, rj := v[i], v[j]
			for _, k := range keys {
				vi, vj := ri[k], rj[k]

				// nils last (same as ASC)
				if vi == nil && vj == nil {
					continue
				}
				if vi == nil {
					return false
				}
				if vj == nil {
					return true
				}

				// numeric compare if both numeric; else string compare
				if fi, ok := asFloat64(vi); ok {
					if fj, ok := asFloat64(vj); ok {
						if fi > fj { // DESC
							return true
						}
						if fi < fj {
							return false
						}
						continue
					}
				}
				si := fmt.Sprint(vi)
				sj := fmt.Sprint(vj)
				if si > sj { // DESC
					return true
				}
				if si < sj {
					return false
				}
				// equal on this key → check next key
			}
			return false // completely equal
		})

		e.SetMemoryValue(stmt.Name, v)
		return nil

	// ---------- LIST (numbers) ----------
	case []float64:
		sort.Sort(sort.Reverse(sort.Float64Slice(v)))
		e.SetMemoryValue(stmt.Name, v)
		return nil

	// ---------- LIST (strings) ----------
	case []string:
		sort.Sort(sort.Reverse(sort.StringSlice(v)))
		e.SetMemoryValue(stmt.Name, v)
		return nil

	// ---------- LIST (interface) - JSON data ----------
	case []any:
		sort.SliceStable(v, func(i, j int) bool {
			vi, vj := v[i], v[j]

			// nils last
			if vi == nil && vj == nil {
				return false
			}
			if vi == nil {
				return false
			}
			if vj == nil {
				return true
			}

			// numeric compare if both numeric, else string compare
			if fi, ok := asFloat64(vi); ok {
				if fj, ok := asFloat64(vj); ok {
					return fi > fj // DESC
				}
			}
			si := fmt.Sprint(vi)
			sj := fmt.Sprint(vj)
			return si > sj // DESC
		})
		e.SetMemoryValue(stmt.Name, v)
		return nil

	default:
		return fmt.Errorf("sortDesc: expected TABLE ([]map[string]any) or LIST ([]string/[]float64/[]any), got %T", data)
	}
}

func _count(stmt *parser.Statement, data any, aggrObj parser.Aggr, e *Evaluator) error {
	var n int
	switch v := data.(type) {
	case []float64:
		n = len(v)
	case []string:
		n = len(v)
	case []bool:
		n = len(v)
	case []map[string]any:
		// supports count on table rows as well
		n = len(v)
	case []any:
		// Handle JSON data from unknown identifiers
		n = len(v)
	default:
		return fmt.Errorf("_count: unsupported input %T", data)
	}
	e.SetMemoryValue(stmt.Name, float64(n))
	return nil
}

func _avg(stmt *parser.Statement, data any, aggrObj parser.Aggr, e *Evaluator) error {
	sum := 0.0
	cnt := 0.0
	switch t := data.(type) {
	case []float64:
		for _, v := range t {
			sum += v
			cnt++
		}
	case []map[string]any:
		if len(aggrObj.Args) == 0 {
			return fmt.Errorf("_avg: missing column name")
		}
		col := aggrObj.Args[0]
		for _, r := range t {
			if f, ok := asFloat64(r[col]); ok {
				sum += f
				cnt++
			}
		}
	case []any:
		// Handle JSON data from unknown identifiers
		for _, item := range t {
			if f, ok := asFloat64(item); ok {
				sum += f
				cnt++
			}
		}
	default:
		return fmt.Errorf("_avg: unsupported input %T", data)
	}
	if cnt == 0 {
		e.SetMemoryValue(stmt.Name, 0.0)
		return nil
	}
	e.SetMemoryValue(stmt.Name, sum/cnt)
	return nil
}

func _min(stmt *parser.Statement, data any, aggrObj parser.Aggr, e *Evaluator) error {
	minSet := false
	minVal := 0.0

	switch t := data.(type) {
	case []float64:
		for _, v := range t {
			if !minSet || v < minVal {
				minVal = v
				minSet = true
			}
		}
	case []map[string]any:
		if len(aggrObj.Args) == 0 {
			return fmt.Errorf("_min: missing column name")
		}
		col := aggrObj.Args[0]
		for _, r := range t {
			if f, ok := asFloat64(r[col]); ok {
				if !minSet || f < minVal {
					minVal = f
					minSet = true
				}
			}
		}
	case []any:
		// Handle JSON data from unknown identifiers
		for _, item := range t {
			if f, ok := asFloat64(item); ok {
				if !minSet || f < minVal {
					minVal = f
					minSet = true
				}
			}
		}
	default:
		return fmt.Errorf("_min: unsupported input %T", data)
	}

	if !minSet {
		return fmt.Errorf("_min: no numeric values found")
	}
	e.SetMemoryValue(stmt.Name, minVal)
	return nil
}

func _max(stmt *parser.Statement, data any, aggrObj parser.Aggr, e *Evaluator) error {
	maxSet := false
	maxVal := 0.0

	switch t := data.(type) {
	case []float64:
		for _, v := range t {
			if !maxSet || v > maxVal {
				maxVal = v
				maxSet = true
			}
		}
	case []map[string]any:
		if len(aggrObj.Args) == 0 {
			return fmt.Errorf("_max: missing column name")
		}
		col := aggrObj.Args[0]
		for _, r := range t {
			if f, ok := asFloat64(r[col]); ok {
				if !maxSet || f > maxVal {
					maxVal = f
					maxSet = true
				}
			}
		}
	case []any:
		// Handle JSON data from unknown identifiers
		for _, item := range t {
			if f, ok := asFloat64(item); ok {
				if !maxSet || f > maxVal {
					maxVal = f
					maxSet = true
				}
			}
		}
	default:
		return fmt.Errorf("_max: unsupported input %T", data)
	}

	if !maxSet {
		return fmt.Errorf("_max: no numeric values found")
	}
	e.SetMemoryValue(stmt.Name, maxVal)
	return nil
}

func _unique(stmt *parser.Statement, data any, aggrObj parser.Aggr, e *Evaluator) error {
	switch t := data.(type) {

	// ---------- LIST (strings) ----------
	case []string:
		seen := make(map[string]struct{}, len(t))
		out := make([]string, 0, len(t))
		for _, s := range t {
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, s)
		}
		e.SetMemoryValue(stmt.Name, out)
		return nil

	// ---------- LIST (numbers) ----------
	case []float64:
		seen := make(map[float64]struct{}, len(t))
		out := make([]float64, 0, len(t))
		for _, f := range t {
			if _, ok := seen[f]; ok {
				continue
			}
			seen[f] = struct{}{}
			out = append(out, f)
		}
		e.SetMemoryValue(stmt.Name, out)
		return nil

	// ---------- LIST (interface) - JSON data ----------
	case []any:
		seen := make(map[string]struct{}, len(t))
		out := make([]any, 0, len(t))
		for _, item := range t {
			// Use string representation as key for deduplication
			key := fmt.Sprint(item)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, item)
		}
		e.SetMemoryValue(stmt.Name, out)
		return nil

	// ---------- TABLE ----------
	case []map[string]any:
		if len(aggrObj.Args) == 0 {
			return fmt.Errorf("_distinct: missing column name(s)")
		}
		cols := make([]string, len(aggrObj.Args))
		for i, a := range aggrObj.Args {
			if a == "" {
				return fmt.Errorf("_distinct: key %d must be non-empty string", i)
			}
			cols[i] = a
		}

		seen := make(map[string]struct{}, len(t))
		out := make([]map[string]any, 0, len(t))

		for _, r := range t {
			key := makeCompositeKey(r, cols)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			// keep the FULL row (all columns), not just the key columns
			out = append(out, r)
		}

		e.SetMemoryValue(stmt.Name, out)
		return nil

	default:
		return fmt.Errorf("_distinct: expected LIST ([]string/[]float64/[]any) or TABLE ([]map[string]any), got %T", data)
	}
}

// helper: composite key builder for DISTINCT over multiple columns (left→right)
func makeCompositeKey(row map[string]any, cols []string) string {
	var b strings.Builder
	const sep = '\x1f' // unit separator
	for i, c := range cols {
		if i > 0 {
			b.WriteByte(byte(sep))
		}
		v := row[c]
		switch {
		case v == nil:
			b.WriteString("N:")
		default:
			if f, ok := asFloat64(v); ok {
				b.WriteString("F:")
				b.WriteString(strconv.FormatFloat(f, 'g', -1, 64))
			} else {
				b.WriteString("S:")
				b.WriteString(fmt.Sprint(v))
			}
		}
	}
	return b.String()
}

func _date(stmt *parser.Statement, data any, aggrObj parser.Aggr, e *Evaluator) error {
	// Default layout; override via args.
	// TABLE input: args[0]=column (required), args[1]=layout (optional)
	// Others (LIST/NUMBER/FIELD): args[0]=layout (optional)
	layout := "2006-01-02 15:04:05"
	var col string

	switch data.(type) {
	case []map[string]any: // TABLE
		if len(aggrObj.Args) == 0 || aggrObj.Args[0] == "" {
			return fmt.Errorf("_date: table input requires column name as first arg")
		}
		col = aggrObj.Args[0]
		if len(aggrObj.Args) > 1 && aggrObj.Args[1] != "" {
			layout = aggrObj.Args[1]
		}
	default:
		if len(aggrObj.Args) > 0 && aggrObj.Args[0] != "" {
			layout = aggrObj.Args[0]
		}
	}

	var sec int64
	found := false

	// Heuristic: >1e12 → milliseconds
	asSec := func(f float64) int64 {
		if f > 1e12 {
			return int64(f / 1000)
		}
		return int64(f)
	}

	switch t := data.(type) {

	// ----- literal numeric / string -----
	case float64:
		sec, found = asSec(t), true
	case float32:
		sec, found = asSec(float64(t)), true
	case int:
		sec, found = asSec(float64(t)), true
	case int64:
		sec, found = asSec(float64(t)), true
	case int32:
		sec, found = asSec(float64(t)), true
	case uint, uint32, uint64:
		sec, found = asSec(float64(fmt.Sprint(t)[0])), true // will be overridden below
		// handle uints precisely:
		switch v := data.(type) {
		case uint:
			sec = asSec(float64(v))
		case uint32:
			sec = asSec(float64(v))
		case uint64:
			sec = asSec(float64(v))
		}
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(t), 64); err == nil {
			sec, found = asSec(f), true
		} else {
			return fmt.Errorf("_date: string literal not numeric: %q", t)
		}

	// ----- list shapes -----
	case []float64:
		if len(t) == 0 {
			return fmt.Errorf("_date: empty []float64")
		}
		sec, found = asSec(t[0]), true

	case []string:
		if len(t) == 0 {
			return fmt.Errorf("_date: empty []string")
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(t[0]), 64); err == nil {
			sec, found = asSec(f), true
		} else {
			return fmt.Errorf("_date: first string not numeric: %q", t[0])
		}

	case []any:
		for _, v := range t {
			switch vv := v.(type) {
			case float64:
				sec, found = asSec(vv), true
			case float32:
				sec, found = asSec(float64(vv)), true
			case int:
				sec, found = asSec(float64(vv)), true
			case int64:
				sec, found = asSec(float64(vv)), true
			case int32:
				sec, found = asSec(float64(vv)), true
			case uint:
				sec, found = asSec(float64(vv)), true
			case uint32:
				sec, found = asSec(float64(vv)), true
			case uint64:
				sec, found = asSec(float64(vv)), true
			case string:
				if f, err := strconv.ParseFloat(strings.TrimSpace(vv), 64); err == nil {
					sec, found = asSec(f), true
				}
			}
			if found {
				break
			}
		}
		if !found {
			return fmt.Errorf("_date: no convertible element in []any")
		}

	// ----- table shape -----
	case []map[string]any:
		for _, r := range t {
			if v, ok := r[col]; ok {
				switch vv := v.(type) {
				case float64:
					sec, found = asSec(vv), true
				case float32:
					sec, found = asSec(float64(vv)), true
				case int:
					sec, found = asSec(float64(vv)), true
				case int64:
					sec, found = asSec(float64(vv)), true
				case int32:
					sec, found = asSec(float64(vv)), true
				case uint:
					sec, found = asSec(float64(vv)), true
				case uint32:
					sec, found = asSec(float64(vv)), true
				case uint64:
					sec, found = asSec(float64(vv)), true
				case string:
					if f, err := strconv.ParseFloat(strings.TrimSpace(vv), 64); err == nil {
						sec, found = asSec(f), true
					}
				}
			}
			if found {
				break
			}
		}
		if !found {
			return fmt.Errorf("_date: no convertible value found in column %q", col)
		}

	default:
		return fmt.Errorf("_date: unsupported input %T", data)
	}

	dt := time.Unix(sec, 0).UTC()
	e.SetMemoryValue(stmt.Name, dt.Format(layout))
	return nil
}

func _like(stmt *parser.Statement, data any, aggrObj parser.Aggr, e *Evaluator) error {
	if len(aggrObj.Args) == 0 {
		return fmt.Errorf("_like: missing pattern argument")
	}

	var pattern string
	var columnName string

	inputType := stmt.Meta["input_type"]

	if inputType == "TABLE" {
		if len(aggrObj.Args) < 2 {
			return fmt.Errorf("_like: table input requires a column name and a pattern")
		}
		columnName = aggrObj.Args[0]
		pattern = aggrObj.Args[1]
	} else {
		pattern = aggrObj.Args[0]
	}

	// Convert SQL LIKE to regex
	regexPatternStr := strings.ReplaceAll(pattern, "%", ".*")
	regexPatternStr = strings.ReplaceAll(regexPatternStr, "_", ".")
	regexPattern, err := regexp.Compile("^" + regexPatternStr + "$")
	if err != nil {
		return fmt.Errorf("_like: invalid pattern: %w", err)
	}

	found := false

	switch values := data.(type) {
	case string: // This would be a FIELD
		found = regexPattern.MatchString(values)
	case []string: // This would be a LIST of strings
		for _, s := range values {
			if regexPattern.MatchString(s) {
				found = true
				break
			}
		}
	case []any:
		for _, item := range values {
			if s, ok := item.(string); ok {
				if regexPattern.MatchString(s) {
					found = true
					break
				}
			}
		}
	case []map[string]any: // This is a TABLE
		for _, row := range values {
			if val, ok := row[columnName]; ok {
				if s, ok := val.(string); ok {
					if regexPattern.MatchString(s) {
						found = true
						break
					}
				}
			}
		}
	default:
		return fmt.Errorf("_like: unsupported input type %T", data)
	}

	result := false
	if found {
		result = true
	}

	e.SetMemoryValue(stmt.Name, result)
	return nil
}
