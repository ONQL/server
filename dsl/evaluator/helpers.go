package evaluator

import (
	"fmt"
	"onql/database"
	"onql/storemanager"
	"strings"
)

func GetTableData(db string, table string) ([]map[string]any, error) {
	pks, err := database.GetAllPks(db, table)
	if err != nil {
		return nil, err
	}
	// fmt.Println(db,table)
	data, err := database.GetWithPKs(db, table, pks)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// func GetTableWithDataWithFilters(db string, table string, filters []string) ([]map[string]any, error) {
// 	// loop filters and get by col value and at last union of pks then get with pks
// 	pks := make([]string, 0)
// 	for i, filter := range filters {
// 		if filter == "and" || filter == "or" {
// 			continue
// 		}
// 		cols := strings.Split(filter, ":")
// 		pk, err := get.GetPksFromIndex(db, table, cols[0]+":"+cols[1])
// 		if err != nil {
// 			return nil, err
// 		}
// 		// check is i and or or
// 		if i == 0 {
// 			pks = pk
// 			continue
// 		}
// 		// check if i-1 is and then union of pks then append otherwise direct appent
// 		if filters[i-1] == "and" {
// 			// union of pks and pk
// 			pks = get.Union(pks, pk)
// 		} else {
// 			pks = append(pks, pk...)
// 		}
// 	}

// 	data, err := get.GetWithPKs(db, table, pks)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return data, nil
// }

func GetTableWithDataWithFilters(db, table string, filters []string) ([]map[string]any, error) {
	if len(filters) == 0 {
		return GetTableData(db, table)
	}

	trim := func(s string) string { return strings.TrimSpace(s) }
	isOp := func(s string) bool {
		ls := strings.ToLower(trim(s))
		return ls == "and" || ls == "or"
	}

	// Stack of PK sets for pending expressions.
	stack := make([][]string, 0, len(filters))

	for i, tok := range filters {
		tok = trim(tok)
		if tok == "" {
			continue
		}

		if isOp(tok) {
			if len(stack) < 2 {
				return nil, fmt.Errorf("operator %q at index %d without two preceding expressions", tok, i)
			}
			right := stack[len(stack)-1]
			left := stack[len(stack)-2]
			stack = stack[:len(stack)-2]

			var merged []string
			if strings.ToLower(tok) == "and" {
				merged = intersect(left, right) // AND = intersection
			} else {
				merged = union(left, right) // OR = union
			}
			stack = append(stack, dedupe(merged))
			continue
		}

		// Expression token: "col:val"
		parts := strings.SplitN(tok, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("bad filter token %q at index %d; expected 'col:val'", tok, i)
		}
		col := trim(parts[0])
		val := trim(parts[1])

		pk, err := database.GetPksFromIndex(db, table, col+":"+val)
		if err != nil {
			return nil, err
		}
		stack = append(stack, dedupe(pk))
	}

	// After consuming all tokens, we should have exactly one PK set.
	if len(stack) == 0 {
		return []map[string]any{}, nil
	}
	if len(stack) > 1 {
		return nil, fmt.Errorf("incomplete filter: leftover %d uncombined expressions (missing operator)", len(stack)-1)
	}

	pks := dedupe(stack[0])
	if len(pks) == 0 {
		return []map[string]any{}, nil
	}
	return database.GetWithPKs(db, table, pks)
}

func GetRelatedTableData(db string, relation storemanager.Relation, value string) ([]map[string]any, error) {
	//two probelems pending first original col name table name and db name not alias second mtm through table thirds in oto and mto case send dict not array
	if relation.Type == "mtm" {
		return GetMTMRelatedTabledData(db, relation, value)
	}
	cols := strings.Split(relation.FKField, ":")
	pks, err := database.GetPksFromIndex(db, relation.Entity, cols[1]+":"+value)
	if err != nil {
		return nil, err
	}
	data, err := database.GetWithPKs(db, relation.Entity, pks)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func GetMTMRelatedTabledData(db string, relation storemanager.Relation, value string) ([]map[string]any, error) {
	cols := strings.Split(relation.FKField, ":")
	pks, err := database.GetPksFromIndex(db, relation.Through, cols[1]+":"+value)
	if err != nil {
		return nil, err
	}
	data, err := database.GetWithPKs(db, relation.Through, pks)
	if err != nil {
		return nil, err
	}
	values := make([]string, 0)
	for _, item := range data {
		if val, ok := item[cols[2]]; ok {
			values = append(values, val.(string))
		}
	}
	return GetTableDataWithColValues(db, relation.Entity, cols[3], values)
}

func GetTableDataWithColValues(db string, table string, col string, values []string) ([]map[string]any, error) {
	pksOuter := make([]string, 0)
	for _, value := range values {
		pks, err := database.GetPksFromIndex(db, table, col+":"+value)
		if err != nil {
			return nil, err
		}
		pksOuter = append(pksOuter, pks...)
	}
	data, err := database.GetWithPKs(db, table, pksOuter)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// func (e *Evaluator) GetDataFromVar(varName string) (any, error) {
// 	value, ok := e.Memory[varName]
// 	if !ok {
// 		return nil, fmt.Errorf("variable not found: %s", varName)
// 	}
// 	return value, nil
// }

// ---------------------- set helpers ----------------------

func union(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for _, x := range a {
		seen[x] = struct{}{}
	}
	for _, x := range b {
		seen[x] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out
}

func intersect(a, b []string) []string {
	if len(a) == 0 || len(b) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(a))
	for _, x := range a {
		seen[x] = struct{}{}
	}
	out := make([]string, 0)
	for _, x := range b {
		if _, ok := seen[x]; ok {
			out = append(out, x)
		}
	}
	// optional dedupe if b had dups (rare)
	if len(out) > 1 {
		tmp := make(map[string]struct{}, len(out))
		uniq := out[:0]
		for _, x := range out {
			if _, ok := tmp[x]; !ok {
				tmp[x] = struct{}{}
				uniq = append(uniq, x)
			}
		}
		out = uniq
	}
	return out
}

// dedupe keeps order roughly arbitrary; if you need stable sort, sort.Strings after
func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, x := range in {
		if _, ok := seen[x]; !ok {
			seen[x] = struct{}{}
			out = append(out, x)
		}
	}
	return out
}
