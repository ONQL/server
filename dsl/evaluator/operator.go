package evaluator

import (
	"errors"
	"fmt"
	"math"
	"onql/dsl/parser"
	"strconv"
	"strings"
)

func (e *Evaluator) EvalOperator() error {
	// Implementation for evaluating field access
	stmt := e.Plan.NextStatement(false)
	if stmt.Operation != parser.OpNormalOperation {
		return errors.New("expect operator")
	}
	expression := strings.Split(stmt.Expressions.(string), " ")
	switch strings.ToLower(expression[1]) {
	case "+", "-", "*", "/", "%", "**":
		return e.EvalArithmeticOperator()
	case "=", "!=", "<", "<=", ">", ">=", "in":
		return e.EvalComparisonOperator()
	case "and", "or", "not":
		return e.EvalLogicalOperator()
	default:
		return fmt.Errorf("unknown operator '%s'", expression[1])
	}
}

func (e *Evaluator) EvalArithmeticOperator() error {
	// Implementation for evaluating arithmetic operations
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpNormalOperation {
		return errors.New("expect arithmetic operator")
	}

	op1Str := ""
	op2Str := ""
	op1Num := 0.0
	op2Num := 0.0

	expression := strings.Split(stmt.Expressions.(string), " ")
	//set left operand
	if stmt.Meta["left_type"] == "var" {
		leftStmtData := e.Memory[e.Plan.StatementMap[expression[0]].Name]
		switch strings.ToUpper(e.Memory[e.Plan.StatementMap[expression[0]].Name+"_meta_type"].(string)) {
		case "STRING":
			op1Str = leftStmtData.(string)
		case "NUMBER", "TIMESTAMP":
			op1Num = leftStmtData.(float64)
		default:
			return fmt.Errorf("invalid data type '%s' on left operand, expected 'NUMBER' or 'TIMESTAMP'", strings.ToUpper(e.Memory[e.Plan.StatementMap[expression[0]].Name+"_meta_type"].(string)))
		}
	} else {
		switch strings.ToUpper(stmt.Meta["left_type"]) {
		case "STRING":
			op1Str = expression[0]
		case "NUMBER", "TIMESTAMP":
			var err error
			op1Num, err = strconv.ParseFloat(expression[0], 64)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid data type '%s' on left operand, expected 'NUMBER' or 'TIMESTAMP'", strings.ToUpper(stmt.Meta["left_type"]))
		}
	}
	//set right operand
	if stmt.Meta["right_type"] == "var" {
		fmt.Println(e.Plan.StatementMap[expression[2]].Name)
		rightStmtData := e.Memory[e.Plan.StatementMap[expression[2]].Name]
		switch strings.ToUpper(e.Memory[e.Plan.StatementMap[expression[2]].Name+"_meta_type"].(string)) {
		case "STRING":
			op2Str = rightStmtData.(string)
		case "NUMBER", "TIMESTAMP":
			op2Num = rightStmtData.(float64)
		default:
			return fmt.Errorf("invalid data type '%s' on right operand, expected 'NUMBER' or 'TIMESTAMP'", strings.ToUpper(e.Memory[e.Plan.StatementMap[expression[2]].Name+"_meta_type"].(string)))
		}
	} else {
		// fmt.Println(stmt.Meta["right_type"])
		switch strings.ToUpper(stmt.Meta["right_type"]) {
		case "STRING":
			op2Str = expression[2]
		case "NUMBER", "TIMESTAMP":
			var err error
			op2Num, err = strconv.ParseFloat(expression[2], 64)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid data type '%s' on right operand, expected 'NUMBER' or 'TIMESTAMP'", strings.ToUpper(stmt.Meta["right_type"]))
		}
	}

	var resultStr string
	var resultNum float64

	if op1Str != "" {
		switch expression[1] {
		case "+":
			// Handle addition
			resultStr = op1Str + op2Str
		default:
			return fmt.Errorf("expected '+' operation on string, got '%s'", expression[1])
		}
	} else {
		switch expression[1] {
		case "+":
			// Handle equality
			resultNum = op1Num + op2Num
		case "-":
			// Handle inequality
			resultNum = op1Num - op2Num
		case "*":
			// Handle less than
			resultNum = op1Num * op2Num
		case "/":
			// Handle less than or equal
			resultNum = op1Num / op2Num
		case "**":
			// Handle exponentiation
			resultNum = math.Pow(op1Num, op2Num)
		case "%":
			// Handle modulus
			resultNum = math.Mod(op1Num, op2Num)
		default:
			return fmt.Errorf("expected arithmetic operation, got '%s'", expression[1])
		}
	}
	// fmt.Println(op1Str, op2Str)
	if resultStr != "" {
		// e.Memory[stmt.Name] = resultStr
		e.SetMemoryValue(stmt.Name, resultStr)
	} else {
		e.SetMemoryValue(stmt.Name, resultNum)
	}
	// e.Memory[stmt.Name+"_meta_type"] = strings.ToUpper(e.Memory[e.Plan.StatementMap[expression[0]].Name+"_meta_type"].(string))
	return nil
}

func (e *Evaluator) EvalComparisonOperator() error {
	// Implementation for evaluating comparison operations
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpNormalOperation {
		return errors.New("expect comparison operator")
	}
	operationOn := "NUMBER"
	op1Str := ""
	op2Str := ""
	op1Num := 0.0
	op2Num := 0.0
	op2StrList := []string{}
	op2NumList := []float64{}

	expression := strings.Split(stmt.Expressions.(string), " ")
	//set left operand
	if stmt.Meta["left_type"] == "var" {
		leftStmtData := e.Memory[e.Plan.StatementMap[expression[0]].Name]
		switch strings.ToUpper(e.Memory[e.Plan.StatementMap[expression[0]].Name+"_meta_type"].(string)) {
		case "STRING":
			op1Str = leftStmtData.(string)
		case "NUMBER", "TIMESTAMP":
			op1Num = leftStmtData.(float64)
		default:
			if e.Plan.StatementMap[expression[0]].Operation == parser.OpAccessJsonProperty || e.Plan.StatementMap[expression[0]].Operation == parser.OpUnknownIdentifier {
				e.Memory[stmt.Name] = false
				return nil
			}
			return fmt.Errorf("invalid data type '%s' on left operand of comparison, expected 'NUMBER' or 'TIMESTAMP'", strings.ToUpper(e.Memory[e.Plan.StatementMap[expression[0]].Name+"_meta_type"].(string)))
		}
		operationOn = strings.ToUpper(e.Memory[e.Plan.StatementMap[expression[0]].Name+"_meta_type"].(string))
	} else {
		switch strings.ToUpper(stmt.Meta["left_type"]) {
		case "STRING":
			op1Str = expression[0]
		case "NUMBER", "TIMESTAMP":
			var err error
			op1Num, err = strconv.ParseFloat(expression[0], 64)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid data type '%s' on left operand of comparison, expected 'NUMBER' or 'TIMESTAMP'", strings.ToUpper(stmt.Meta["left_type"]))
		}
		operationOn = strings.ToUpper(stmt.Meta["left_type"])
	}
	//set right operand
	if stmt.Meta["right_type"] == "var" {
		fmt.Println(e.Memory[e.Plan.StatementMap[expression[2]].Name+"_meta_type"])
		rightStmtData := e.Memory[e.Plan.StatementMap[expression[2]].Name]
		switch strings.ToUpper(e.Memory[e.Plan.StatementMap[expression[2]].Name+"_meta_type"].(string)) {
		case "STRING":
			op2Str = rightStmtData.(string)
		case "NUMBER", "TIMESTAMP":
			op2Num = rightStmtData.(float64)
		case "ARRAY_OF_STRING":
			for _, v := range rightStmtData.([]string) {
				op2StrList = append(op2StrList, v)
			}
		case "ARRAY_OF_NUMBER":
			for _, v := range rightStmtData.([]float64) {
				op2NumList = append(op2NumList, v)
			}
		default:
			if e.Plan.StatementMap[expression[2]].Operation == parser.OpAccessJsonProperty || e.Plan.StatementMap[expression[2]].Operation == parser.OpUnknownIdentifier {
				e.Memory[stmt.Name] = false
				return nil
			}
			return fmt.Errorf("invalid data type '%s' on right operand of comparison, expected 'NUMBER' or 'TIMESTAMP'", strings.ToUpper(e.Memory[e.Plan.StatementMap[expression[2]].Name+"_meta_type"].(string)))
		}
	} else {
		// fmt.Println(stmt.Meta["right_type"])
		switch strings.ToUpper(stmt.Meta["right_type"]) {
		case "STRING":
			op2Str = expression[2]
		case "NUMBER", "TIMESTAMP":
			var err error
			op2Num, err = strconv.ParseFloat(expression[2], 64)
			if err != nil {
				return err
			}
		// case "ARRAY"
		default:

			return fmt.Errorf("invalid data type '%s' on right operand of comparison, expected 'NUMBER' or 'TIMESTAMP'", strings.ToUpper(stmt.Meta["right_type"]))
		}
	}

	var result bool

	if operationOn == "STRING" {
		switch expression[1] {
		case "=":
			// Handle equality
			result = op1Str == op2Str
		case "!=":
			// Handle inequality
			result = op1Str != op2Str
		case "<":
			// Handle less than
			result = op1Str < op2Str
		case "<=":
			// Handle less than or equal
			result = op1Str <= op2Str
		case ">":
			// Handle greater than
			result = op1Str > op2Str
		case ">=":
			// Handle greater than or equal
			result = op1Str >= op2Str
		case "in":
			for _, v := range op2StrList {
				if op1Str == v {
					result = true
					break
				}
			}
		default:
			return fmt.Errorf("expected comparison operation on string, got '%s'", expression[1])
		}
	} else {
		switch expression[1] {
		case "=":
			// Handle equality
			result = op1Num == op2Num
		case "!=":
			// Handle inequality
			result = op1Num != op2Num
		case "<":
			// Handle less than
			result = op1Num < op2Num
		case "<=":
			// Handle less than or equal
			result = op1Num <= op2Num
		case ">":
			// Handle greater than
			result = op1Num > op2Num
		case ">=":
			// Handle greater than or equal
			result = op1Num >= op2Num
		case "in":
			for _, v := range op2NumList {
				if op1Num == v {
					result = true
					break
				}
			}
		default:
			return fmt.Errorf("expected comparison operation on number, got '%s'", expression[1])
		}
	}
	// fmt.Println(op1Str, op2Str)
	e.SetMemoryValue(stmt.Name, result)
	return nil
}

func (e *Evaluator) EvalLogicalOperator() error {
	// Implementation for evaluating logical operations
	stmt := e.Plan.NextStatement(true)
	if stmt.Operation != parser.OpNormalOperation {
		return errors.New("expect logical operator")
	}

	expression := strings.Split(stmt.Expressions.(string), " ")
	var op1 bool
	if expression[1] != "!" && expression[1] != "not" {
		op1 = e.Memory[expression[0]].(bool)
	}
	op2 := e.Memory[expression[2]].(bool)
	var result bool

	switch strings.ToLower(expression[1]) {
	case "and":
		// Handle logical AND
		result = op1 && op2
	case "or":
		// Handle logical OR
		result = op1 || op2
	case "not":
		result = !op2
	default:
		return errors.New("expect logical operation")
	}
	// e.Memory[stmt.Name] = result
	e.SetMemoryValue(stmt.Name, result)
	return nil
}

// NOP always like C != 3 or C != "2"

// func ConvertDataType(variable *any, targetType string) error {
// 	switch targetType {
// 	case "number":
// 		switch v := (*variable).(type) {
// 		case string:
// 			// Try to convert string to number
// 			if num, err := strconv.ParseFloat(v, 64); err == nil {
// 				*variable = num
// 			} else {
// 				return err
// 			}
// 		}
// 	case "string":
// 		switch v := (*variable).(type) {
// 		case float64:
// 			*variable = fmt.Sprintf("%f", v)
// 		}
// 	}
// 	return nil
// }

// func (e *Evaluator) GetOperatorExpressionData(stmt *parser.Statement, variable *any) {
// 	expression := strings.Split(stmt.Expressions.(string), " ")
// 	if expression[0][0] == '$' {
// 		varName := expression[0][1:]
// 		if val, ok := e.Memory[varName]; ok {
// 			*variable = val
// 		}
// 	}
// }
