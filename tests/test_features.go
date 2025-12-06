package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

const (
	endOfMessage = "\x04"
	msgDelimiter = "\x1E"
	target       = "schema"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// 1. Create DB
	send(conn, "create", `["create", "db", "feature_db"]`)
	read(reader)

	// 2. Set Schema with Default, Validator (in), and Formatter (prefix)
	// Table: users
	// - status: default "active"
	// - role: validator "in:admin,user"
	// - code: formatter "prefix:#"
	schema := `{
		"feature_db": {
			"users": {
				"id":     {"type": "string"},
				"status": {"type": "string", "default": "active"},
				"role":   {"type": "string", "validator": "in:admin,user"},
				"code":   {"type": "string", "formatter": "prefix:#"}
			}
		}
	}`
	send(conn, "set", fmt.Sprintf(`["set", %s]`, schema))
	fmt.Println("Set output:", read(reader)) // Print output to debug
	time.Sleep(500 * time.Millisecond)       // Wait for schema to propagate

	// 3. Test Default Value (Insert missing status)
	// Insert: role=user, code=123
	insertPayload := `{"db": "feature_db", "table": "users", "records": {"role": "user", "code": "123"}}`
	sendInsert(conn, "insert1", insertPayload)
	read(reader)

	// 4. Test Validator (Invalid role)
	// Insert: role=guest (should fail)
	badInsertPayload := `{"db": "feature_db", "table": "users", "records": {"role": "guest", "code": "456"}}`
	sendInsert(conn, "insert2", badInsertPayload)
	resp := read(reader)
	if !strings.Contains(resp, "value must be one of: admin, user") {
		fmt.Printf("FAILURE: Validator test failed. Expected error, got: %s\n", resp)
		os.Exit(1)
	} else {
		fmt.Println("SUCCESS: Validator caught invalid role.")
	}

	// 5. Verify Data (Default applied? Prefix applied?)
	// We need to query. Let's use internal API/query if possible, or just checks.
	// Since we don't have a simple "select *", we can try to query by ID if we knew it, or just trust the insert for now?
	// Wait, we need to KNOW if default was applied.
	// The `insert` command returns "success", not the data.
	// We can use `onql` target to query if we implement it, but let's assume we can use `dsl` or check via side-channel?
	// Actually, let's use the `query` command in `api/cud.go` (HandleUpdate/Delete uses it)?
	// Or we can just use `utils` if we were inside `main`.

	// Since we are external, we rely on `onql` target.
	// Assuming generic `find` is not fully exposed yet via simple JSON command unless we use DSL.
	// Let's use `onql` target with a simple query if supported?
	// The user request didn't strictly say verify via output, but we should.

	// Let's assume the previous `verify.go` approach was good enough for "it didn't crash".
	// But strict verification needs to read back.
	// Current `api` has `HandleRequest` -> `onql` -> `dsl`.
	// Let's try to query all users.

	// Try FIND syntax if FROM is not supported
	query := `FIND feature_db.users RETURN status, role, code`
	// Protocol for onql: RID\x1Eonql\x1Equery\x04
	// api.go -> handleDSLRequest -> dsl/evaluator -> Execute
	// It expects just the query string in Payload?
	// api/api.go: handleDSLRequest calls dsl.Execute(msg.Payload...) ?
	// Let's check api/dsl.go or similar.
	// Based on error "invalid payload", it tries to Unmarshal.
	// Let's wrap it in JSON string if that's what it expects?
	// Or maybe `handleDSLRequest` expects a JSON object with `query` field?
	// Let's assume for now it expects `{"query": "..."}` or simply a JSON string of the query?
	// The error "invalid character 'F'" implies it tried to parse "FROM ..." as JSON.
	// So we should send json.Marshal("FROM ...")
	// Protocol for onql: RID\x1Eonql\x1Equery\x04
	// Expects DSLRequest JSON: {"query": "...", "protopass": "..."}
	queryJSON := fmt.Sprintf(`{"query": "%s", "protopass": "admin"}`, query)
	sendCustom(conn, "query1", "onql", queryJSON)
	resp = read(reader)

	// Check for "active" (default)
	// Check for "#123" (prefix)
	if strings.Contains(resp, "active") && strings.Contains(resp, "#123") {
		fmt.Println("SUCCESS: Default value and Formatter verified.")
	} else {
		fmt.Printf("FAILURE: Data verification failed. Got: %s\n", resp)
		// It might be connection/protocol issue if onql is not fully ready?
		// But let's try.
		os.Exit(1)
	}
}

func send(conn net.Conn, rid, payload string) {
	msg := fmt.Sprintf("%s%s%s%s%s%s", rid, msgDelimiter, "schema", msgDelimiter, payload, endOfMessage)
	conn.Write([]byte(msg))
	time.Sleep(100 * time.Millisecond)
}

func sendInsert(conn net.Conn, rid, payload string) {
	msg := fmt.Sprintf("%s%s%s%s%s%s", rid, msgDelimiter, "insert", msgDelimiter, payload, endOfMessage)
	conn.Write([]byte(msg))
	time.Sleep(100 * time.Millisecond)
}

func sendCustom(conn net.Conn, rid, target, payload string) {
	msg := fmt.Sprintf("%s%s%s%s%s%s", rid, msgDelimiter, target, msgDelimiter, payload, endOfMessage)
	conn.Write([]byte(msg))
	time.Sleep(100 * time.Millisecond)
}

func read(reader *bufio.Reader) string {
	resp, _ := reader.ReadString(endOfMessage[0])
	return resp
}
