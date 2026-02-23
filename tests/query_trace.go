package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"
)

const (
	eom = "\x04"
	sep = "\x1E"
)

func sendQuery(conn net.Conn, reader *bufio.Reader, rid, query, protoPass string) string {
	payload := fmt.Sprintf(`{"query": %q, "protopass": %q}`, query, protoPass)
	msg := rid + sep + "onql" + sep + payload + eom
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	conn.Write([]byte(msg))
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, _ := reader.ReadString(eom[0])
	return resp
}

func main() {
	conn, err := net.Dial("tcp", "localhost:5656")
	if err != nil {
		fmt.Println("Connect error:", err)
		os.Exit(1)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	queries := []struct {
		name  string
		query string
	}{
		// Test 1: original failing query (full)
		{"slice+projection", `fintrabit.trades._desc(time)[0:4]{"trading_name":instruments.trading_name[0],account_id,id,tid,time,type}`},
		// Test 2: empty table variant (trades table might be empty on dev)
		{"simple slice", `fintrabit.trades._desc(time)[0:4]{id,account_id}`},
		// Test 3: no slice just projection
		{"no slice", `fintrabit.trades{id,account_id}`},
	}

	for _, q := range queries {
		fmt.Printf("\n[%s]\nQuery: %s\n", q.name, q.query)
		resp := sendQuery(conn, reader, q.name, q.query, "admin")
		fmt.Println("Response:", resp)
	}
}
