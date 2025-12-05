# Message Protocol Specification

## Overview
The RDBMS uses a delimiter-based message protocol that supports **parallel requests** over a single TCP connection.

## Message Format

### Request Format
```
RID\x1Etarget\x1Edata\x04
```

**Components:**
- `RID` - Request ID (for matching responses)
- `\x1E` - Record Separator (field delimiter)
- `target` - Target handler (database, onql, protocol, schema)
- `\x1E` - Record Separator
- `data` - Payload (JSON or command array)
- `\x04` - End of Transmission (message terminator)

### Response Format
```
RID\x1Eresponse_data\x04
```

**Components:**
- `RID` - Request ID (matches request)
- `\x1E` - Record Separator
- `response_data` - Response payload (JSON)
- `\x04` - End of Transmission

## Delimiters

| Character | Hex    | Name                | Purpose                    |
|-----------|--------|---------------------|----------------------------|
| `\x1E`    | 0x1E   | Record Separator    | Separates fields in message|
| `\x04`    | 0x04   | End of Transmission | Marks end of message       |

## Parallel Requests

The protocol supports **multiple concurrent requests** over a single connection:

1. Client sends multiple requests with different RIDs
2. Server processes each request in parallel (separate goroutines)
3. Responses include RID for client to match with requests
4. Order of responses is **not guaranteed**

## Examples

### Example 1: Database Query
**Request:**
```
req001\x1Edatabase\x1E{"function":"GetDatabases","args":[]}\x04
```

**Response:**
```
req001\x1E["testdb","mydb"]\x04
```

### Example 2: DSL Query
**Request:**
```
req002\x1Eonql\x1E{"protopass":"myproto","query":"db.users[age>25]","ctxkey":"","ctxvalues":[]}\x04
```

**Response:**
```
req002\x1E{"data":[...],"error":""}\x04
```

### Example 3: Schema Command
**Request:**
```
req003\x1Eschema\x1E["desc","mydb"]\x04
```

**Response:**
```
req003\x1E["users","products","orders"]\x04
```

### Example 4: Parallel Requests
**Client sends (rapid fire):**
```
req001\x1Edatabase\x1E{"function":"GetDatabases","args":[]}\x04
req002\x1Eschema\x1E["desc"]\x04
req003\x1Eonql\x1E{"protopass":"p1","query":"db.users","ctxkey":"","ctxvalues":[]}\x04
```

**Server responds (any order):**
```
req002\x1E["testdb","mydb"]\x04
req001\x1E["testdb","mydb"]\x04
req003\x1E{"data":[...],"error":""}\x04
```

## Benefits

1. **Parallel Processing** - Multiple requests processed simultaneously
2. **Request Matching** - RID allows client to match responses to requests
3. **Efficient** - Single connection for multiple operations
4. **Simple** - No complex framing, just delimiters
5. **Debuggable** - Human-readable when escaped

## Client Implementation Example

```go
type Client struct {
    conn      net.Conn
    responses map[string]chan string
    mu        sync.RWMutex
}

func (c *Client) Send(rid, target, data string) (string, error) {
    // Create response channel
    respChan := make(chan string, 1)
    c.mu.Lock()
    c.responses[rid] = respChan
    c.mu.Unlock()
    
    // Send request
    msg := fmt.Sprintf("%s\x1E%s\x1E%s\x04", rid, target, data)
    c.conn.Write([]byte(msg))
    
    // Wait for response
    response := <-respChan
    return response, nil
}

func (c *Client) readResponses() {
    reader := bufio.NewReader(c.conn)
    for {
        msg, _ := reader.ReadString('\x04')
        parts := strings.Split(msg, "\x1E")
        if len(parts) >= 2 {
            rid := parts[0]
            data := parts[1]
            
            c.mu.RLock()
            if ch, ok := c.responses[rid]; ok {
                ch <- data
            }
            c.mu.RUnlock()
        }
    }
}
```

## Error Handling

**Invalid Format:**
```
req001\x1Einvalid\x04
```
**Response:**
```
req001\x1E{"error":"invalid message format, expected: RID\x1Etarget\x1Edata"}\x04
```

## Migration from Old Format

**Old (JSON-based):**
```json
{"id":"client","target":"database","rid":"req1","payload":"...","type":"request"}\x04
```

**New (Delimiter-based):**
```
req1\x1Edatabase\x1E...\x04
```

**Advantages:**
- 70% smaller message size
- No JSON parsing overhead for routing
- Simpler client implementation
- Natural support for parallel requests
