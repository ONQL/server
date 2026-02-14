# Go RDBMS with BadgerDB

A high-performance, embedded **message-based RDBMS** written in Go, powered by [BadgerDB](https://github.com/dgraph-io/badger) with a complete TCP server, query language (DSL), and protocol system for entity mapping.

## 🚀 Key Features

### Core Database
*   **3-Layer Architecture**: Clean separation between Engine, Store Manager, and Database layers
*   **Hybrid Storage**: RAM buffering with asynchronous disk persistence (500ms flush)
*   **Full Indexing**: Automatic reverse indexing for every column
*   **Laravel-style Validation & Formatting**: Schema-level rules (e.g., `required|min:18`, `trim|upper`)
*   **Production Ready**: Structured logging, graceful shutdown, configuration via environment

### Message-Based System
*   **TCP Server**: Multi-client support with connection pooling
*   **Message Router**: Routes requests to database, DSL, or extensions
*   **Protocol System**: Maps entity aliases to actual DB/table names with relationships
*   **DSL Query Engine**: SQL-like query language with filters, projections, and aggregations

### Query Cache
*   **Hot Read Caching**: Optional TTL-based LRU cache for frequently executed DSL queries. Configure `CACHE_MAX_MB` (>0) to enable and `CACHE_TTL` to control freshness. Writes automatically invalidate the cache to keep results consistent.

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    TCP Server (Port 8080)                │
│                  Message Protocol (\x04)                 │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                   Message Router                         │
│  Routes: database | onql | protocol | schema            │
└─────┬──────────┬──────────┬────────────┬───────────────┘
      │          │          │            │
      ▼          ▼          ▼            ▼
┌──────────┐ ┌──────┐ ┌──────────┐ ┌──────────┐
│ Database │ │ DSL  │ │ Protocol │ │  Schema  │
│ Handler  │ │Engine│ │Extension │ │Extension │
└────┬─────┘ └──┬───┘ └────┬─────┘ └────┬─────┘
     │          │          │            │
     └──────────┴──────────┴────────────┘
                     │
                     ▼
          ┌──────────────────────┐
          │   Database Layer     │
          │ (Validation/Format)  │
          └──────────┬───────────┘
                     │
                     ▼
          ┌──────────────────────┐
          │   Store Manager      │
          │ (Buffer/Flush/Index) │
          └──────────┬───────────┘
                     │
                     ▼
          ┌──────────────────────┐
          │   BadgerDB Engine    │
          │   (LSM Tree Store)   │
          └──────────────────────┘
```

## 📋 Protocol System

The **Protocol System** allows you to define **entity aliases** that map to actual database tables with field mappings and relationships.

### Protocol Structure

```json
{
  "database": "mydb",
  "entities": {
    "users": {
      "table": "accounts",
      "fields": {
        "id": "account_id",
        "name": "full_name",
        "email": "email_address"
      },
      "relations": {
        "orders": {
          "type": "otm",
          "entity": "orders",
          "fk_field": "id:user_id"
        }
      },
      "context": {
        "user": "mydb.users[id=$1]",
        "admin": "mydb.users[role='admin']"
      }
    }
  }
}
```

### Relationship Types
- **`oto`** (One-to-One): Single related record
- **`otm`** (One-to-Many): Multiple related records
- **`mto`** (Many-to-One): Reverse of one-to-many
- **`mtm`** (Many-to-Many): Through junction table

### Protocol Commands

**Store a Protocol:**
```json
{
  "target": "protocol",
  "payload": ["set", "mypassword", {...protocol_data...}]
}
```

**Retrieve Protocol:**
```json
{
  "target": "protocol",
  "payload": ["desc", "mypassword"]
}
```

**Delete Protocol:**
```json
{
  "target": "protocol",
  "payload": ["drop", "mypassword"]
}
```

## 🔍 DSL Query Language

The DSL (Domain Specific Language) allows SQL-like queries with entity aliases:

```
mydb.users[age>18].name
mydb.orders[status='pending'].customer.name
mydb.products._count()
```

**Features:**
- Entity-based queries (uses protocol mappings)
- Filtering with conditions
- Relationship traversal
- Aggregations (count, sum, avg, etc.)
- Projections and field selection

## 💻 Message Protocol

All communication uses JSON messages with this structure:

```json
{
  "id": "sender_id",
  "target": "database|onql|protocol|schema",
  "rid": "request_id",
  "type": "request|response",
  "payload": "json_string"
}
```

Messages are delimited by `\x04` (EOT character).

### Database Functions

```json
{
  "target": "database",
  "payload": {
    "function": "GetDatabases|CreateDatabase|GetTables|...",
    "args": [...]
  }
}
```

### DSL Queries

```json
{
  "target": "onql",
  "payload": {
    "protopass": "mypassword",
    "query": "mydb.users[age>18]",
    "ctxkey": "user",
    "ctxvalues": ["123"]
  }
}
```

## 🏁 Getting Started

### Prerequisites
*   Go 1.21+

### Installation
```bash
git clone <repo-url>
cd rdbms
go mod tidy
```

### Running the Server
```bash
go run examples/server_demo.go
```

The server starts on **port 8080** by default.

### Configuration
Environment variables:
*   `DB_PATH`: Data storage path (default: `./store`)
*   `FLUSH_INTERVAL`: Buffer flush interval (default: `500ms`)
*   `LOG_LEVEL`: `DEBUG|INFO|WARN|ERROR` (default: `INFO`)
*   `CACHE_MAX_MB`: Memory budget for the DSL query cache (default: `0`, disabled)
*   `CACHE_TTL`: Go duration for cached payload freshness (default: `60s`)

## 📁 Project Structure

```
rdbms/
├── engine/              # BadgerDB wrapper
├── storemanager/        # Buffer, indexing, schema
├── database/            # Validation, formatting, protocols
├── dsl/                 # Query language parser & evaluator
├── router/              # Message routing
├── server/              # TCP server
├── extensions/          # Extension system
│   ├── protocol/        # Protocol management extension
│   └── schema/          # Schema management extension
├── config/              # Configuration loader
├── logger/              # Structured logging
└── examples/            # Demo applications
```

## 🎯 Use Cases

*   **Multi-tenant Applications**: Use protocols for tenant isolation
*   **API Backends**: Expose via TCP with custom client libraries
*   **Embedded Systems**: Lightweight, single-binary database
*   **Real-time Applications**: Fast RAM access with disk durability
*   **Microservices**: Message-based communication between services

## 📚 Examples

See `examples/` directory for:
- `protocol_demo.go` - Protocol system demonstration
- `server_demo.go` - TCP server with client
- `dsl_demo.go` - DSL query examples

## 🔧 Advanced Features

- **Context Queries**: Parameterized queries with `$1, $2` placeholders
- **Field Mapping**: Alias fields to actual column names
- **Relationship Traversal**: Navigate through entity relationships
- **Extension System**: Add custom handlers via extensions
- **Graceful Shutdown**: Ensures data integrity on exit

---

**Built with ❤️ using Go and BadgerDB**
