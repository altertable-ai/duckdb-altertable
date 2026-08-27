# DuckDB Altertable Extension

A DuckDB extension for connecting to Altertable. Query Altertable databases directly from DuckDB using the high-performance Arrow Flight protocol.

## Features

- **ATTACH databases** - Connect to remote Altertable servers as attached databases
- **Direct table access** - Query remote tables using standard SQL syntax
- **Raw query execution** - Run arbitrary SQL queries and DDL statements on remote servers
- **Attached writes** - Use `CREATE TABLE`, `CREATE TABLE AS`, `INSERT ... VALUES`, and `INSERT ... SELECT` against attached Altertable tables
- **Catalog integration** - Browse schemas and tables through DuckDB's catalog
- **Nested Arrow types** - JSON, LIST, MAP, and unnamed STRUCT columns round-trip through attached scans and `altertable_query`

## Installation

```sql
INSTALL altertable FROM community;
LOAD altertable;
```

## Quick Start

### Attach a Remote Database

```sql
-- Attach an Altertable database
CREATE SECRET my_altertable (
    TYPE altertable,
    USER 'my-user',
    PASSWORD 'my-pass'
);
ATTACH 'catalog=my-altertable-catalog' AS db (TYPE ALTERTABLE, SECRET my_altertable);

-- Query tables directly
SELECT * FROM db.main.events;

-- Create and load remote tables through the attached database
CREATE TABLE db.main.example_events (id INTEGER, name VARCHAR);
INSERT INTO db.main.example_events VALUES (1, 'launch');
INSERT INTO db.main.example_events SELECT id, name FROM local_events;

-- Detach when done
DETACH db;
```

### Connection String Parameters

| Parameter      | Description                        | Example                    |
| -------------- | ---------------------------------- | -------------------------- |
| `user`         | Username for authentication        | `your-altertable-user`     |
| `password`     | Password for authentication        | `your-altertable-password` |
| `catalog`      | Remote Altertable catalog          | `analytics`                |
| `compute_size` | Optional compute size (see below)  | `XL`                       |
| `host`         | Server hostname or IP address      | `flight.altertable.ai`     |
| `port`         | Server port                        | `443`                      |
| `ssl`          | Enable SSL/TLS (`true` or `false`) | `true`                     |

Default connection behavior:

- `ATTACH` opens a Flight SQL session immediately (`SELECT 1`) and fails if the host is unreachable, credentials are rejected, or a selected `catalog` is invalid
- set `catalog` in the DSN or secret when the server exposes multiple Flight SQL catalogs and you need metadata filtering (`duckdb_tables()`, schema listing) or a session catalog; omitting `catalog` lists every schema the server returns (works with altertable-mock) and remote objects must be referenced with catalog-qualified names
- JSON, LIST, MAP, and unnamed STRUCT columns round-trip through attached scans and `altertable_query`
- set `compute_size` optionally to request a compute tier for the Flight SQL session; omit it to use the server default. Accepted values: `XS`, `S`, `M`, `L`, `XL`, `2XL`/`XXL`, `3XL`/`XXXL`, `4XL`/`XXXXL`
- DSN keys are case-insensitive and values can be quoted with single or double quotes when needed
- Prefer secrets over inline DSNs so passwords do not appear in SQL history or attached database paths

When the attachment only needs to select a remote catalog, a single-word path
is accepted as shorthand for `catalog=...`:

```sql
ATTACH 'analytics' AS analytics (TYPE ALTERTABLE, SECRET my_altertable);
```

### Secrets

Use DuckDB secrets so credentials are not repeated in SQL statements. Put
attachment-specific settings such as `catalog` in the `ATTACH` connection
string:

```sql
CREATE SECRET my_altertable (
    TYPE altertable,
    USER 'your-user',
    PASSWORD 'your-password',
    COMPUTE_SIZE 'XL'
);

ATTACH 'catalog=your-altertable-catalog' AS analytics (TYPE altertable, SECRET my_altertable);
```

## Functions

### `altertable_query(database, query)`

Execute a SELECT query on the attached database and return results. When the first
argument names an attached Altertable database, the function uses that database's
DuckDB transaction connection; its reads therefore observe uncommitted attached
writes and follow `COMMIT`/`ROLLBACK`. When the first argument is a raw DSN, the
function opens an independent Flight session that does not share DuckDB
transactions.

```sql
-- Run a query and get results
SELECT * FROM altertable_query('db', 'SELECT id, name FROM users WHERE active = true');
```

### `altertable_execute(database, statement)`

Execute DDL or DML statements (CREATE, INSERT, UPDATE, DELETE, etc.) on the remote
database. The first argument must be an attached Altertable database name, not a
raw DSN. The statement participates in the current DuckDB transaction and is
rejected for `READ_ONLY` attachments.

```sql
-- Create a table
CALL altertable_execute('db', 'CREATE TABLE my_table (id INTEGER, name VARCHAR)');

-- Insert data
CALL altertable_execute('db', 'INSERT INTO my_table VALUES (1, ''Alice'')');

-- Drop a table
CALL altertable_execute('db', 'DROP TABLE IF EXISTS my_table');
```

## Usage Examples

### Full Workflow Example

```sql
-- Load the extension
LOAD altertable;

-- Connect to a remote Arrow Flight SQL server
CREATE SECRET acme_altertable (
    TYPE altertable,
    USER 'acme',
    PASSWORD 'secret'
);
ATTACH 'catalog=analytics' AS analytics (TYPE ALTERTABLE, SECRET acme_altertable);

-- Explore available tables
SELECT * FROM analytics.information_schema.tables;

-- Query remote data
SELECT
    customer_id,
    SUM(order_total) as total_spent
FROM analytics.sales.orders
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;

-- Join local and remote data
CREATE TABLE local_customers AS SELECT * FROM read_csv('customers.csv');

SELECT l.name, r.total_orders
FROM local_customers l
JOIN analytics.sales.customer_summary r ON l.id = r.customer_id;
```

### Pushdown behavior

An all-remote `SELECT` can be forwarded as one Flight SQL statement, including
joins, aggregates, CTEs, set operations, projections, filters, ordering, and
limits when DuckDB can bind every referenced relation to the same attachment.
Mixed local/remote plans use scan-level projection, filter, and limit pushdown
where the predicate is representable by Altertable; the remaining work stays in
DuckDB. Unsupported scan filters fail rather than being silently omitted.

Use `EXPLAIN` to verify whether a query is remote. Setting
`SET altertable_debug_show_queries = true` emits only a remote SQL byte count
and fingerprint, not query text or literal values. This setting is process-wide
and is intended for diagnostics, not application-level telemetry.

### Attached DDL and Writes

The attached database path supports common relation DDL and inserts:

```sql
CREATE TABLE analytics.main.new_orders (order_id INTEGER, amount DOUBLE);
INSERT INTO analytics.main.new_orders VALUES (1, 42.50);
INSERT INTO analytics.main.new_orders
SELECT order_id, amount FROM local_orders;

CREATE TABLE analytics.main.top_customers AS
SELECT customer_id, SUM(amount) AS total_amount
FROM analytics.main.new_orders
GROUP BY customer_id;

ALTER TABLE analytics.main.new_orders ADD COLUMN note VARCHAR;
DROP TABLE analytics.main.top_customers;
```

`READ_ONLY` attachments reject attached writes and `altertable_execute`, including
schema/table creation, alteration, and drops.
Attached DML and `altertable_execute` against an attached database name share the
current DuckDB transaction: `BEGIN` / `COMMIT` / `ROLLBACK` apply on the remote
Flight session. Use `altertable_execute('attached_name', ...)` for statements
that cannot be pushed down; do not pass a raw DSN, which would open an
independent autocommit session.
Attached `UPDATE` and `DELETE` statements whose complete plan is remote are
forwarded as one Flight SQL statement. This includes same-attachment
`UPDATE ... FROM`, `DELETE ... USING`, and remote subqueries or CTEs. Mixed
local/remote writes and `INSERT ... RETURNING` are not supported yet and are
rejected. Fully remote `UPDATE ... RETURNING` and `DELETE ... RETURNING`
statements return the rows produced by the remote statement:
Use `altertable_execute` when a statement falls outside this pushdown scope:

```sql
UPDATE analytics.main.new_orders SET note = 'reviewed' WHERE order_id = 1;
DELETE FROM analytics.main.new_orders WHERE order_id = 1;
UPDATE analytics.main.new_orders SET note = 'reviewed'
RETURNING order_id, note;
DELETE FROM analytics.main.new_orders WHERE order_id = 1
RETURNING order_id, note;

CALL altertable_execute('analytics', 'UPDATE main.new_orders SET note = ''reviewed'' WHERE order_id = 1');
CALL altertable_execute('analytics', 'DELETE FROM main.new_orders WHERE order_id = 1');
```

## Building from Source

### Prerequisites

- DuckDB source (as git submodule)
- VCPKG for dependency management
- CMake 3.20+ (matches the extension `CMakeLists.txt`)
- Arrow Flight SQL libraries (arrow, arrow-flight, arrow-flight-sql)

### Setup VCPKG

```bash
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg && ./bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=$(pwd)/scripts/buildsystems/vcpkg.cmake
```

### Build

```bash
git clone --recurse-submodules https://github.com/altertable-ai/duckdb-altertable.git
cd duckdb-altertable
GEN=ninja make
```

Build outputs:

- `./build/release/duckdb` - DuckDB shell with extension loaded
- `./build/release/extension/altertable/altertable.duckdb_extension` - Loadable extension

### Run Tests

```bash
# Recommended: starts the official mock container, sets ALTERTABLE_TEST_* for you, then runs the suite
make test-mock
```

For a manual server (no Docker), set the variables yourself, for example:

```bash
export ALTERTABLE_TEST_HOST=127.0.0.1
export ALTERTABLE_TEST_PORT=15002
export ALTERTABLE_TEST_USER=testuser
export ALTERTABLE_TEST_PASSWORD=testpass
export ALTERTABLE_TEST_SSL=false
make test
```

## License

MIT License - see [LICENSE](LICENSE) for details.
