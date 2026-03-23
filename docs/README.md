# Altertable - DuckDB Arrow Flight SQL Extension

A DuckDB extension for connecting to Altertable. Query Altertable databases directly from DuckDB using the high-performance Arrow Flight protocol.

## Features

- **ATTACH databases** - Connect to remote Altertable servers as attached databases
- **Direct table access** - Query remote tables using standard SQL syntax
- **Raw query execution** - Run arbitrary SQL queries and DDL statements on remote servers
- **Catalog integration** - Browse schemas and tables through DuckDB's catalog

## Installation

```sql
INSTALL altertable FROM community;
LOAD altertable;
```

## Quick Start

### Attach a Remote Database

```sql
-- Attach an Altertable database
ATTACH 'user=myuser password=mypass' AS db (TYPE ALTERTABLE);

-- Query tables directly
SELECT * FROM db.main.events;

-- Detach when done
DETACH db;
```

### Connection String Parameters

| Parameter  | Description                        | Example                    |
| ---------- | ---------------------------------- | -------------------------- |
| `host`     | Server hostname or IP address      | `flight.altertable.ai`     |
| `port`     | Server port                        | `443`                      |
| `user`     | Username for authentication        | `your-altertable-user`     |
| `password` | Password for authentication        | `your-altertable-password` |
| `ssl`      | Enable SSL/TLS (`true` or `false`) | `true`                     |

Default connection behavior:

- `host=flight.altertable.ai`
- `port=443`
- `ssl=true` (TLS enabled unless you explicitly set `ssl=false`)

### Secrets

Use DuckDB secrets so credentials are not repeated in SQL statements:

```sql
CREATE SECRET my_altertable (
    TYPE altertable,
    HOST 'flight.altertable.ai',
    PORT '443',
    USER 'your-user',
    PASSWORD 'your-password',
    SSL 'true'
);

ATTACH '' AS analytics (TYPE altertable, SECRET my_altertable);
```

## Functions

### `altertable_query(database, query)`

Execute a SELECT query on the attached database and return results.

```sql
-- Run a query and get results
SELECT * FROM altertable_query('db', 'SELECT id, name FROM users WHERE active = true');
```

### `altertable_execute(database, statement)`

Execute DDL or DML statements (CREATE, INSERT, UPDATE, DELETE, etc.) on the remote database.

```sql
-- Create a table
CALL altertable_execute('db', 'CREATE TABLE my_table (id INTEGER, name VARCHAR)');

-- Insert data
CALL altertable_execute('db', 'INSERT INTO my_table VALUES (1, ''Alice'')');

-- Drop a table
CALL altertable_execute('db', 'DROP TABLE IF EXISTS my_table');
```

### `altertable_scan(connection_string, schema, table)`

Scan a remote table directly without attaching a database.

```sql
SELECT * FROM altertable_scan(
    'user=u password=p',
    'main',
    'my_table'
);
```

### `altertable_scan_pushdown(connection_string, schema, table)`

Same implementation as `altertable_scan` (predicate and projection pushdown are enabled for both). Use whichever name you prefer; there is no behavioral difference today.

```sql
SELECT * FROM altertable_scan_pushdown(
    'host=server.com port=443 user=u password=p ssl=true',
    'public',
    'my_table'
) WHERE id > 100;
```

## Usage Examples

### Full Workflow Example

```sql
-- Load the extension
LOAD altertable;

-- Connect to a remote Arrow Flight SQL server
ATTACH 'user=acme password=secret ssl=true' AS analytics (TYPE ALTERTABLE);

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
