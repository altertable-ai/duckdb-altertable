//===----------------------------------------------------------------------===//
//                         DuckDB
//
// altertable_connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/api.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "altertable_result.hpp"

namespace duckdb {

struct OwnedAltertableConnection {
	explicit OwnedAltertableConnection(std::unique_ptr<arrow::flight::sql::FlightSqlClient> client_p = nullptr)
	    : client(std::move(client_p)) {
	}
	~OwnedAltertableConnection() {
	}

	std::unique_ptr<arrow::flight::sql::FlightSqlClient> client;
	arrow::flight::FlightCallOptions call_options;
	std::mutex connection_lock;
	//! The catalog name from dbname parameter - used for three-part identifiers
	string catalog;
};

class AltertableConnection {
public:
	explicit AltertableConnection(shared_ptr<OwnedAltertableConnection> connection = nullptr);
	~AltertableConnection();
	// disable copy constructors
	AltertableConnection(const AltertableConnection &other) = delete;
	AltertableConnection &operator=(const AltertableConnection &) = delete;
	//! enable move constructors
	AltertableConnection(AltertableConnection &&other) noexcept;
	AltertableConnection &operator=(AltertableConnection &&) noexcept;

public:
	static AltertableConnection Open(const string &dsn);

	// Execute a query and return the flight info (metadata)
	std::unique_ptr<arrow::flight::FlightInfo> Execute(const string &query);

	// Execute a query and return a reader for the results
	std::unique_ptr<arrow::flight::FlightStreamReader> QueryStream(const string &query);

	// Execute multiple queries (stub for compatibility with transaction)
	vector<unique_ptr<AltertableResult>> ExecuteQueries(const string &queries);

	// Query method that returns AltertableResult (stub for compatibility with transaction)
	unique_ptr<AltertableResult> Query(const string &query);

	// Get the FlightSqlClient
	arrow::flight::sql::FlightSqlClient *GetClient() {
		if (!connection || !connection->client) {
			throw InternalException("AltertableConnection::GetClient - no connection available");
		}
		return connection->client.get();
	}

	arrow::flight::FlightCallOptions &GetCallOptions() {
		if (!connection) {
			throw InternalException("AltertableConnection::GetCallOptions - no connection available");
		}
		return connection->call_options;
	}

	// Get the underlying shared connection
	shared_ptr<OwnedAltertableConnection> GetSharedConnection() {
		return connection;
	}

	// Get the DSN string
	const string &GetDSN() const {
		return dsn;
	}

	// Get the catalog name (from dbname parameter)
	const string &GetCatalog() const {
		if (!connection) {
			static const string empty;
			return empty;
		}
		return connection->catalog;
	}

	bool IsOpen();
	void Close();

	static void DebugSetPrintQueries(bool print);
	static bool DebugPrintQueries();

private:
	shared_ptr<OwnedAltertableConnection> connection;
	string dsn;
};

} // namespace duckdb
