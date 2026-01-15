//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/altertable_connection_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "altertable_connection.hpp"

namespace duckdb {
class AltertableCatalog;
class AltertableConnectionPool;

class AltertablePoolConnection {
public:
	AltertablePoolConnection();
	AltertablePoolConnection(optional_ptr<AltertableConnectionPool> pool, AltertableConnection connection);
	~AltertablePoolConnection();
	// disable copy constructors
	AltertablePoolConnection(const AltertablePoolConnection &other) = delete;
	AltertablePoolConnection &operator=(const AltertablePoolConnection &) = delete;
	//! enable move constructors
	AltertablePoolConnection(AltertablePoolConnection &&other) noexcept;
	AltertablePoolConnection &operator=(AltertablePoolConnection &&) noexcept;

	bool HasConnection();
	AltertableConnection &GetConnection();

private:
	optional_ptr<AltertableConnectionPool> pool;
	AltertableConnection connection;
};

class AltertableConnectionPool {
public:
	static constexpr const idx_t DEFAULT_MAX_CONNECTIONS = 64;

	AltertableConnectionPool(AltertableCatalog &altertable_catalog, idx_t maximum_connections = DEFAULT_MAX_CONNECTIONS);

public:
	bool TryGetConnection(AltertablePoolConnection &connection);
	AltertablePoolConnection GetConnection();
	//! Always returns a connection - even if the connection slots are exhausted
	AltertablePoolConnection ForceGetConnection();
	void ReturnConnection(AltertableConnection connection);
	void SetMaximumConnections(idx_t new_max);

	static void AltertableSetConnectionCache(ClientContext &context, SetScope scope, Value &parameter);

private:
	AltertableCatalog &altertable_catalog;
	mutex connection_lock;
	idx_t active_connections;
	idx_t maximum_connections;
	vector<AltertableConnection> connection_cache;

private:
	AltertablePoolConnection GetConnectionInternal(unique_lock<mutex> &lock);
};

} // namespace duckdb
