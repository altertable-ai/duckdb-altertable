#include "storage/altertable_connection_pool.hpp"
#include "storage/altertable_catalog.hpp"

namespace duckdb {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
static bool use_connection_cache = true;

AltertablePoolConnection::AltertablePoolConnection() : pool(nullptr) {
}

AltertablePoolConnection::AltertablePoolConnection(optional_ptr<AltertableConnectionPool> pool,
                                                   AltertableConnection connection_p)
    : pool(pool), connection(std::move(connection_p)) {
}

AltertablePoolConnection::~AltertablePoolConnection() {
	if (pool) {
		pool->ReturnConnection(std::move(connection));
	}
}

AltertablePoolConnection::AltertablePoolConnection(AltertablePoolConnection &&other) noexcept {
	std::swap(pool, other.pool);
	std::swap(connection, other.connection);
}

AltertablePoolConnection &AltertablePoolConnection::operator=(AltertablePoolConnection &&other) noexcept {
	std::swap(pool, other.pool);
	std::swap(connection, other.connection);
	return *this;
}

bool AltertablePoolConnection::HasConnection() {
	return pool;
}

AltertableConnection &AltertablePoolConnection::GetConnection() {
	if (!HasConnection()) {
		throw InternalException("AltertablePoolConnection::GetConnection called without a transaction pool");
	}
	return connection;
}

AltertableConnectionPool::AltertableConnectionPool(AltertableCatalog &altertable_catalog, idx_t maximum_connections_p)
    : altertable_catalog(altertable_catalog), active_connections(0), maximum_connections(maximum_connections_p) {
}

AltertablePoolConnection AltertableConnectionPool::GetConnectionInternal(unique_lock<mutex> &lock) {
	active_connections++;
	// check if we have any cached connections left
	if (!connection_cache.empty()) {
		auto connection = AltertablePoolConnection(this, std::move(connection_cache.back()));
		connection_cache.pop_back();
		return connection;
	}
	// no cached connections left but there is space to open a new one - open it after releasing the cache lock
	lock.unlock();
	return AltertablePoolConnection(this, AltertableConnection::Open(altertable_catalog.connection_string));
}

AltertablePoolConnection AltertableConnectionPool::ForceGetConnection() {
	unique_lock<mutex> l(connection_lock);
	return GetConnectionInternal(l);
}

bool AltertableConnectionPool::TryGetConnection(AltertablePoolConnection &connection) {
	unique_lock<mutex> l(connection_lock);
	if (active_connections >= maximum_connections) {
		return false;
	}
	connection = GetConnectionInternal(l);
	return true;
}

void AltertableConnectionPool::AltertableSetConnectionCache(ClientContext &context, SetScope scope, Value &parameter) {
	if (parameter.IsNull()) {
		throw BinderException("Cannot be set to NULL");
	}
	use_connection_cache = BooleanValue::Get(parameter);
}

AltertablePoolConnection AltertableConnectionPool::GetConnection() {
	AltertablePoolConnection result;
	if (!TryGetConnection(result)) {
		throw IOException("Failed to get connection from AltertableConnectionPool - maximum connection count exceeded "
		                  "(%llu/%llu max)",
		                  active_connections, maximum_connections);
	}
	return result;
}

void AltertableConnectionPool::ReturnConnection(AltertableConnection connection) {
	unique_lock<mutex> l(connection_lock);
	if (active_connections <= 0) {
		throw InternalException("AltertableConnectionPool::ReturnConnection called but active_connections is 0");
	}
	if (!use_connection_cache) {
		// not caching - just return
		active_connections--;
		return;
	}
	// we want to cache the connection
	// check if the underlying connection is still usable
	// avoid holding the lock while doing this
	l.unlock();
	// For Arrow Flight SQL, we'll just assume connections are valid
	// TODO: Implement proper connection health checking for Flight SQL
	bool connection_is_bad = false;
	// lock and return the connection
	l.lock();
	active_connections--;
	if (connection_is_bad) {
		// if the connection is bad we cannot cache it
		return;
	}
	if (active_connections >= maximum_connections) {
		// if the maximum number of connections has been decreased by the user we might need to reclaim the connection
		// immediately
		return;
	}
	connection_cache.push_back(std::move(connection));
}

void AltertableConnectionPool::SetMaximumConnections(idx_t new_max) {
	lock_guard<mutex> l(connection_lock);
	if (new_max < maximum_connections) {
		// potentially close connections
		// note that we can only close connections in the connection cache
		// we will have to wait for connections to be returned
		auto total_open_connections = active_connections + connection_cache.size();
		while (!connection_cache.empty() && total_open_connections > new_max) {
			total_open_connections--;
			connection_cache.pop_back();
		}
	}
	maximum_connections = new_max;
}

} // namespace duckdb
