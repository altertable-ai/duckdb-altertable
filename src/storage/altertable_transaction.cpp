#include "storage/altertable_transaction.hpp"
#include "storage/altertable_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "altertable_result.hpp"

namespace duckdb {

AltertableTransaction::AltertableTransaction(AltertableCatalog &altertable_catalog, TransactionManager &manager,
                                             ClientContext &context)
    : Transaction(manager, context), access_mode(altertable_catalog.access_mode) {
	connection = altertable_catalog.GetConnectionPool().GetConnection();
}

AltertableTransaction::~AltertableTransaction() {
	// If DuckDB destroyed the transaction without Commit/Rollback, close the remote transaction so the
	// connection can be returned to the pool without leaving an idle open transaction on the server.
	if (transaction_state == AltertableTransactionState::TRANSACTION_STARTED) {
		try {
			GetConnectionRaw().Execute("ROLLBACK");
		} catch (...) {
		}
		transaction_state = AltertableTransactionState::TRANSACTION_FINISHED;
	}
}

ClientContext &AltertableTransaction::GetContext() {
	return *context.lock();
}

void AltertableTransaction::Start() {
	transaction_state = AltertableTransactionState::TRANSACTION_NOT_YET_STARTED;
}
void AltertableTransaction::Commit() {
	if (transaction_state == AltertableTransactionState::TRANSACTION_STARTED) {
		transaction_state = AltertableTransactionState::TRANSACTION_FINISHED;
		GetConnectionRaw().Execute("COMMIT");
	}
}
void AltertableTransaction::Rollback() {
	if (transaction_state == AltertableTransactionState::TRANSACTION_STARTED) {
		transaction_state = AltertableTransactionState::TRANSACTION_FINISHED;
		GetConnectionRaw().Execute("ROLLBACK");
	}
}

string AltertableTransaction::GetBeginTransactionQuery() {
	// PostgreSQL-compatible session transaction (Flight SQL / Altertable backend)
	string result = "BEGIN TRANSACTION";
	if (access_mode == AccessMode::READ_ONLY) {
		result += " READ ONLY";
	}
	return result;
}

AltertableConnection &AltertableTransaction::GetConnectionWithoutTransaction() {
	if (transaction_state == AltertableTransactionState::TRANSACTION_STARTED) {
		throw std::runtime_error("Execution without a Transaction is not possible if a Transaction already started");
	}
	if (access_mode == AccessMode::READ_ONLY) {
		throw std::runtime_error("Execution without a Transaction is not possible in Read Only Mode");
	}
	return connection.GetConnection();
}

AltertableConnection &AltertableTransaction::GetConnection() {
	auto &con = GetConnectionRaw();
	if (transaction_state == AltertableTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = AltertableTransactionState::TRANSACTION_STARTED;
		string query = GetBeginTransactionQuery();
		con.Execute(query);
	}
	return con;
}

AltertableConnection &AltertableTransaction::GetConnectionRaw() {
	return connection.GetConnection();
}

string AltertableTransaction::GetDSN() {
	return GetConnectionRaw().GetDSN();
}

unique_ptr<AltertableResult> AltertableTransaction::Query(const string &query) {
	auto &con = GetConnectionRaw();
	if (transaction_state == AltertableTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = AltertableTransactionState::TRANSACTION_STARTED;
		string transaction_start = GetBeginTransactionQuery();
		// Execute BEGIN statement separately since DuckDB doesn't support multiple statements
		con.Execute(transaction_start);
	}
	return con.Query(query);
}

int64_t AltertableTransaction::ExecuteUpdate(const string &query) {
	auto &con = GetConnectionRaw();
	if (transaction_state == AltertableTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = AltertableTransactionState::TRANSACTION_STARTED;
		string transaction_start = GetBeginTransactionQuery();
		con.Execute(transaction_start);
	}
	return con.ExecuteUpdate(query);
}

unique_ptr<AltertableResult> AltertableTransaction::QueryWithoutTransaction(const string &query) {
	auto &con = GetConnectionRaw();
	if (transaction_state == AltertableTransactionState::TRANSACTION_STARTED) {
		throw std::runtime_error("Execution without a Transaction is not possible if a Transaction already started");
	}
	if (access_mode == AccessMode::READ_ONLY) {
		throw std::runtime_error("Execution without a Transaction is not possible in Read Only Mode");
	}
	return con.Query(query);
}

vector<unique_ptr<AltertableResult>> AltertableTransaction::ExecuteQueries(const string &queries) {
	auto &con = GetConnectionRaw();
	if (transaction_state == AltertableTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = AltertableTransactionState::TRANSACTION_STARTED;
		string transaction_start = GetBeginTransactionQuery();
		// Execute BEGIN statement separately since DuckDB doesn't support multiple statements
		con.Execute(transaction_start);
	}
	return con.ExecuteQueries(queries);
}

optional_ptr<CatalogEntry> AltertableTransaction::ReferenceEntry(shared_ptr<CatalogEntry> &entry) {
	auto &ref = *entry;
	referenced_entries.emplace(ref, entry);
	return ref;
}

string AltertableTransaction::GetTemporarySchema() {
	// DuckDB uses 'temp' as the temporary schema name
	if (temporary_schema.empty()) {
		temporary_schema = "temp";
	}
	return temporary_schema;
}

AltertableTransaction &AltertableTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<AltertableTransaction>();
}

} // namespace duckdb
