//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/altertable_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "altertable_connection.hpp"
#include "storage/altertable_connection_pool.hpp"

namespace duckdb {
class AltertableCatalog;
class AltertableSchemaEntry;
class AltertableTableEntry;

enum class AltertableTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class AltertableTransaction : public Transaction {
public:
	AltertableTransaction(AltertableCatalog &altertable_catalog, TransactionManager &manager, ClientContext &context);
	~AltertableTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	AltertableConnection &GetConnectionWithoutTransaction();
	AltertableConnection &GetConnection();
	ClientContext &GetContext();

	string GetDSN();
	unique_ptr<AltertableResult> Query(const string &query);
	int64_t ExecuteUpdate(const string &query);
	unique_ptr<AltertableResult> QueryWithoutTransaction(const string &query);
	static AltertableTransaction &Get(ClientContext &context, Catalog &catalog);

	optional_ptr<CatalogEntry> ReferenceEntry(shared_ptr<CatalogEntry> &entry);

	string GetTemporarySchema();

private:
	AltertablePoolConnection connection;
	AltertableTransactionState transaction_state;
	AccessMode access_mode;
	string temporary_schema;
	reference_map_t<CatalogEntry, shared_ptr<CatalogEntry>> referenced_entries;

private:
	//! Retrieves the connection **without** starting a transaction if none is active
	AltertableConnection &GetConnectionRaw();

	string GetBeginTransactionQuery();
};

} // namespace duckdb
