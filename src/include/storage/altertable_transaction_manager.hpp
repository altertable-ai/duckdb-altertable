//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/altertable_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/altertable_catalog.hpp"
#include "storage/altertable_transaction.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

class AltertableTransactionManager : public TransactionManager {
public:
	AltertableTransactionManager(AttachedDatabase &db_p, AltertableCatalog &altertable_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	AltertableCatalog &altertable_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<AltertableTransaction>> transactions;
};

} // namespace duckdb
