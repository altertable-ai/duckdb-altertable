#include "storage/altertable_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

AltertableTransactionManager::AltertableTransactionManager(AttachedDatabase &db_p,
                                                           AltertableCatalog &altertable_catalog)
    : TransactionManager(db_p), altertable_catalog(altertable_catalog) {
}

Transaction &AltertableTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<AltertableTransaction>(altertable_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData AltertableTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &altertable_transaction = transaction.Cast<AltertableTransaction>();
	altertable_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void AltertableTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &altertable_transaction = transaction.Cast<AltertableTransaction>();
	altertable_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void AltertableTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// DuckDB CHECKPOINT is for local storage; remote Arrow Flight SQL has no equivalent.
	// Do not send SQL "CHECKPOINT" to the remote engine (PostgreSQL treats it as a privileged WAL flush).
}

} // namespace duckdb
