//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/altertable_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "altertable_connection.hpp"
#include "storage/altertable_schema_set.hpp"
#include "storage/altertable_connection_pool.hpp"

namespace duckdb {
class AltertableCatalog;
class AltertableSchemaEntry;

class AltertableCatalog : public Catalog {
public:
	explicit AltertableCatalog(AttachedDatabase &db_p, string connection_string, string attach_path,
	                           AccessMode access_mode, string schema_to_load);
	~AltertableCatalog() override;

	string connection_string;
	string attach_path;
	//! Remote Flight SQL catalog for metadata (from catalog/dbname/database in DSN or secret only).
	string remote_catalog;
	AccessMode access_mode;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "altertable";
	}
	optional<Identifier> GetDefaultSchema() const override {
		return Identifier(default_schema.empty() ? "public" : default_schema);
	}

	static string GetConnectionString(ClientContext &context, const string &attach_path, string secret_name);
	static string ExtractCatalogFromConnectionString(const string &connection_string);

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	// These methods are required by the Catalog interface but not supported
	// since Altertable handles all compute server-side
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	bool Supports(RemoteCapability capability) const override {
		switch (capability) {
		case RemoteCapability::IS_REMOTE:
		case RemoteCapability::EXECUTE_QUERY_NODE:
		case RemoteCapability::EXECUTE_STATEMENT:
			return true;
		default:
			return false;
		}
	}
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<SQLStatement> statement) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, const string &sql) override;
	bool SupportsPushdown(const SQLStatement &statement) override;

	const string &GetRemoteCatalog() const {
		return remote_catalog;
	}

	//! Whether or not this is an in-memory Altertable database
	bool InMemory() override;
	string GetDBPath() override;

	AltertableConnectionPool &GetConnectionPool() {
		return connection_pool;
	}

	void ClearCache();

	//! Open a pooled Flight connection and run `SELECT 1` so ATTACH fails on
	//! bad credentials, unreachable hosts, or an invalid selected catalog.
	void ValidateConnection();

	//! Whether or not this catalog should search a specific type with the standard priority
	CatalogLookupBehavior CatalogTypeLookupRule(CatalogType type) const override {
		switch (type) {
		case CatalogType::TABLE_ENTRY:
		case CatalogType::TYPE_ENTRY:
		case CatalogType::VIEW_ENTRY:
			return CatalogLookupBehavior::STANDARD;
		default:
			// unsupported type (e.g. scalar functions, aggregates, indexes...)
			return CatalogLookupBehavior::NEVER_LOOKUP;
		}
	}

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

	AltertableSchemaSet schemas;
	AltertableConnectionPool connection_pool;
	string default_schema;
};

} // namespace duckdb
