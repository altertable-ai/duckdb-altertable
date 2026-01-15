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
	~AltertableCatalog();

	string connection_string;
	string attach_path;
	string remote_catalog;  // The catalog name from dbname parameter
	AccessMode access_mode;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "altertable";
	}
	string GetDefaultSchema() const override {
		return default_schema.empty() ? "public" : default_schema;
	}

	static string GetConnectionString(ClientContext &context, const string &attach_path, string secret_name);

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

	AltertableVersion GetAltertableVersion() const {
		return version;
	}
	
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

private:
	AltertableVersion version;
	AltertableSchemaSet schemas;
	AltertableConnectionPool connection_pool;
	string default_schema;
};

} // namespace duckdb
