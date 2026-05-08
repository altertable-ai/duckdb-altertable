#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "storage/altertable_catalog.hpp"
#include "storage/altertable_table_entry.hpp"

namespace duckdb {

class AltertablePhysicalInsert : public PhysicalOperator {
public:
	AltertablePhysicalInsert(PhysicalPlan &physical_plan, vector<LogicalType> types, AltertableCatalog &catalog,
	                         TableCatalogEntry &table, idx_t estimated_cardinality, bool return_chunk);
	AltertablePhysicalInsert(PhysicalPlan &physical_plan, LogicalOperator &op, AltertableCatalog &catalog,
	                         SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info,
	                         idx_t estimated_cardinality);

public:
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}
	bool SinkOrderDependent() const override {
		return true;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

private:
	string GetQualifiedTableName() const;
	string BuildInsertSQL(DataChunk &chunk) const;

private:
	AltertableCatalog &catalog;
	optional_ptr<TableCatalogEntry> table;
	optional_ptr<SchemaCatalogEntry> schema;
	unique_ptr<BoundCreateTableInfo> create_info;
	vector<string> column_names;
	bool return_chunk;
};

} // namespace duckdb
