#include "altertable_physical.hpp"
#include "altertable_utils.hpp"
#include "storage/altertable_transaction.hpp"
#include "storage/altertable_table_set.hpp"

namespace duckdb {

class AltertableInsertGlobalState : public GlobalSinkState {
public:
	mutex lock;
	idx_t insert_count = 0;
	bool emitted_count = false;
};

class AltertableInsertLocalState : public LocalSinkState {};

AltertablePhysicalInsert::AltertablePhysicalInsert(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                   AltertableCatalog &catalog_p, TableCatalogEntry &table_p,
                                                   idx_t estimated_cardinality, bool return_chunk_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::INSERT, std::move(types), estimated_cardinality),
      catalog(catalog_p), table(table_p), return_chunk(return_chunk_p) {
	for (auto &column : table->GetColumns().Logical()) {
		column_names.push_back(column.Name());
	}
}

AltertablePhysicalInsert::AltertablePhysicalInsert(PhysicalPlan &physical_plan, LogicalOperator &op,
                                                   AltertableCatalog &catalog_p, SchemaCatalogEntry &schema_p,
                                                   unique_ptr<BoundCreateTableInfo> info, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::INSERT, op.types, estimated_cardinality),
      catalog(catalog_p), schema(schema_p), create_info(std::move(info)), return_chunk(false) {
	for (auto &column : create_info->Base().columns.Logical()) {
		column_names.push_back(column.Name());
	}
}

string AltertablePhysicalInsert::GetQualifiedTableName() const {
	string schema_name;
	string table_name;
	if (table) {
		schema_name = table->schema.name;
		table_name = table->name;
	} else {
		auto &base = create_info->Base();
		schema_name = base.schema.empty() ? schema->name : base.schema;
		table_name = base.table;
	}
	return AltertableUtils::QuoteAltertableIdentifier(schema_name) + "." +
	       AltertableUtils::QuoteAltertableIdentifier(table_name);
}

string AltertablePhysicalInsert::BuildInsertSQL(DataChunk &chunk) const {
	string sql = "INSERT INTO " + GetQualifiedTableName();
	sql += " (";
	for (idx_t col = 0; col < column_names.size(); col++) {
		if (col > 0) {
			sql += ", ";
		}
		sql += AltertableUtils::QuoteAltertableIdentifier(column_names[col]);
	}
	sql += ") VALUES ";
	for (idx_t row = 0; row < chunk.size(); row++) {
		if (row > 0) {
			sql += ", ";
		}
		sql += "(";
		for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
			if (col > 0) {
				sql += ", ";
			}
			sql += chunk.GetValue(col, row).ToSQLString();
		}
		sql += ")";
	}
	return sql;
}

unique_ptr<GlobalSinkState> AltertablePhysicalInsert::GetGlobalSinkState(ClientContext &context) const {
	if (create_info) {
		auto &transaction = AltertableTransaction::Get(context, catalog);
		auto create_sql = GetAltertableCreateTable(create_info->Base());
		transaction.ExecuteUpdate(create_sql);
		catalog.ClearCache();
	}
	return make_uniq<AltertableInsertGlobalState>();
}

unique_ptr<LocalSinkState> AltertablePhysicalInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<AltertableInsertLocalState>();
}

SinkResultType AltertablePhysicalInsert::Sink(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSinkInput &input) const {
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	auto sql = BuildInsertSQL(chunk);
	auto &transaction = AltertableTransaction::Get(context.client, catalog);
	transaction.ExecuteUpdate(sql);

	auto &gstate = input.global_state.Cast<AltertableInsertGlobalState>();
	lock_guard<mutex> lock(gstate.lock);
	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType AltertablePhysicalInsert::Combine(ExecutionContext &context,
                                                        OperatorSinkCombineInput &input) const {
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType AltertablePhysicalInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                    OperatorSinkFinalizeInput &input) const {
	catalog.ClearCache();
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> AltertablePhysicalInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<GlobalSourceState>();
}

SourceResultType AltertablePhysicalInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<AltertableInsertGlobalState>();
	if (insert_gstate.emitted_count) {
		return SourceResultType::FINISHED;
	}
	insert_gstate.emitted_count = true;
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(insert_gstate.insert_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
