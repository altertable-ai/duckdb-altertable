#include "altertable_optimizer.hpp"
#include "altertable_scanner.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

bool AltertableLimitPushdownOptimizer::IsAltertableScan(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	auto &get = op.Cast<LogicalGet>();
	return IsAltertableScanTableFunction(get.function);
}

void AltertableLimitPushdownOptimizer::PushLimitIntoScan(LogicalOperator &scan_op, idx_t limit_value) {
	auto &get = scan_op.Cast<LogicalGet>();
	auto &bind_data = get.bind_data->Cast<AltertableBindData>();

	// Only push down if we don't already have a limit (don't overwrite a smaller limit)
	if (bind_data.limit.empty()) {
		bind_data.limit = " LIMIT " + to_string(limit_value);
	} else {
		// If there's already a limit, take the smaller one
		// Parse existing limit (format: " LIMIT X")
		string existing = bind_data.limit;
		if (existing.find("LIMIT") != string::npos) {
			size_t pos = existing.find("LIMIT") + 6;
			idx_t existing_limit = std::stoull(existing.substr(pos));
			if (limit_value < existing_limit) {
				bind_data.limit = " LIMIT " + to_string(limit_value);
			}
		}
	}
}

struct AltertableRemotePlan {
	string sql;
	vector<string> names;
	vector<LogicalType> types;
	unordered_map<string, string> bindings;
	optional_ptr<AltertableBindData> source_bind;
};

static string BindingKey(const ColumnBinding &binding) {
	return to_string(binding.table_index) + ":" + to_string(binding.column_index);
}

static string AltertableTableReference(const AltertableBindData &bind_data) {
	if (!bind_data.catalog_name.empty()) {
		return AltertableUtils::QuoteAltertableIdentifier(bind_data.catalog_name) + "." +
		       AltertableUtils::QuoteAltertableIdentifier(bind_data.schema_name) + "." +
		       AltertableUtils::QuoteAltertableIdentifier(bind_data.table_name);
	}
	return AltertableUtils::QuoteAltertableIdentifier(bind_data.schema_name) + "." +
	       AltertableUtils::QuoteAltertableIdentifier(bind_data.table_name);
}

static string ComparisonOperator(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "<>";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return "IS DISTINCT FROM";
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return "IS NOT DISTINCT FROM";
	default:
		throw NotImplementedException("Unsupported comparison for Altertable pushdown");
	}
}

static string GetPredicateFromFilter(TableFilter &filter, const string &col_name) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		return AltertableUtils::QuoteAltertableIdentifier(col_name) + " " +
		       ComparisonOperator(constant_filter.comparison_type) + " " + constant_filter.constant.ToSQLString();
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		string result;
		for (auto &child_filter : and_filter.child_filters) {
			auto child_predicate = GetPredicateFromFilter(*child_filter, col_name);
			if (child_predicate.empty()) {
				return "";
			}
			if (!result.empty()) {
				result += " AND ";
			}
			result += "(" + child_predicate + ")";
		}
		return result;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = filter.Cast<ConjunctionOrFilter>();
		string result;
		for (auto &child_filter : or_filter.child_filters) {
			auto child_predicate = GetPredicateFromFilter(*child_filter, col_name);
			if (child_predicate.empty()) {
				return "";
			}
			if (!result.empty()) {
				result += " OR ";
			}
			result += "(" + child_predicate + ")";
		}
		return result;
	}
	case TableFilterType::IS_NULL:
		return AltertableUtils::QuoteAltertableIdentifier(col_name) + " IS NULL";
	case TableFilterType::IS_NOT_NULL:
		return AltertableUtils::QuoteAltertableIdentifier(col_name) + " IS NOT NULL";
	default:
		return "";
	}
}

class AltertableRemoteSQLBuilder {
public:
	explicit AltertableRemoteSQLBuilder(ClientContext &context) {
	}

	bool TryBuild(LogicalOperator &op, AltertableRemotePlan &result) {
		try {
			result = Build(op);
			return true;
		} catch (NotImplementedException &) {
			return false;
		}
	}

private:
	idx_t next_alias = 0;

	string NextAlias() {
		return "__altertable_" + to_string(next_alias++);
	}

	static string OutputName(Expression &expr, idx_t index) {
		auto alias = expr.GetAlias();
		return alias.empty() ? "column" + to_string(index) : alias;
	}

	unordered_map<string, string> ChildBindings(LogicalOperator &child, const AltertableRemotePlan &plan,
	                                            const string &alias) {
		auto child_bindings = child.GetColumnBindings();
		unordered_map<string, string> result;
		for (idx_t i = 0; i < child_bindings.size(); i++) {
			result[BindingKey(child_bindings[i])] = AltertableUtils::QuoteAltertableIdentifier(alias) + "." +
			                                        AltertableUtils::QuoteAltertableIdentifier(plan.names[i]);
		}
		return result;
	}

	void SetOutputBindings(LogicalOperator &op, AltertableRemotePlan &plan) {
		auto bindings = op.GetColumnBindings();
		for (idx_t i = 0; i < bindings.size(); i++) {
			plan.bindings[BindingKey(bindings[i])] = AltertableUtils::QuoteAltertableIdentifier(plan.names[i]);
		}
	}

	string RenderExpression(Expression &expr, const unordered_map<string, string> &bindings) {
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::BOUND_COLUMN_REF: {
			auto &col_ref = expr.Cast<BoundColumnRefExpression>();
			auto entry = bindings.find(BindingKey(col_ref.binding));
			if (entry == bindings.end()) {
				throw NotImplementedException("Column binding not available for Altertable pushdown");
			}
			return entry->second;
		}
		case ExpressionClass::BOUND_CONSTANT:
			return expr.Cast<BoundConstantExpression>().value.ToSQLString();
		case ExpressionClass::BOUND_CAST: {
			auto &cast = expr.Cast<BoundCastExpression>();
			return "CAST(" + RenderExpression(*cast.child, bindings) + " AS " + cast.return_type.ToString() + ")";
		}
		case ExpressionClass::BOUND_COMPARISON: {
			auto &comparison = expr.Cast<BoundComparisonExpression>();
			return "(" + RenderExpression(*comparison.left, bindings) + " " + ComparisonOperator(comparison.type) +
			       " " + RenderExpression(*comparison.right, bindings) + ")";
		}
		case ExpressionClass::BOUND_CONJUNCTION: {
			auto &conjunction = expr.Cast<BoundConjunctionExpression>();
			string op = conjunction.type == ExpressionType::CONJUNCTION_AND ? " AND " : " OR ";
			string result;
			for (auto &child : conjunction.children) {
				if (!result.empty()) {
					result += op;
				}
				result += "(" + RenderExpression(*child, bindings) + ")";
			}
			return result;
		}
		case ExpressionClass::BOUND_FUNCTION: {
			auto &function = expr.Cast<BoundFunctionExpression>();
			vector<string> children;
			for (auto &child : function.children) {
				children.push_back(RenderExpression(*child, bindings));
			}
			return function.function.name + "(" + StringUtil::Join(children, ", ") + ")";
		}
		case ExpressionClass::BOUND_OPERATOR: {
			auto &op = expr.Cast<BoundOperatorExpression>();
			if (op.type == ExpressionType::OPERATOR_NOT && op.children.size() == 1) {
				return "(NOT " + RenderExpression(*op.children[0], bindings) + ")";
			}
			if (op.type == ExpressionType::OPERATOR_IS_NULL && op.children.size() == 1) {
				return "(" + RenderExpression(*op.children[0], bindings) + " IS NULL)";
			}
			if (op.type == ExpressionType::OPERATOR_IS_NOT_NULL && op.children.size() == 1) {
				return "(" + RenderExpression(*op.children[0], bindings) + " IS NOT NULL)";
			}
			throw NotImplementedException("Unsupported operator for Altertable pushdown");
		}
		default:
			throw NotImplementedException("Unsupported expression for Altertable pushdown");
		}
	}

	string RenderAggregate(Expression &expr, const unordered_map<string, string> &bindings) {
		auto &aggregate = expr.Cast<BoundAggregateExpression>();
		if (aggregate.filter || aggregate.order_bys || aggregate.aggr_type != AggregateType::NON_DISTINCT) {
			throw NotImplementedException("Unsupported aggregate modifier for Altertable pushdown");
		}
		if (aggregate.function.name == "count_star") {
			return "COUNT(*)";
		}
		vector<string> children;
		for (auto &child : aggregate.children) {
			children.push_back(RenderExpression(*child, bindings));
		}
		return StringUtil::Upper(aggregate.function.name) + "(" + StringUtil::Join(children, ", ") + ")";
	}

	AltertableRemotePlan Build(LogicalOperator &op) {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_GET:
			return BuildGet(op.Cast<LogicalGet>());
		case LogicalOperatorType::LOGICAL_PROJECTION:
			return BuildProjection(op.Cast<LogicalProjection>());
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
			return BuildAggregate(op.Cast<LogicalAggregate>());
		case LogicalOperatorType::LOGICAL_FILTER:
			return BuildFilter(op.Cast<LogicalFilter>());
		case LogicalOperatorType::LOGICAL_LIMIT:
			return BuildLimit(op.Cast<LogicalLimit>());
		case LogicalOperatorType::LOGICAL_ORDER_BY:
			return BuildOrder(op.Cast<LogicalOrder>());
		default:
			throw NotImplementedException("Unsupported operator for Altertable pushdown");
		}
	}

	AltertableRemotePlan BuildGet(LogicalGet &get) {
		if (!AltertableLimitPushdownOptimizer::IsAltertableScan(get)) {
			throw NotImplementedException("Not an Altertable scan");
		}
		auto &bind_data = get.bind_data->Cast<AltertableBindData>();
		if (!bind_data.sql.empty()) {
			throw NotImplementedException("Already a query scan");
		}

		AltertableRemotePlan result;
		result.source_bind = &bind_data;

		auto &column_ids = get.GetColumnIds();
		vector<string> select_list;
		if (column_ids.empty()) {
			for (idx_t i = 0; i < bind_data.names.size(); i++) {
				auto name = bind_data.names[i];
				select_list.push_back(AltertableUtils::QuoteAltertableIdentifier(name));
				result.names.push_back(name);
				result.types.push_back(bind_data.types[i]);
			}
		} else {
			// Column bindings follow projection_ids when set (see LogicalGet::GetColumnBindings).
			auto emit_column = [&](const ColumnIndex &column_id) {
				if (column_id.IsRowIdColumn() || column_id.IsVirtualColumn()) {
					// Virtual/rowid columns (e.g. after column pruning for COUNT(*)) can't
					// be fetched from remote. Emit a cheap placeholder so the position in
					// names/types stays in sync with GetColumnBindings().
					select_list.push_back("1");
					result.names.push_back("__altertable_virtual");
					result.types.push_back(column_id.IsRowIdColumn() ? LogicalType::ROW_TYPE
					                                                 : LogicalType::BIGINT);
					return;
				}
				auto col_idx = column_id.GetPrimaryIndex();
				if (col_idx >= bind_data.names.size()) {
					throw NotImplementedException("Altertable column index out of range");
				}
				auto name = bind_data.names[col_idx];
				select_list.push_back(AltertableUtils::QuoteAltertableIdentifier(name));
				result.names.push_back(name);
				result.types.push_back(bind_data.types[col_idx]);
			};
			if (get.projection_ids.empty()) {
				for (auto &column_id : column_ids) {
					emit_column(column_id);
				}
			} else {
				for (auto proj_index : get.projection_ids) {
					if (proj_index >= column_ids.size()) {
						throw NotImplementedException("Altertable projection id out of range");
					}
					emit_column(column_ids[proj_index]);
				}
			}
		}

		result.sql = "SELECT " + StringUtil::Join(select_list, ", ") + " FROM " + AltertableTableReference(bind_data);
		string filters;
		for (auto &entry : get.table_filters.filters) {
			// Keys are storage column ids (GetPrimaryIndex), not indices into column_ids — see
			// duckdb propagate_get.cpp / PhysicalPlanGenerator::CreateTableFilterSet.
			idx_t table_col_idx = entry.first;
			if (table_col_idx >= bind_data.names.size()) {
				throw NotImplementedException("Altertable filter table column out of range");
			}
			auto predicate = GetPredicateFromFilter(*entry.second, bind_data.names[table_col_idx]);
			if (predicate.empty()) {
				throw NotImplementedException("Unsupported table filter for Altertable pushdown");
			}
			if (!filters.empty()) {
				filters += " AND ";
			}
			filters += predicate;
		}
		if (!filters.empty()) {
			result.sql += " WHERE " + filters;
		}

		SetOutputBindings(get, result);
		return result;
	}

	AltertableRemotePlan BuildProjection(LogicalProjection &projection) {
		if (projection.children.size() != 1) {
			throw NotImplementedException("Projection without one child");
		}
		auto child_plan = Build(*projection.children[0]);
		auto alias = NextAlias();
		auto bindings = ChildBindings(*projection.children[0], child_plan, alias);

		AltertableRemotePlan result;
		result.source_bind = child_plan.source_bind;
		vector<string> select_list;
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			auto &expr = projection.expressions[i];
			auto name = OutputName(*expr, i);
			select_list.push_back(RenderExpression(*expr, bindings) + " AS " +
			                      AltertableUtils::QuoteAltertableIdentifier(name));
			result.names.push_back(name);
			result.types.push_back(expr->return_type);
		}
		result.sql = "SELECT " + StringUtil::Join(select_list, ", ") + " FROM (" + child_plan.sql + ") " +
		             AltertableUtils::QuoteAltertableIdentifier(alias);
		SetOutputBindings(projection, result);
		return result;
	}

	AltertableRemotePlan BuildAggregate(LogicalAggregate &aggregate) {
		if (aggregate.children.size() != 1 || !aggregate.grouping_sets.empty() ||
		    !aggregate.grouping_functions.empty()) {
			throw NotImplementedException("Unsupported aggregate shape");
		}
		auto child_plan = Build(*aggregate.children[0]);
		auto alias = NextAlias();
		auto bindings = ChildBindings(*aggregate.children[0], child_plan, alias);

		AltertableRemotePlan result;
		result.source_bind = child_plan.source_bind;
		vector<string> select_list;
		vector<string> group_list;
		for (idx_t i = 0; i < aggregate.groups.size(); i++) {
			auto &expr = aggregate.groups[i];
			auto name = OutputName(*expr, i);
			auto sql = RenderExpression(*expr, bindings);
			select_list.push_back(sql + " AS " + AltertableUtils::QuoteAltertableIdentifier(name));
			group_list.push_back(sql);
			result.names.push_back(name);
			result.types.push_back(expr->return_type);
		}
		for (idx_t i = 0; i < aggregate.expressions.size(); i++) {
			auto &expr = aggregate.expressions[i];
			if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
				throw NotImplementedException("Unsupported aggregate expression");
			}
			auto name = OutputName(*expr, aggregate.groups.size() + i);
			select_list.push_back(RenderAggregate(*expr, bindings) + " AS " +
			                      AltertableUtils::QuoteAltertableIdentifier(name));
			result.names.push_back(name);
			result.types.push_back(expr->return_type);
		}
		result.sql = "SELECT " + StringUtil::Join(select_list, ", ") + " FROM (" + child_plan.sql + ") " +
		             AltertableUtils::QuoteAltertableIdentifier(alias);
		if (!group_list.empty()) {
			result.sql += " GROUP BY " + StringUtil::Join(group_list, ", ");
		}
		SetOutputBindings(aggregate, result);
		return result;
	}

	AltertableRemotePlan BuildFilter(LogicalFilter &filter) {
		if (filter.children.size() != 1 || filter.HasProjectionMap()) {
			throw NotImplementedException("Unsupported filter shape");
		}
		auto child_plan = Build(*filter.children[0]);
		auto alias = NextAlias();
		auto bindings = ChildBindings(*filter.children[0], child_plan, alias);

		vector<string> predicates;
		for (auto &expr : filter.expressions) {
			predicates.push_back(RenderExpression(*expr, bindings));
		}

		AltertableRemotePlan result;
		result.source_bind = child_plan.source_bind;
		result.names = child_plan.names;
		result.types = child_plan.types;
		result.sql = "SELECT * FROM (" + child_plan.sql + ") " + AltertableUtils::QuoteAltertableIdentifier(alias) +
		             " WHERE " + StringUtil::Join(predicates, " AND ");
		SetOutputBindings(filter, result);
		return result;
	}

	AltertableRemotePlan BuildLimit(LogicalLimit &limit) {
		if (limit.children.size() != 1 || limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE ||
		    (limit.offset_val.Type() != LimitNodeType::UNSET &&
		     limit.offset_val.Type() != LimitNodeType::CONSTANT_VALUE)) {
			throw NotImplementedException("Unsupported limit shape");
		}
		auto child_plan = Build(*limit.children[0]);
		AltertableRemotePlan result;
		result.source_bind = child_plan.source_bind;
		result.names = child_plan.names;
		result.types = child_plan.types;
		result.sql = child_plan.sql + " LIMIT " + to_string(limit.limit_val.GetConstantValue());
		if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE && limit.offset_val.GetConstantValue() > 0) {
			result.sql += " OFFSET " + to_string(limit.offset_val.GetConstantValue());
		}
		SetOutputBindings(limit, result);
		return result;
	}

	AltertableRemotePlan BuildOrder(LogicalOrder &order) {
		if (order.children.size() != 1 || order.HasProjectionMap()) {
			throw NotImplementedException("Unsupported order shape");
		}
		auto child_plan = Build(*order.children[0]);
		auto alias = NextAlias();

		vector<string> order_list;
		for (auto &order_node : order.orders) {
			if (order_node.expression->GetExpressionClass() != ExpressionClass::BOUND_REF) {
				throw NotImplementedException("Unsupported order expression for Altertable pushdown");
			}
			auto &ref = order_node.expression->Cast<BoundReferenceExpression>();
			if (ref.index >= child_plan.names.size()) {
				throw NotImplementedException("Order reference out of range");
			}
			string order_sql = AltertableUtils::QuoteAltertableIdentifier(alias) + "." +
			                   AltertableUtils::QuoteAltertableIdentifier(child_plan.names[ref.index]);
			if (order_node.type == OrderType::ASCENDING) {
				order_sql += " ASC";
			} else if (order_node.type == OrderType::DESCENDING) {
				order_sql += " DESC";
			}
			if (order_node.null_order == OrderByNullType::NULLS_FIRST) {
				order_sql += " NULLS FIRST";
			} else if (order_node.null_order == OrderByNullType::NULLS_LAST) {
				order_sql += " NULLS LAST";
			}
			order_list.push_back(order_sql);
		}

		AltertableRemotePlan result;
		result.source_bind = child_plan.source_bind;
		result.names = child_plan.names;
		result.types = child_plan.types;
		result.sql = "SELECT * FROM (" + child_plan.sql + ") " + AltertableUtils::QuoteAltertableIdentifier(alias) +
		             " ORDER BY " + StringUtil::Join(order_list, ", ");
		SetOutputBindings(order, result);
		return result;
	}
};

bool AltertableLimitPushdownOptimizer::TryPushWholeQuery(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	AltertableRemoteSQLBuilder builder(context);
	AltertableRemotePlan remote_plan;
	if (!builder.TryBuild(*plan, remote_plan)) {
		return false;
	}
	if (!remote_plan.source_bind) {
		return false;
	}

	// Preserve the binder's table index so parent operators (joins, subqueries, etc.)
	// still resolve column references. LogicalGet(0, ...) breaks bindings like #[1.0].
	auto output_bindings = plan->GetColumnBindings();
	idx_t table_index = 0;
	if (!output_bindings.empty()) {
		table_index = output_bindings[0].table_index;
	}

	auto &source = *remote_plan.source_bind;
	auto bind_data = make_uniq<AltertableBindData>(context);
	bind_data->dsn = source.dsn;
	bind_data->attach_path = source.attach_path;
	bind_data->catalog_name = source.catalog_name;
	bind_data->schema_name = source.schema_name;
	bind_data->table_name = source.table_name;
	bind_data->sql = remote_plan.sql;
	bind_data->names = remote_plan.names;
	bind_data->types = remote_plan.types;
	bind_data->read_only = source.read_only;
	bind_data->requires_materialization = source.requires_materialization;
	bind_data->can_use_main_thread = source.can_use_main_thread;
	bind_data->max_threads = 1;
	if (source.GetCatalog()) {
		bind_data->SetCatalog(*source.GetCatalog());
	}

	auto function = AltertableScanFunction();
	function.projection_pushdown = false;
	function.filter_pushdown = false;
	auto logical_get = make_uniq<LogicalGet>(table_index, function, std::move(bind_data), std::move(remote_plan.types),
	                                         std::move(remote_plan.names));
	// Empty column_ids make LogicalGet::ResolveTypes add only one column (GetAnyColumn),
	// which breaks multi-column pushed queries and leaves a single #[n.0] binding.
	for (idx_t col = 0; col < logical_get->returned_types.size(); col++) {
		logical_get->AddColumnId(col);
	}
	plan = std::move(logical_get);
	return true;
}

//! Check if the limit can safely be pushed through this operator type
static bool CanPushLimitThrough(LogicalOperatorType type) {
	switch (type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
		// Projections don't change row count
		return true;
	case LogicalOperatorType::LOGICAL_GET:
		// Table scans are the target
		return true;
	case LogicalOperatorType::LOGICAL_LIMIT:
		// Limits are where we extract the limit value
		return true;
	default:
		// For all other operators (filters, joins, aggregates, etc.),
		// we cannot safely push the limit through because they may change
		// the number of rows or require all input rows to produce correct results
		return false;
	}
}

void AltertableLimitPushdownOptimizer::OptimizeRecursive(ClientContext &context, unique_ptr<LogicalOperator> &op,
                                                         optional_idx parent_limit) {
	if (TryPushWholeQuery(context, op)) {
		return;
	}

	// First, check if this is a LIMIT operator
	optional_idx current_limit = parent_limit;
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			idx_t limit_value = limit.limit_val.GetConstantValue();
			// Add offset to limit for correct pushdown
			if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
				limit_value += limit.offset_val.GetConstantValue();
			}
			// Track the limit for children
			if (!current_limit.IsValid() || limit_value < current_limit.GetIndex()) {
				current_limit = limit_value;
			}
		}
	}

	// Check if this is an Altertable scan that we can push a limit into
	if (current_limit.IsValid() && IsAltertableScan(*op)) {
		PushLimitIntoScan(*op, current_limit.GetIndex());
	}

	// Determine if we can pass the limit through to children
	optional_idx limit_for_children;
	if (current_limit.IsValid() && CanPushLimitThrough(op->type)) {
		limit_for_children = current_limit;
	}

	// Recursively process children
	for (auto &child : op->children) {
		OptimizeRecursive(context, child, limit_for_children);
	}
}

void AltertableLimitPushdownOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	OptimizeRecursive(input.context, plan, optional_idx());
}

OptimizerExtension CreateAltertableLimitPushdownOptimizer() {
	OptimizerExtension extension;
	extension.optimize_function = AltertableLimitPushdownOptimizer::Optimize;
	return extension;
}

} // namespace duckdb
