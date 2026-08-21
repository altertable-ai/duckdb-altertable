#include "altertable_optimizer.hpp"
#include "altertable_physical.hpp"
#include "altertable_scanner.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
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
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "storage/altertable_catalog.hpp"
#include "storage/altertable_table_entry.hpp"

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
	return AltertableUtils::QualifiedTableReference(bind_data.catalog_name, bind_data.schema_name,
	                                                bind_data.table_name);
}

static string OutputExpressionName(Expression &expr, idx_t index) {
	auto alias = expr.GetAlias();
	return alias.empty() ? "column" + to_string(index) : alias;
}

static void ExtractPlanOutputNames(LogicalOperator &op, vector<string> &names) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			names.push_back(OutputExpressionName(*projection.expressions[i], i));
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		idx_t index = 0;
		for (auto &group : aggregate.groups) {
			names.push_back(OutputExpressionName(*group, index++));
		}
		for (auto &expr : aggregate.expressions) {
			names.push_back(OutputExpressionName(*expr, index++));
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		ExtractPlanOutputNames(*op.children[0], names);
		return;
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (!get.GetColumnIds().empty()) {
			for (auto &col_id : get.GetColumnIds()) {
				if (col_id.IsRowIdColumn() || col_id.IsVirtualColumn()) {
					continue;
				}
				names.push_back(get.names[col_id.GetPrimaryIndex()]);
			}
			return;
		}
		names = get.names;
		return;
	}
	default:
		throw NotImplementedException("Unsupported operator for Altertable output name extraction");
	}
}

static void ExtractPlanOutputTypes(LogicalOperator &op, vector<LogicalType> &types) {
	types = op.types;
	if (!types.empty()) {
		return;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		for (auto &group : aggregate.groups) {
			types.push_back(group->return_type);
		}
		for (auto &expr : aggregate.expressions) {
			types.push_back(expr->return_type);
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		ExtractPlanOutputTypes(*op.children[0], types);
		return;
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (!get.GetColumnIds().empty()) {
			for (auto &col_id : get.GetColumnIds()) {
				if (col_id.IsRowIdColumn() || col_id.IsVirtualColumn()) {
					continue;
				}
				types.push_back(get.returned_types[col_id.GetPrimaryIndex()]);
			}
			return;
		}
		types = get.returned_types;
		return;
	}
	default:
		throw NotImplementedException("Unsupported operator for Altertable output type extraction");
	}
}

static bool PlanUsesOnlyAltertableScans(LogicalOperator &op, vector<AltertableBindData *> &sources) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		if (!AltertableLimitPushdownOptimizer::IsAltertableScan(op)) {
			return false;
		}
		auto &bind_data = op.Cast<LogicalGet>().bind_data->Cast<AltertableBindData>();
		if (!bind_data.sql.empty()) {
			return false;
		}
		sources.push_back(&bind_data);
		return true;
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		return false;
	default:
		if (op.children.empty()) {
			return false;
		}
		for (auto &child : op.children) {
			if (!PlanUsesOnlyAltertableScans(*child, sources)) {
				return false;
			}
		}
		return true;
	}
}

static bool AltertableSourcesCompatible(const vector<AltertableBindData *> &sources) {
	if (sources.empty()) {
		return false;
	}
	for (idx_t i = 1; i < sources.size(); i++) {
		if (sources[i]->dsn != sources[0]->dsn || sources[i]->attach_path != sources[0]->attach_path) {
			return false;
		}
	}
	return true;
}

static bool RewriteAltertableTableRef(ClientContext &context, TableRef &ref, AltertableCatalog &target_catalog,
                                      const unordered_set<string> &visible_ctes);

static bool RewriteAltertableQueryNode(ClientContext &context, QueryNode &node, AltertableCatalog &target_catalog,
                                       const unordered_set<string> &visible_ctes);

static bool RewriteAltertableParsedExpression(ClientContext &context, ParsedExpression &expression,
                                              AltertableCatalog &target_catalog,
                                              const unordered_set<string> &visible_ctes) {
	if (expression.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &column_ref = expression.Cast<ColumnRefExpression>();
		if (column_ref.column_names.size() == 4) {
			auto db = DatabaseManager::Get(context).GetDatabase(context, column_ref.column_names[0]);
			if (!db || &db->GetCatalog() != &target_catalog) {
				return false;
			}
			if (target_catalog.GetRemoteCatalog().empty()) {
				column_ref.column_names.erase(column_ref.column_names.begin());
			} else {
				column_ref.column_names[0] = target_catalog.GetRemoteCatalog();
			}
		}
		return true;
	}
	bool result = true;
	if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expression.Cast<SubqueryExpression>();
		if (!subquery.subquery ||
		    !RewriteAltertableQueryNode(context, *subquery.subquery->node, target_catalog, visible_ctes)) {
			result = false;
		}
	}
	try {
		ParsedExpressionIterator::EnumerateChildren(expression, [&](unique_ptr<ParsedExpression> &child) {
			if (result && child && !RewriteAltertableParsedExpression(context, *child, target_catalog, visible_ctes)) {
				result = false;
			}
		});
	} catch (NotImplementedException &) {
		return false;
	}
	return result;
}

static bool RewriteAltertableTableRef(ClientContext &context, TableRef &ref, AltertableCatalog &target_catalog,
                                      const unordered_set<string> &visible_ctes) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		if (base.catalog_name == INVALID_CATALOG && visible_ctes.find(base.table_name) != visible_ctes.end()) {
			return true;
		}
		EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, base.table_name);
		auto entry =
		    Catalog::GetEntry(context, base.catalog_name, base.schema_name, lookup_info, OnEntryNotFound::RETURN_NULL);
		if (!entry || &entry->ParentCatalog() != &target_catalog) {
			return false;
		}
		base.catalog_name = target_catalog.GetRemoteCatalog();
		return true;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		if (!RewriteAltertableTableRef(context, *join.left, target_catalog, visible_ctes) ||
		    !RewriteAltertableTableRef(context, *join.right, target_catalog, visible_ctes)) {
			return false;
		}
		return !join.condition ||
		       RewriteAltertableParsedExpression(context, *join.condition, target_catalog, visible_ctes);
	}
	case TableReferenceType::SUBQUERY:
		return RewriteAltertableQueryNode(context, *ref.Cast<SubqueryRef>().subquery->node, target_catalog,
		                                  visible_ctes);
	case TableReferenceType::EXPRESSION_LIST: {
		auto &expression_list = ref.Cast<ExpressionListRef>();
		for (auto &row : expression_list.values) {
			for (auto &expression : row) {
				if (!RewriteAltertableParsedExpression(context, *expression, target_catalog, visible_ctes)) {
					return false;
				}
			}
		}
		return true;
	}
	case TableReferenceType::EMPTY_FROM:
	case TableReferenceType::CTE:
		return true;
	default:
		return false;
	}
}

static bool RewriteAltertableQueryNode(ClientContext &context, QueryNode &node, AltertableCatalog &target_catalog,
                                       const unordered_set<string> &visible_ctes) {
	auto query_ctes = visible_ctes;
	for (auto &cte : node.cte_map.map) {
		query_ctes.insert(cte.first);
	}
	for (auto &cte : node.cte_map.map) {
		if (!RewriteAltertableQueryNode(context, *cte.second->query->node, target_catalog, query_ctes)) {
			return false;
		}
	}
	for (auto &modifier : node.modifiers) {
		switch (modifier->type) {
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order = modifier->Cast<OrderModifier>();
			for (auto &order_by : order.orders) {
				if (!order_by.expression ||
				    !RewriteAltertableParsedExpression(context, *order_by.expression, target_catalog, query_ctes)) {
					return false;
				}
			}
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit = modifier->Cast<LimitModifier>();
			if ((limit.limit &&
			     !RewriteAltertableParsedExpression(context, *limit.limit, target_catalog, query_ctes)) ||
			    (limit.offset &&
			     !RewriteAltertableParsedExpression(context, *limit.offset, target_catalog, query_ctes))) {
				return false;
			}
			break;
		}
		case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
			auto &limit_percent = modifier->Cast<LimitPercentModifier>();
			if ((limit_percent.limit &&
			     !RewriteAltertableParsedExpression(context, *limit_percent.limit, target_catalog, query_ctes)) ||
			    (limit_percent.offset &&
			     !RewriteAltertableParsedExpression(context, *limit_percent.offset, target_catalog, query_ctes))) {
				return false;
			}
			break;
		}
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct = modifier->Cast<DistinctModifier>();
			for (auto &expression : distinct.distinct_on_targets) {
				if (!RewriteAltertableParsedExpression(context, *expression, target_catalog, query_ctes)) {
					return false;
				}
			}
			break;
		}
		default:
			return false;
		}
	}
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		if (select.from_table && !RewriteAltertableTableRef(context, *select.from_table, target_catalog, query_ctes)) {
			return false;
		}
		for (auto &expression : select.select_list) {
			if (!RewriteAltertableParsedExpression(context, *expression, target_catalog, query_ctes)) {
				return false;
			}
		}
		if (select.where_clause &&
		    !RewriteAltertableParsedExpression(context, *select.where_clause, target_catalog, query_ctes)) {
			return false;
		}
		if (select.having && !RewriteAltertableParsedExpression(context, *select.having, target_catalog, query_ctes)) {
			return false;
		}
		if (select.qualify &&
		    !RewriteAltertableParsedExpression(context, *select.qualify, target_catalog, query_ctes)) {
			return false;
		}
		return true;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop = node.Cast<SetOperationNode>();
		for (auto &child : setop.children) {
			if (!RewriteAltertableQueryNode(context, *child, target_catalog, query_ctes)) {
				return false;
			}
		}
		return true;
	}
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &recursive_cte = node.Cast<RecursiveCTENode>();
		return RewriteAltertableQueryNode(context, *recursive_cte.left, target_catalog, query_ctes) &&
		       RewriteAltertableQueryNode(context, *recursive_cte.right, target_catalog, query_ctes);
	}
	default:
		return false;
	}
}

static bool RewriteAltertableUpdate(ClientContext &context, UpdateStatement &statement,
                                    AltertableCatalog &target_catalog) {
	unordered_set<string> statement_ctes;
	for (auto &cte : statement.cte_map.map) {
		statement_ctes.insert(cte.first);
	}
	for (auto &cte : statement.cte_map.map) {
		if (!RewriteAltertableQueryNode(context, *cte.second->query->node, target_catalog, statement_ctes)) {
			return false;
		}
	}
	if (!RewriteAltertableTableRef(context, *statement.table, target_catalog, statement_ctes)) {
		return false;
	}
	if (statement.from_table &&
	    !RewriteAltertableTableRef(context, *statement.from_table, target_catalog, statement_ctes)) {
		return false;
	}
	for (auto &expression : statement.set_info->expressions) {
		if (!RewriteAltertableParsedExpression(context, *expression, target_catalog, statement_ctes)) {
			return false;
		}
	}
	if (statement.set_info->condition &&
	    !RewriteAltertableParsedExpression(context, *statement.set_info->condition, target_catalog, statement_ctes)) {
		return false;
	}
	for (auto &expression : statement.returning_list) {
		if (!RewriteAltertableParsedExpression(context, *expression, target_catalog, statement_ctes)) {
			return false;
		}
	}
	return true;
}

static bool RewriteAltertableDelete(ClientContext &context, DeleteStatement &statement,
                                    AltertableCatalog &target_catalog) {
	unordered_set<string> statement_ctes;
	for (auto &cte : statement.cte_map.map) {
		statement_ctes.insert(cte.first);
	}
	for (auto &cte : statement.cte_map.map) {
		if (!RewriteAltertableQueryNode(context, *cte.second->query->node, target_catalog, statement_ctes)) {
			return false;
		}
	}
	if (!RewriteAltertableTableRef(context, *statement.table, target_catalog, statement_ctes)) {
		return false;
	}
	for (auto &using_clause : statement.using_clauses) {
		if (!RewriteAltertableTableRef(context, *using_clause, target_catalog, statement_ctes)) {
			return false;
		}
	}
	if (statement.condition &&
	    !RewriteAltertableParsedExpression(context, *statement.condition, target_catalog, statement_ctes)) {
		return false;
	}
	for (auto &expression : statement.returning_list) {
		if (!RewriteAltertableParsedExpression(context, *expression, target_catalog, statement_ctes)) {
			return false;
		}
	}
	return true;
}

static string NormalizeDeparsedSQL(string sql) {
	return StringUtil::Replace(std::move(sql), "count_star()", "COUNT(*)");
}

static bool QueryContainsOffset(const string &query) {
	return StringUtil::Contains(StringUtil::Lower(query), " offset ");
}

static unique_ptr<SQLStatement> ExtractAltertableStatement(ClientContext &context) {
	const auto &query = context.GetCurrentQuery();
	if (query.empty()) {
		return nullptr;
	}
	Parser parser;
	parser.ParseQuery(query);
	if (parser.statements.size() != 1) {
		return nullptr;
	}
	auto copied = parser.statements[0]->Copy();
	if (copied->type == StatementType::EXPLAIN_STATEMENT) {
		auto &explain = copied->Cast<ExplainStatement>();
		if (!explain.stmt) {
			return nullptr;
		}
		return explain.stmt->Copy();
	}
	return copied;
}

static unique_ptr<SelectStatement> ExtractAltertableSelectStatement(ClientContext &context) {
	auto statement = ExtractAltertableStatement(context);
	if (!statement || statement->type != StatementType::SELECT_STATEMENT) {
		return nullptr;
	}
	return unique_ptr_cast<SQLStatement, SelectStatement>(std::move(statement));
}

static bool ReplacePlanWithRemoteScan(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                      AltertableBindData &source, string sql, vector<string> names,
                                      vector<LogicalType> types) {
	auto output_bindings = plan->GetColumnBindings();
	idx_t table_index = 0;
	if (!output_bindings.empty()) {
		table_index = output_bindings[0].table_index;
	}

	auto bind_data = make_uniq<AltertableBindData>(context);
	bind_data->dsn = source.dsn;
	bind_data->attach_path = source.attach_path;
	bind_data->catalog_name = source.catalog_name;
	bind_data->schema_name = source.schema_name;
	bind_data->table_name = source.table_name;
	bind_data->sql = std::move(sql);
	bind_data->names = names;
	bind_data->types = types;
	bind_data->max_threads = 1;
	if (source.GetCatalog()) {
		bind_data->SetCatalog(*source.GetCatalog());
	}

	auto function = AltertableScanFunction();
	function.projection_pushdown = false;
	function.filter_pushdown = false;
	auto logical_get =
	    make_uniq<LogicalGet>(table_index, function, std::move(bind_data), std::move(types), std::move(names));
	for (idx_t col = 0; col < logical_get->returned_types.size(); col++) {
		logical_get->AddColumnId(col);
	}
	plan = std::move(logical_get);
	return true;
}

static bool TryPushDeparsedQuery(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	if (plan->type == LogicalOperatorType::LOGICAL_EXPLAIN) {
		return false;
	}
	if (QueryContainsOffset(context.GetCurrentQuery())) {
		return false;
	}
	vector<AltertableBindData *> sources;
	if (!PlanUsesOnlyAltertableScans(*plan, sources) || !AltertableSourcesCompatible(sources)) {
		return false;
	}

	auto select = ExtractAltertableSelectStatement(context);
	if (!select || !sources[0]->GetCatalog()) {
		return false;
	}
	unordered_set<string> no_ctes;
	if (!RewriteAltertableQueryNode(context, *select->node, *sources[0]->GetCatalog(), no_ctes)) {
		return false;
	}

	vector<string> names;
	vector<LogicalType> types;
	try {
		types = plan->types;
		if (types.empty()) {
			ExtractPlanOutputTypes(*plan, types);
		}
		ExtractPlanOutputNames(*plan, names);
	} catch (NotImplementedException &) {
		return false;
	}
	if (types.empty() || names.empty() || types.size() != names.size()) {
		return false;
	}

	return ReplacePlanWithRemoteScan(context, plan, *sources[0], NormalizeDeparsedSQL(select->ToString()),
	                                 std::move(names), std::move(types));
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
			string comparison_operator;
			if (!TryGetAltertableComparisonOperator(comparison.type, comparison_operator)) {
				throw NotImplementedException("Unsupported comparison for Altertable pushdown");
			}
			return "(" + RenderExpression(*comparison.left, bindings) + " " + comparison_operator + " " +
			       RenderExpression(*comparison.right, bindings) + ")";
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
		case ExpressionClass::BOUND_BETWEEN: {
			auto &between = expr.Cast<BoundBetweenExpression>();
			string lower_op = between.lower_inclusive ? " >= " : " > ";
			string upper_op = between.upper_inclusive ? " <= " : " < ";
			auto input = RenderExpression(*between.input, bindings);
			return "(" + input + lower_op + RenderExpression(*between.lower, bindings) + " AND " + input + upper_op +
			       RenderExpression(*between.upper, bindings) + ")";
		}
		case ExpressionClass::BOUND_AGGREGATE:
			return RenderAggregate(expr, bindings);
		default:
			throw NotImplementedException("Unsupported expression for Altertable pushdown");
		}
	}

	idx_t next_alias = 0;

	string NextAlias() {
		return "__altertable_" + to_string(next_alias++);
	}

	string RenderAggregateOrderBy(const BoundOrderModifier &order_bys, const unordered_map<string, string> &bindings) {
		string result;
		for (idx_t i = 0; i < order_bys.orders.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += RenderExpression(*order_bys.orders[i].expression, bindings);
			result += order_bys.orders[i].GetOrderModifier();
		}
		return result;
	}

	string RenderAggregate(Expression &expr, const unordered_map<string, string> &bindings) {
		auto &aggregate = expr.Cast<BoundAggregateExpression>();
		if (aggregate.function.name == "count_star") {
			return "COUNT(*)";
		}

		string result = aggregate.function.name;
		result += "(";
		if (aggregate.IsDistinct()) {
			result += "DISTINCT ";
		}
		vector<string> children;
		for (auto &child : aggregate.children) {
			children.push_back(RenderExpression(*child, bindings));
		}
		result += StringUtil::Join(children, ", ");
		if (aggregate.order_bys && !aggregate.order_bys->orders.empty()) {
			if (aggregate.children.empty()) {
				result += ") WITHIN GROUP (";
			}
			result += " ORDER BY ";
			result += RenderAggregateOrderBy(*aggregate.order_bys, bindings);
		}
		result += ")";
		if (aggregate.filter) {
			result += " FILTER (WHERE " + RenderExpression(*aggregate.filter, bindings) + ")";
		}
		return result;
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
					result.types.push_back(LogicalType(LogicalTypeId::BIGINT));
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
			string predicate;
			if (!TryGetAltertablePredicate(*entry.second, bind_data.names[table_col_idx], predicate)) {
				throw NotImplementedException("Unsupported table filter for Altertable pushdown");
			}
			if (predicate.empty()) {
				continue;
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

class LogicalAltertableExecuteUpdate : public LogicalExtensionOperator {
public:
	LogicalAltertableExecuteUpdate(AltertableCatalog &catalog_p, string sql_p)
	    : catalog(catalog_p), sql(std::move(sql_p)) {
		types.emplace_back(LogicalTypeId::BIGINT);
	}

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		return planner.Make<AltertablePhysicalExecuteUpdate>(catalog, sql, estimated_cardinality);
	}

	string GetName() const override {
		return "ALTERTABLE_EXECUTE_UPDATE";
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override {
		InsertionOrderPreservingMap<string> result;
		result["Query"] = AltertableUtils::QueryFingerprint(sql);
		return result;
	}

	bool SupportSerialization() const override {
		return false;
	}

	idx_t EstimateCardinality(ClientContext &context) override {
		return 1;
	}

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalTypeId::BIGINT);
	}

private:
	AltertableCatalog &catalog;
	string sql;
};

static string AltertableInsertTarget(const AltertableCatalog &catalog, const AltertableTableEntry &table) {
	return AltertableUtils::QualifiedTableReference(catalog.GetRemoteCatalog(), table.schema.name, table.name);
}

static string AltertableInsertColumnList(const AltertableTableEntry &table) {
	vector<string> columns;
	for (auto &name : table.altertable_names) {
		columns.push_back(AltertableUtils::QuoteAltertableIdentifier(name));
	}
	return "(" + StringUtil::Join(columns, ", ") + ")";
}

static bool PlanUsesOnlyAltertableDMLSources(LogicalOperator &op, AltertableCatalog &target_catalog) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (!IsAltertableScanTableFunction(get.function) || !get.bind_data) {
			return false;
		}
		auto &bind_data = get.bind_data->Cast<AltertableBindData>();
		return bind_data.sql.empty() && bind_data.GetCatalog().get() == &target_catalog;
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
	case LogicalOperatorType::LOGICAL_CTE_REF:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		return true;
	default:
		if (op.children.empty()) {
			return false;
		}
		for (auto &child : op.children) {
			if (!PlanUsesOnlyAltertableDMLSources(*child, target_catalog)) {
				return false;
			}
		}
		return true;
	}
}

static bool FindAltertableDMLSource(LogicalOperator &op, AltertableCatalog &target_catalog,
                                    AltertableBindData *&source) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (!IsAltertableScanTableFunction(get.function) || !get.bind_data) {
			return false;
		}
		auto &bind_data = get.bind_data->Cast<AltertableBindData>();
		if (!bind_data.sql.empty() || bind_data.GetCatalog().get() != &target_catalog) {
			return false;
		}
		if (!source) {
			source = &bind_data;
			return true;
		}
		return source->dsn == bind_data.dsn && source->attach_path == bind_data.attach_path;
	}
	if (op.type == LogicalOperatorType::LOGICAL_EXPRESSION_GET || op.type == LogicalOperatorType::LOGICAL_DUMMY_SCAN ||
	    op.type == LogicalOperatorType::LOGICAL_EMPTY_RESULT || op.type == LogicalOperatorType::LOGICAL_CTE_REF ||
	    op.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return true;
	}
	if (op.children.empty()) {
		return false;
	}
	for (auto &child : op.children) {
		if (!FindAltertableDMLSource(*child, target_catalog, source)) {
			return false;
		}
	}
	return true;
}

bool AltertableLimitPushdownOptimizer::TryPushRemoteDMLReturning(ClientContext &context,
                                                                 unique_ptr<LogicalOperator> &plan) {
	if (plan->type != LogicalOperatorType::LOGICAL_PROJECTION || plan->children.size() != 1) {
		return false;
	}

	auto &projection = plan->Cast<LogicalProjection>();
	auto &dml = *plan->children[0];
	AltertableCatalog *target_catalog = nullptr;
	if (dml.type == LogicalOperatorType::LOGICAL_UPDATE) {
		auto &update = dml.Cast<LogicalUpdate>();
		if (!update.return_chunk || update.table.catalog.GetCatalogType() != "altertable") {
			return false;
		}
		target_catalog = &update.table.catalog.Cast<AltertableCatalog>();
	} else if (dml.type == LogicalOperatorType::LOGICAL_DELETE) {
		auto &del = dml.Cast<LogicalDelete>();
		if (!del.return_chunk || del.table.catalog.GetCatalogType() != "altertable") {
			return false;
		}
		target_catalog = &del.table.catalog.Cast<AltertableCatalog>();
	} else {
		return false;
	}

	if (target_catalog->access_mode == AccessMode::READ_ONLY) {
		throw BinderException("Cannot modify read-only Altertable database");
	}
	AltertableBindData *source = nullptr;
	if (dml.children.size() != 1 || !FindAltertableDMLSource(*dml.children[0], *target_catalog, source) || !source) {
		return false;
	}

	auto statement = ExtractAltertableStatement(context);
	if (!statement) {
		return false;
	}
	bool rewritten = false;
	if (dml.type == LogicalOperatorType::LOGICAL_UPDATE && statement->type == StatementType::UPDATE_STATEMENT) {
		rewritten = RewriteAltertableUpdate(context, statement->Cast<UpdateStatement>(), *target_catalog);
	} else if (dml.type == LogicalOperatorType::LOGICAL_DELETE && statement->type == StatementType::DELETE_STATEMENT) {
		rewritten = RewriteAltertableDelete(context, statement->Cast<DeleteStatement>(), *target_catalog);
	}
	if (!rewritten) {
		return false;
	}

	vector<string> names;
	names.reserve(projection.expressions.size());
	for (auto &expression : projection.expressions) {
		names.push_back(expression->GetName());
	}
	auto types = projection.types;
	if (types.empty()) {
		for (auto &expression : projection.expressions) {
			types.push_back(expression->return_type);
		}
	}
	if (types.empty() || names.empty() || types.size() != names.size()) {
		return false;
	}

	return ReplacePlanWithRemoteScan(context, plan, *source, NormalizeDeparsedSQL(statement->ToString()),
	                                 std::move(names), std::move(types));
}

bool AltertableLimitPushdownOptimizer::TryPushRemoteDML(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	AltertableCatalog *target_catalog = nullptr;
	if (plan->type == LogicalOperatorType::LOGICAL_UPDATE) {
		auto &update = plan->Cast<LogicalUpdate>();
		if (update.return_chunk || update.table.catalog.GetCatalogType() != "altertable") {
			return false;
		}
		target_catalog = &update.table.catalog.Cast<AltertableCatalog>();
	} else if (plan->type == LogicalOperatorType::LOGICAL_DELETE) {
		auto &del = plan->Cast<LogicalDelete>();
		if (del.return_chunk || del.table.catalog.GetCatalogType() != "altertable") {
			return false;
		}
		target_catalog = &del.table.catalog.Cast<AltertableCatalog>();
	} else {
		return false;
	}

	if (target_catalog->access_mode == AccessMode::READ_ONLY) {
		throw BinderException("Cannot modify read-only Altertable database");
	}
	if (plan->children.size() != 1 || !PlanUsesOnlyAltertableDMLSources(*plan->children[0], *target_catalog)) {
		return false;
	}

	auto statement = ExtractAltertableStatement(context);
	if (!statement) {
		return false;
	}
	bool rewritten = false;
	if (plan->type == LogicalOperatorType::LOGICAL_UPDATE && statement->type == StatementType::UPDATE_STATEMENT) {
		rewritten = RewriteAltertableUpdate(context, statement->Cast<UpdateStatement>(), *target_catalog);
	} else if (plan->type == LogicalOperatorType::LOGICAL_DELETE &&
	           statement->type == StatementType::DELETE_STATEMENT) {
		rewritten = RewriteAltertableDelete(context, statement->Cast<DeleteStatement>(), *target_catalog);
	}
	if (!rewritten) {
		return false;
	}

	plan = make_uniq<LogicalAltertableExecuteUpdate>(*target_catalog, NormalizeDeparsedSQL(statement->ToString()));
	return true;
}

bool AltertableLimitPushdownOptimizer::TryPushRemoteInsert(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	if (plan->type != LogicalOperatorType::LOGICAL_INSERT) {
		return false;
	}
	auto &insert = plan->Cast<LogicalInsert>();
	if (insert.table.catalog.GetCatalogType() != "altertable") {
		return false;
	}
	if (insert.children.size() != 1 || insert.return_chunk ||
	    insert.on_conflict_info.action_type != OnConflictAction::THROW) {
		return false;
	}
	if (!insert.column_index_map.empty()) {
		return false;
	}

	AltertableRemoteSQLBuilder builder(context);
	AltertableRemotePlan source_plan;
	if (!builder.TryBuild(*insert.children[0], source_plan)) {
		return false;
	}
	if (!source_plan.source_bind || !source_plan.source_bind->GetCatalog()) {
		return false;
	}

	auto &target_catalog = insert.table.catalog.Cast<AltertableCatalog>();
	if (source_plan.source_bind->GetCatalog().get() != &target_catalog) {
		return false;
	}

	auto &target_table = insert.table.Cast<AltertableTableEntry>();
	if (source_plan.names.size() != target_table.GetColumns().LogicalColumnCount()) {
		return false;
	}

	string sql = "INSERT INTO " + AltertableInsertTarget(target_catalog, target_table) + " " +
	             AltertableInsertColumnList(target_table) + " " + source_plan.sql;
	plan = make_uniq<LogicalAltertableExecuteUpdate>(target_catalog, std::move(sql));
	return true;
}

bool AltertableLimitPushdownOptimizer::TryPushWholeQuery(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	if (TryPushDeparsedQuery(context, plan)) {
		return true;
	}

	AltertableRemoteSQLBuilder builder(context);
	AltertableRemotePlan remote_plan;
	if (!builder.TryBuild(*plan, remote_plan)) {
		return false;
	}
	if (!remote_plan.source_bind) {
		return false;
	}

	return ReplacePlanWithRemoteScan(context, plan, *remote_plan.source_bind, std::move(remote_plan.sql),
	                                 std::move(remote_plan.names), std::move(remote_plan.types));
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
                                                         optional_idx parent_limit, bool is_root) {
	if (is_root && TryPushRemoteInsert(context, op)) {
		return;
	}
	if (is_root && TryPushRemoteDMLReturning(context, op)) {
		return;
	}
	if (is_root && TryPushRemoteDML(context, op)) {
		return;
	}
	// Whole-query pushdown must only run at the plan root. Running it on nested
	// scans (e.g. under a MARK join for large IN lists) replaces the scan with a
	// remote query that returns final projected columns while parent operators
	// still expect join/filter columns from the scan output.
	if (is_root && TryPushWholeQuery(context, op)) {
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
		// EXPLAIN wraps the real plan; pushdown must run on that child, not only on
		// the outermost operator (which would skip INSERT/SELECT under EXPLAIN).
		const bool child_is_root = op->type == LogicalOperatorType::LOGICAL_EXPLAIN;
		OptimizeRecursive(context, child, limit_for_children, child_is_root);
	}
}

void AltertableLimitPushdownOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	OptimizeRecursive(input.context, plan, optional_idx(), true);
}

OptimizerExtension CreateAltertableLimitPushdownOptimizer() {
	OptimizerExtension extension;
	extension.optimize_function = AltertableLimitPushdownOptimizer::Optimize;
	return extension;
}

} // namespace duckdb
