#include "altertable_optimizer.hpp"
#include "altertable_scanner.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

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

	if (bind_data.limit.empty()) {
		bind_data.limit = " LIMIT " + to_string(limit_value);
		return;
	}

	auto existing = bind_data.limit;
	if (existing.find("LIMIT") == string::npos) {
		return;
	}
	auto pos = existing.find("LIMIT") + 6;
	idx_t existing_limit = std::stoull(existing.substr(pos));
	if (limit_value < existing_limit) {
		bind_data.limit = " LIMIT " + to_string(limit_value);
	}
}

static bool CanPushLimitThrough(LogicalOperatorType type) {
	switch (type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_LIMIT:
		return true;
	default:
		return false;
	}
}

void AltertableLimitPushdownOptimizer::OptimizeRecursive(ClientContext &context, unique_ptr<LogicalOperator> &op,
                                                         optional_idx parent_limit, bool is_root) {
	optional_idx current_limit = parent_limit;
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			idx_t limit_value = limit.limit_val.GetConstantValue();
			if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
				limit_value += limit.offset_val.GetConstantValue();
			}
			if (!current_limit.IsValid() || limit_value < current_limit.GetIndex()) {
				current_limit = limit_value;
			}
		}
	}

	if (current_limit.IsValid() && IsAltertableScan(*op)) {
		PushLimitIntoScan(*op, current_limit.GetIndex());
	}

	optional_idx limit_for_children;
	if (current_limit.IsValid() && CanPushLimitThrough(op->type)) {
		limit_for_children = current_limit;
	}

	for (auto &child : op->children) {
		OptimizeRecursive(context, child, limit_for_children, false);
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
