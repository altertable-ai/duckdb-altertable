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
	// Check if this is an altertable_scan or altertable_scan_pushdown function
	return get.function.name == "altertable_scan" || get.function.name == "altertable_scan_pushdown";
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
