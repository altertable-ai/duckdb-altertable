//===----------------------------------------------------------------------===//
//                         DuckDB
//
// altertable_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class AltertableLimitPushdownOptimizer {
public:
	//! The optimize function that pushes down LIMIT into Altertable scans
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

	//! Check if this is an Altertable scan function
	static bool IsAltertableScan(LogicalOperator &op);

private:
	//! Recursively traverse the plan and push down limits
	static void OptimizeRecursive(ClientContext &context, unique_ptr<LogicalOperator> &op,
	                              optional_idx parent_limit = optional_idx());

	//! Try to replace a fully Altertable-backed query with one remote SQL scan
	static bool TryPushWholeQuery(ClientContext &context, unique_ptr<LogicalOperator> &plan);

	//! Try to push the limit into the Altertable bind data
	static void PushLimitIntoScan(LogicalOperator &scan_op, idx_t limit_value);
};

//! Create an optimizer extension for Altertable limit pushdown
OptimizerExtension CreateAltertableLimitPushdownOptimizer();

} // namespace duckdb
