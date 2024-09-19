#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_tokens.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"

namespace duckdb {
unique_ptr<LogicalOperator> FilterPushdown::PushdownUnnest(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_UNNEST);

	auto &unnest = op->Cast<LogicalUnnest>();

	// pushdown into UNNEST
	FilterPushdown child_pushdown(optimizer, convert_mark_joins);
	for (idx_t i = 0; i < filters.size(); i++) {
		auto &f = *filters[i];
		if (f.bindings.find(unnest.unnest_index) != f.bindings.end()) {
			// filter on unnest: cannot pushdown
			continue;
		}
		// no unnest! we can push this down
		// add the filter to the child node
		if (child_pushdown.AddFilter(std::move(f.filter)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
		// erase the filter from here
		filters.erase_at(i);
		i--;
	}
	child_pushdown.GenerateFilters();

	op->children[0] = child_pushdown.Rewrite(std::move(op->children[0]));
	return FinishPushdown(std::move(op));
}
} // namespace duckdb
