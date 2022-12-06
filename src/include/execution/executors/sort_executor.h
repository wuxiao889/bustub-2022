//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<std::pair<Tuple, RID>> result_;
  uint64_t cur_;
  bool sorted_;
};

}  // namespace bustub

/*

If there is an index over a table, the query processing layer will automatically pick it for sorting. In other cases,
you will need a special sort executor to do this.

For all order by clauses, we assume every sort key will only appear once. You do not need to worry about ties in
sorting.

This plan node does not change schema (i.e., the output schema is the same as the input schema).

You can extract sort keys from order_bys, and then use std::sort with a custom comparator to sort all tuples from the
child. You can assume that all entries in a table can fit in memory.

If the query does not include a sort direction in the ORDER BY clause (i.e., ASC, DESC), then the sort mode will be
default (which is ASC).


bustub> EXPLAIN SELECT * FROM __mock_table_1 ORDER BY colA ASC, colB DESC;
=== BINDER ===
BoundSelect {
  table=BoundBaseTableRef { table=__mock_table_1, oid=0 },
  columns=[__mock_table_1.colA, __mock_table_1.colB],
  groupBy=[],
  having=,
  where=,
  limit=,
  offset=,
  order_by=[BoundOrderBy { type=Ascending, expr=__mock_table_1.colA }, BoundOrderBy { type=Descending,
expr=__mock_table_1.colB }], is_distinct=false, ctes=,
}

=== PLANNER ===
Sort { order_bys=[(Ascending, #0.0), (Descending, #0.1)] } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
  Projection { exprs=[#0.0, #0.1] } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
    MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
=== OPTIMIZER ===
Sort { order_bys=[(Ascending, #0.0), (Descending, #0.1)] } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
  MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)bustub> EXPLAIN SELECT *
FROM __mock_table_1 ORDER BY colA ASC, colB DESC;
*/