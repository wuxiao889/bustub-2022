//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  bool sorted_;
  std::vector<std::pair<Tuple, RID>> heap_;
  uint64_t cur_;
};
}  // namespace bustub

/*

For this last task, you are going to modify BusTub's optimizer to support converting top-N queries. Consider the
following query:

EXPLAIN SELECT * FROM __mock_table_1 ORDER BY colA LIMIT 10;
By default, BusTub will execute this query by (1) sort all data from the table (2) get the first 10 elements. This is
obviously inefficient, since the query only needs the smallest values. A smarter way of doing this is to dynamically
keep track of the smallest 10 elements so far. This is what the BusTub's TopNExecutor does.

You will need to modify the optimizer to support converting a query with ORDER BY + LIMIT clauses to use the
TopNExecutor. See OptimizeSortLimitAsTopN for more information.

An example of the optimized plan of this query:

 TopN { n=10, order_bys=[(Default, #0.0)]} | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
   MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)

Note: Think of what data structure can be used to track the top n elements (Andy mentioned it in the lecture). The
struct should hold at most k elements (where k is the number specified in LIMIT clause).

Note: Though we did not say it explicitly, the BusTub optimizer is a rule-based optimizer. Most optimizer rules
construct optimized plans in a bottom-up way.

Hint: At this point, your implementation should be able to pass SQLLogicTests #13 to #16. Integration-test-2 requires
you to use release mode to run.

*/
