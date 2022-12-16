//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new NestedLoopJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The NestedLoop join plan to be executed
   * @param left_executor The child executor that produces tuple for the left side of join
   * @param right_executor The child executor that produces tuple for the right side of join
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced, not used by nested loop join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  auto NextBothSeq(Tuple *tuple, RID *rid) -> bool;
  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  auto GenerateValue(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                     const Schema &right_schema) -> std::vector<Value>;

  void AnotherLoop();

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_executor_;
  std::unique_ptr<AbstractExecutor> right_child_executor_;

  Tuple left_tuple_{};
  bool joined_;
  bool reorded_;
};

}  // namespace bustub

/*

1. You will need to implement inner join and left join for NestedLoopJoinExecutor by using the basic nested loop join
    algorithm mentioned in the lecture.
2. The output schema of this operator is all columns from the left table followed by
    all the columns from the right table.

3.  This executor should implement the simple nested loop join algorithm presented in Lecture 11.
      That is, for each tuple in the join's outer table, you should consider each tuple in the join's inner table,
      and emit an output tuple if the join predicate is satisfied.

HINT: You will want to make use of the predicate in the NestedLoopJoinPlanNode.
      In particular, take a look at AbstractExpression::EvaluateJoin, which handles the left tuple and right tuple and
their respective schemas. Note that this returns a Value, which could be false, true, or NULL. See FilterExecutor on how
to apply predicates on tuples.

// filter_executor.cpp
auto FilterExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto filter_expr = plan_->GetPredicate();

  while (true) {
    // Get the next tuple
    const auto status = child_executor_->Next(tuple, rid);

    if (!status) {
      return false;
    }

    auto value = filter_expr->Evaluate(tuple, child_executor_->GetOutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      return true;
    }
  }
}

BLOCK NESTED LOOP JOIN
foreach B - 2 pages pR ∈ R:
  foreach page pS ∈ S:
    foreach tuple r ∈ B - 2 pages:
      foreach tuple s ∈ ps:
        emit, if r and s match


bustub> EXPLAIN SELECT * FROM __mock_table_1, __mock_table_3 WHERE colA = colE;
=== BINDER ===
BoundSelect {
  table=BoundCrossProductRef { left=BoundBaseTableRef { table=__mock_table_1, oid=0 }, right=BoundBaseTableRef {
table=__mock_table_3, oid=2 } }, columns=[__mock_table_1.colA, __mock_table_1.colB, __mock_table_3.colE,
__mock_table_3.colF], groupBy=[], having=, where=(__mock_table_1.colA=__mock_table_3.colE), limit=, offset=,
  order_by=[],
  is_distinct=false,
  ctes=,
}

=== PLANNER ===
Projection { exprs=[#0.0, #0.1, #0.2, #0.3] } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER,
__mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR) Filter { predicate=(#0.0=#0.2) } |
(__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER, __mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR)
    NestedLoopJoin { type=Inner, predicate=true } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER,
__mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR) MockScan { table=__mock_table_1 } |
(__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER) MockScan { table=__mock_table_3 } |
(__mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR)
=== OPTIMIZER ===
HashJoin { type=Inner, left_key=#0.0, right_key=#0.0 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER,
__mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR) MockScan { table=__mock_table_1 } |
(__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER) MockScan { table=__mock_table_3 } |
(__mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR)

bustub> EXPLAIN SELECT * FROM __mock_table_1 INNER JOIN __mock_table_3 ON colA = colE;
=== BINDER ===
BoundSelect {
  table=BoundJoin { type=Inner, left=BoundBaseTableRef { table=__mock_table_1, oid=0 }, right=BoundBaseTableRef {
table=__mock_table_3, oid=2 }, condition=(__mock_table_1.colA=__mock_table_3.colE) }, columns=[__mock_table_1.colA,
__mock_table_1.colB, __mock_table_3.colE, __mock_table_3.colF], groupBy=[], having=, where=, limit=, offset=,
  order_by=[],
  is_distinct=false,
  ctes=,
}
=== PLANNER ===
Projection { exprs=[#0.0, #0.1, #0.2, #0.3] } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER,
__mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR) NestedLoopJoin { type=Inner, predicate=(#0.0=#1.0) } |
(__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER, __mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR)
    MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
    MockScan { table=__mock_table_3 } | (__mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR)
=== OPTIMIZER ===
HashJoin { type=Inner, left_key=#0.0, right_key=#0.0 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER,
__mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR) MockScan { table=__mock_table_1 } |
(__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER) MockScan { table=__mock_table_3 } |
(__mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR)

bustub> EXPLAIN SELECT * FROM __mock_table_1 LEFT OUTER JOIN __mock_table_3 ON colA = colE;
=== BINDER ===
BoundSelect {
  table=BoundJoin { type=Left, left=BoundBaseTableRef { table=__mock_table_1, oid=0 }, right=BoundBaseTableRef {
table=__mock_table_3, oid=2 }, condition=(__mock_table_1.colA=__mock_table_3.colE) }, columns=[__mock_table_1.colA,
__mock_table_1.colB, __mock_table_3.colE, __mock_table_3.colF], groupBy=[], having=, where=, limit=, offset=,
  order_by=[],
  is_distinct=false,
  ctes=,
}
=== PLANNER ===
Projection { exprs=[#0.0, #0.1, #0.2, #0.3] } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER,
__mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR) NestedLoopJoin { type=Left, predicate=(#0.0=#1.0) } |
(__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER, __mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR)
    MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
    MockScan { table=__mock_table_3 } | (__mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR)
=== OPTIMIZER ===
HashJoin { type=Left, left_key=#0.0, right_key=#0.0 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER,
__mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR) MockScan { table=__mock_table_1 } |
(__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER) MockScan { table=__mock_table_3 } |
(__mock_table_3.colE:INTEGER, __mock_table_3.colF:VARCHAR) bustub> EXPLAIN SELECT * FROM __mock_table_1, __mock_table_3
WHERE colA = colE;bustub> EXPLAIN SELECT * FROM __mock_table_1 INNER JOIN __mock_table_3 ON colA = colE;bustub> EXPLAIN
SELECT * FROM __mock_table_1 LEFT OUTER JOIN __mock_table_3 ON colA = colE;

*/
