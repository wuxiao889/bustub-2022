//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// projection_executor.h
//
// Identification: src/include/execution/executors/projection_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The ProjectionExecutor executor executes a projection.
 * A projection plan node is used to do computation over an input. It will always have exactly one child.
 */
class ProjectionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new ProjectionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The projection plan to be executed
   */
  ProjectionExecutor(ExecutorContext *exec_ctx, const ProjectionPlanNode *plan,
                     std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the projection */
  void Init() override;

  /**
   * Yield the next tuple from the projection.
   * @param[out] tuple The next tuple produced by the projection
   * @param[out] rid The next tuple RID produced by the projection
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the projection plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The projection plan node to be executed */
  const ProjectionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
};
}  // namespace bustub


/*
bustub> EXPLAIN SELECT 1 + 2;
 === BINDER ===
 BoundSelect {
   table=<empty>,
   columns=[(1+2)],
   groupBy=[],
   having=,
   where=,
   limit=,
   offset=,
   order_by=[],
   is_distinct=false,
   ctes=,
 }
 === PLANNER ===
 Projection { exprs=[(1+2)] } | (__unnamed#0:INTEGER)
   Values { rows=1 } | ()
 === OPTIMIZER ===
 Projection { exprs=[(1+2)] } | (__unnamed#0:INTEGER)
   Values { rows=1 } | ()

bustub> EXPLAIN SELECT colA FROM __mock_table_1;
 === BINDER ===
 BoundSelect {
   table=BoundBaseTableRef { table=__mock_table_1, oid=0 },
   columns=[__mock_table_1.colA],
   groupBy=[],
   having=,
   where=,
   limit=,
   offset=,
   order_by=[],
   is_distinct=false,
   ctes=,
 }
 === PLANNER ===
 Projection { exprs=[#0.0] } | (__mock_table_1.colA:INTEGER)
   MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
 === OPTIMIZER ===
 Projection { exprs=[#0.0] } | (__mock_table_1.colA:INTEGER)
   MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)

bustub> EXPLAIN SELECT colA + colB AS a, 1 + 2 AS b FROM __mock_table_1;
 === BINDER ===
 BoundSelect {
   table=BoundBaseTableRef { table=__mock_table_1, oid=0 },
   columns=[((__mock_table_1.colA+__mock_table_1.colB) as a), ((1+2) as b)],
   groupBy=[],
   having=,
   where=,
   limit=,
   offset=,
   order_by=[],
   is_distinct=false,
   ctes=,
 }
 === PLANNER ===
 Projection { exprs=[(#0.0+#0.1), (1+2)] } | (a:INTEGER, b:INTEGER)
   MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
 === OPTIMIZER ===
 Projection { exprs=[(#0.0+#0.1), (1+2)] } | (a:INTEGER, b:INTEGER)
   MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)

*/