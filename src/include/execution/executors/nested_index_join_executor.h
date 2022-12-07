//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 */
class NestIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new nested index join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the nested index join plan node
   * @param child_executor the outer table
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  auto GenerateValue(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                     const Schema &right_schema) -> std::vector<Value>;

 private:
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
};
}  // namespace bustub

/*
The DBMS will use NestedIndexJoinPlanNode if the query contains a join with an equi-condition and the right side of the
join has an index over the condition.

In the plan phase, the query is planned as a NestedLoopJoin of two tables. The optimizer identifies that the right side
of the join (SeqScan t2) has an index on column v3, and the join condition is an equi-condition v1 = v3. This means that
for all tuples from the left side, the system can use the key v1 to query the index t2v3 to produce the join result.

The schema of NestedIndexJoin is all columns from the left table (child, outer) and then from the right table (index,
inner).

This executor will have only one child that propagates tuples corresponding to the outer table of the join.
For each of these tuples, you will need to find the corresponding tuple in the inner table that matches the index key
given by utilizing the index in the catalog.

Hint: You will want to fetch the tuple from the outer table, construct the index probe key by using key_predicate, and
then look up the RID in the index to retrieve the corresponding tuple for the inner table.

Note: We will provide all test cases on Gradescope AS-IS. You only need to pass the tests. Do not think of strange edge
cases of NULLs (e.g., NULLs in group by and in indices)

CREATE TABLE t1(v1 int, v2 int);
CREATE TABLE t2(v3 int, v4 int);
CREATE INDEX t2v3 on t2(v3);
EXPLAIN SELECT * FROM t1 INNER JOIN t2 ON v1 = v3;
=== PLANNER ===
Projection { exprs=[#0.0, #0.1, #0.2, #0.3] } | (t1.v1:INTEGER, t1.v2:INTEGER, t2.v3:INTEGER, t2.v4:INTEGER)
  NestedLoopJoin { predicate=#0.0=#1.0 } | (t1.v1:INTEGER, t1.v2:INTEGER, t2.v3:INTEGER, t2.v4:INTEGER)
    SeqScan { table=t1 } | (t1.v1:INTEGER, t1.v2:INTEGER)
    SeqScan { table=t2 } | (t2.v3:INTEGER, t2.v4:INTEGER)
=== OPTIMIZER ===
NestedIndexJoin { key_predicate=#0.0, index=t2v3, index_table=t2 }
  SeqScan { table=t1 }

*/
