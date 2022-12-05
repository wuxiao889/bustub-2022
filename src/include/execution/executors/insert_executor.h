//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * InsertExecutor executes an insert on a table.
 * Inserted values are always pulled from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new InsertExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the insert */
  void Init() override;

  /**
   * Yield the number of rows inserted into the table.
   * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
   * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   *
   * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
   * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The insert plan node to be executed*/
  const InsertPlanNode *plan_;

  /** The exactly one child producing values to be inserted into the table. */
  std::unique_ptr<AbstractExecutor> child_executor_;
  
  bool inserted_{};
};

}  // namespace bustub

/*

bustub> EXPLAIN (o,s) INSERT INTO t1 VALUES (1, 'a'), (2, 'b');
 === OPTIMIZER ===
 Insert { table_oid=22 } | (__bustub_internal.insert_rows:INTEGER)
   Values { rows=2 } | (__values#0.0:INTEGER, __values#0.1:VARCHAR)

The InsertExecutor inserts tuples into a table and updates indexes.
1. It has exactly one child producing values to be inserted into the table. The planner will ensure values have the same
schema as the table.
2. The executor will produce a single tuple of an integer number as the output, indicating how many rows have been
inserted into the table, after allmrows are inserted.
3. Remember to update the index when inserting into the table, if it has an index associated with it.

Hint: You will need to lookup table information for the target of the insert during executor initialization. See the
System Catalog section below for additional information on accessing the catalog.
Catalog::GetTable() and Catalog::GetIndex().

Hint: You will need to update all indexes for the table into which tuples are inserted. See the Index Updates section
below for further details.

Hint: You will need to use the TableHeap class to perform table modifications.

Index Updates
For the table modification executors (InsertExecutor and DeleteExecutor) you must modify all indexes for the table
targeted by the operation. You may find the Catalog::GetTableIndexes() function useful for querying all of the indexes
defined for a particular table. Once you have the IndexInfo instance for each of the table's indexes, you can invoke
index modification operations on the underlying index structure.

In this project, we use your implementation of B+ Tree Index from Project 2 as the underlying data structure for all
index operations. Therefore, successful completion of this project relies on a working implementation of the B+ Tree
Index.
*/
