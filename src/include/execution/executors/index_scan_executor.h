//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/catalog.h"
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  /*
  The IndexScanExecutor iterates over an index to retrieve RIDs for tuples. The operator then uses these RIDs to
  retrieve their tuples in the corresponding table. It then emits these tuples one-at-a-time.
  */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  IndexInfo *index_info_;
  IndexIterator<bustub::GenericKey<4>, bustub::RID, bustub::GenericComparator<4>> cur_;
};
}  // namespace bustub

/*
You can then construct index iterator from the index object, scan through all the keys and tuple IDs, lookup the tuple
from the table heap, and emit all tuples in the index key's order as the output of the executor. BusTub only supports
indexes with a single, unique integer column. There will not be duplicate keys in the test cases.
*/
