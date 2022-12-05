//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      cur_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->Begin(exec_ctx->GetTransaction())),
      end_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->End()) {}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_ != end_) {
    *tuple = *cur_;
    *rid = cur_->GetRid();
    cur_++;
    return true;
  }
  return false;
}

}  // namespace bustub
