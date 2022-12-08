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
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      cur_(nullptr, {}, nullptr) {}

void SeqScanExecutor::Init() { cur_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto end = table_info_->table_->End();
  while (cur_ != end) {
    *tuple = *cur_;
    *rid = cur_->GetRid();
    cur_++;
    if (plan_->filter_predicate_ != nullptr) {
      const auto value = plan_->filter_predicate_->Evaluate(tuple, plan_->OutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        continue;
      }
    }
    // fmt::print("SeqScanExecutor::Next {}\n", tuple->ToString(&plan_->OutputSchema()));
    return true;
  }
  return false;
}

}  // namespace bustub
