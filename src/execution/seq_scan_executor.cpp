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
#include "common/config.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      cur_(nullptr, {}, nullptr),
      cur_page_id_(INVALID_PAGE_ID) {}

void SeqScanExecutor::Init() {
  cur_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  cur_page_id_ = INVALID_PAGE_ID;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto end = table_info_->table_->End();
  const auto &bpm = exec_ctx_->GetBufferPoolManager();
  while (cur_ != end) {
    // TODO(wxx) optimize lurk
    // lruk实现有问题，
    const auto page_id = cur_->GetRid().GetPageId();
    if (page_id != cur_page_id_) {
      if (cur_page_id_ != INVALID_PAGE_ID) {
        bpm->UnpinPage(cur_page_id_, false);
      }
      cur_page_id_ = page_id;
      bpm->FetchPage(cur_page_id_);
    }
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
  if (cur_page_id_ != INVALID_PAGE_ID) {
    bpm->UnpinPage(cur_page_id_, false);
  }
  return false;
}

}  // namespace bustub
