//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/table_page.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), index_info_(exec_ctx_->GetCatalog()->GetIndex(plan->index_oid_)) {}

void IndexScanExecutor::Init() {
  auto *tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  cur_ = tree->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto *tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  auto end = tree->GetEndIterator();
  auto *bpm = exec_ctx_->GetBufferPoolManager();
  TablePage *table_page{};
  if (cur_ != end) {
    *rid = (*cur_).second;
    table_page = reinterpret_cast<TablePage *>(bpm->FetchPage(rid->GetPageId())->GetData());
    // assert?
    table_page->GetTuple(*rid, tuple, exec_ctx_->GetTransaction(), exec_ctx_->GetLockManager());
    bpm->UnpinPage(rid->GetPageId(), false);
    ++cur_;
    return true;
  }
  return false;
}

}  // namespace bustub
