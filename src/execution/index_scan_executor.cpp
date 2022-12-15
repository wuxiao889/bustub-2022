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
#include "catalog/catalog.h"
#include "common/config.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "storage/index/index_iterator.h"
#include "storage/page/table_page.h"
#include "type/value.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx_->GetCatalog()->GetIndex(plan->index_oid_)),
      scaned_(false) {}

void IndexScanExecutor::Init() {
  auto *tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  cur_ = tree->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (scaned_) {
    return false;
  }

  auto *tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  auto end = tree->GetEndIterator();
  auto *bpm = exec_ctx_->GetBufferPoolManager();
  auto *txn = exec_ctx_->GetTransaction();
  // auto *ctx = GetExecutorContext();
  // auto *table_info = ctx->GetCatalog()->GetTable(index_info_->table_name_);

  TablePage *table_page{};

  if (plan_->filter_predicate_ != nullptr) {
    const auto *cmp_expr = static_cast<const ComparisonExpression *>(plan_->filter_predicate_.get());
    const auto *const_expr = static_cast<const ConstantValueExpression *>(cmp_expr->GetChildAt(1).get());
    std::vector<RID> result;
    Tuple key{{const_expr->val_}, &index_info_->key_schema_};
    tree->ScanKey(key, &result, txn);
    // 其实只有一个
    scaned_ = true;
    for (auto result_rid : result) {
      table_page = reinterpret_cast<TablePage *>(bpm->FetchPage(result_rid.GetPageId())->GetData());
      table_page->GetTuple(result_rid, tuple, txn, exec_ctx_->GetLockManager());
      *rid = result_rid;
      bpm->UnpinPage(result_rid.GetPageId(), false);
      return true;
    }
    return false;
  }

  if (cur_ != end) {
    *rid = (*cur_).second;
    table_page = reinterpret_cast<TablePage *>(bpm->FetchPage(rid->GetPageId())->GetData());
    // assert?
    table_page->GetTuple(*rid, tuple, txn, exec_ctx_->GetLockManager());
    bpm->UnpinPage(rid->GetPageId(), false);
    ++cur_;
    return true;
  }
  return false;
}

}  // namespace bustub
