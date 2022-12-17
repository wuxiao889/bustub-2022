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
#include <cassert>
#include "catalog/catalog.h"
#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executor_context.h"
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
      table_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
      scaned_(false) {}

void IndexScanExecutor::Init() {
  auto *tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  cur_ = tree->GetBeginIterator();
  LockTable();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (scaned_) {
    return false;
  }

  auto *tree = static_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  assert(tree);
  auto end = tree->GetEndIterator();
  const auto &bpm = exec_ctx_->GetBufferPoolManager();
  const auto &txn = exec_ctx_->GetTransaction();

  TablePage *table_page{};

  if (plan_->filter_predicate_ != nullptr) {
    scaned_ = true;
    const auto *cmp_expr = static_cast<const ComparisonExpression *>(plan_->filter_predicate_.get());
    const auto *const_expr = static_cast<const ConstantValueExpression *>(cmp_expr->GetChildAt(1).get());
    std::vector<RID> result;
    Tuple key{{const_expr->val_}, &index_info_->key_schema_};
    index_info_->index_->ScanKey(key, &result, txn);
    // 只有一个
    for (auto result_rid : result) {
      LockRow(result_rid);
      table_page = reinterpret_cast<TablePage *>(bpm->FetchPage(result_rid.GetPageId())->GetData());
      table_page->GetTuple(result_rid, tuple, txn, exec_ctx_->GetLockManager());
      *rid = result_rid;
      bpm->UnpinPage(result_rid.GetPageId(), false);
      UnLockRow(result_rid);
      UnLockTable();
      return true;
    }
    UnLockTable();
    return false;
  }

  if (cur_ != end) {
    *rid = (*cur_).second;
    ++cur_;

    LockRow(*rid);
    table_page = reinterpret_cast<TablePage *>(bpm->FetchPage(rid->GetPageId())->GetData());
    table_page->GetTuple(*rid, tuple, txn, exec_ctx_->GetLockManager());
    bpm->UnpinPage(rid->GetPageId(), false);
    UnLockRow(*rid);

    return true;
  }

  UnLockTable();
  return false;
}

void IndexScanExecutor::LockTable() {
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &oid = table_info_->oid_;
  bool res = true;
  try {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        res = lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, oid);
      }
    }
    if (!res) {
      assert(txn->GetState() == TransactionState::ABORTED);
      throw ExecutionException("IndexScanExecutor::LockTable() lock fail");
    }
  } catch (TransactionAbortException &e) {
    assert(txn->GetState() == TransactionState::ABORTED);
    throw ExecutionException("IndexScanExecutor::LockTable() lock fail");
  }
}

void IndexScanExecutor::UnLockTable() {
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &oid = table_info_->oid_;
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->IsTableIntentionSharedLocked(oid)) {
      lock_mgr->UnlockTable(txn, oid);
    }
  }
}

void IndexScanExecutor::LockRow(const RID &rid) {
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &oid = table_info_->oid_;
  bool res = true;
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!txn->IsRowExclusiveLocked(oid, rid)) {
      res = lock_mgr->LockRow(txn, LockManager::LockMode::SHARED, oid, rid);
    }
  }
  if (!res) {
    txn->SetState(TransactionState::ABORTED);
    throw ExecutionException("IndexScanExecutor::Next() lock fail");
  }
}

void IndexScanExecutor::UnLockRow(const RID &rid) {
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &oid = table_info_->oid_;
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->IsRowSharedLocked(oid, rid)) {
      lock_mgr->UnlockRow(txn, oid, rid);
    }
  }
}

}  // namespace bustub
