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
      scaned_(false) {}

void IndexScanExecutor::Init() {
  auto *tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  cur_ = tree->GetBeginIterator();

  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &txn = exec_ctx_->GetTransaction();
  const auto oid = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)->oid_;
  LockTable(lock_mgr, txn, oid);
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (scaned_) {
    return false;
  }

  auto *tree = static_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  assert(tree);
  auto end = tree->GetEndIterator();
  const auto &bpm = exec_ctx_->GetBufferPoolManager();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &txn = exec_ctx_->GetTransaction();
  const auto oid = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)->oid_;
  // auto *ctx = GetExecutorContext();
  // auto *table_info = ctx->GetCatalog()->GetTable(index_info_->table_name_);

  TablePage *table_page{};

  if (plan_->filter_predicate_ != nullptr) {
    const auto *cmp_expr = static_cast<const ComparisonExpression *>(plan_->filter_predicate_.get());
    const auto *const_expr = static_cast<const ConstantValueExpression *>(cmp_expr->GetChildAt(1).get());
    std::vector<RID> result;
    Tuple key{{const_expr->val_}, &index_info_->key_schema_};
    index_info_->index_->ScanKey(key, &result, txn);
    // 其实只有一个
    scaned_ = true;
    for (auto result_rid : result) {
      LockRow(lock_mgr, txn, oid, result_rid);

      table_page = reinterpret_cast<TablePage *>(bpm->FetchPage(result_rid.GetPageId())->GetData());
      table_page->GetTuple(result_rid, tuple, txn, exec_ctx_->GetLockManager());
      *rid = result_rid;
      bpm->UnpinPage(result_rid.GetPageId(), false);

      UnLockRow(lock_mgr, txn, oid, result_rid);
      UnLockTable(lock_mgr, txn, oid);
      return true;
    }

    UnLockTable(lock_mgr, txn, oid);
    return false;
  }

  if (cur_ != end) {
    *rid = (*cur_).second;
    ++cur_;

    LockRow(lock_mgr, txn, oid, *rid);

    table_page = reinterpret_cast<TablePage *>(bpm->FetchPage(rid->GetPageId())->GetData());
    table_page->GetTuple(*rid, tuple, txn, exec_ctx_->GetLockManager());
    bpm->UnpinPage(rid->GetPageId(), false);

    UnLockRow(lock_mgr, txn, oid, *rid);

    return true;
  }

  UnLockTable(lock_mgr, txn, oid);
  return false;
}

void IndexScanExecutor::LockTable(LockManager *lock_mgr, Transaction *txn, table_oid_t oid) {
  bool res = true;
  if (!txn->IsTableExclusiveLocked(oid)) {
    if (plan_->filter_predicate_ != nullptr) {
      if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        // S SIX IX -> IS
        if (!txn->IsTableSharedLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
            !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
          res = lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, oid);
        }
      }
    } else {
      if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        // IX SIX -> S
        if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
          res = lock_mgr->LockTable(txn, LockManager::LockMode::SHARED, oid);
        }
      }
    }
  }
  if (!res) {
    txn->SetState(TransactionState::ABORTED);
    throw ExecutionException("IndexScanExecutor::Init() lock fail");
  }
}

void IndexScanExecutor::UnLockTable(LockManager *lock_mgr, Transaction *txn, table_oid_t oid) {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->IsTableSharedLocked(oid) || txn->IsTableIntentionSharedLocked(oid) ||
        txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      lock_mgr->UnlockTable(txn, oid);
    }
  }
}

void IndexScanExecutor::LockRow(LockManager *lock_mgr, Transaction *txn, table_oid_t oid, const RID &rid) {
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

void IndexScanExecutor::UnLockRow(LockManager *lock_mgr, Transaction *txn, table_oid_t oid, const RID &rid) {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->IsRowSharedLocked(oid, rid)) {
      lock_mgr->UnlockRow(txn, oid, rid);
    }
  }
}

}  // namespace bustub
