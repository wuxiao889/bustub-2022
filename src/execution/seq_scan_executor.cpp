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
#include <cassert>
#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      cur_(nullptr, {}, nullptr),
      cur_page_id_(INVALID_PAGE_ID) {}

void SeqScanExecutor::Init() {
  const auto &txn = exec_ctx_->GetTransaction();
  cur_ = table_info_->table_->Begin(txn);
  cur_page_id_ = INVALID_PAGE_ID;
  LockTable();
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

    *rid = cur_->GetRid();

    LockRow(*rid);
    *tuple = *cur_;
    UnLockRow(*rid);

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

  UnLockTable();
  return false;
}

void SeqScanExecutor::LockTable() {
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto oid = plan_->table_oid_;
  bool res = true;

  try {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        res = lock_mgr->LockTable(txn, LockManager::LockMode::SHARED, oid);
      }
    }
    if (!res) {
      assert(txn->GetState() == TransactionState::ABORTED);
      throw ExecutionException("SeqScanExecutor::Init() lock fail");
    }
  } catch (TransactionAbortException &e) {
    assert(txn->GetState() == TransactionState::ABORTED);
    throw ExecutionException("SeqScanExecutor::Init() lock fail");
  }
}

void SeqScanExecutor::UnLockTable() {
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto oid = plan_->GetTableOid();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->IsTableSharedLocked(oid)) {
      lock_mgr->UnlockTable(txn, oid);
    }
  }
}

void SeqScanExecutor::LockRow(const RID &rid) {
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto oid = plan_->GetTableOid();
  bool res = true;
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!txn->IsRowExclusiveLocked(oid, rid)) {
      res = lock_mgr->LockRow(txn, LockManager::LockMode::SHARED, oid, rid);
    }
  }
  if (!res) {
    txn->SetState(TransactionState::ABORTED);
    throw ExecutionException("SeqScanExecutor::Next() lock fail");
  }
}

void SeqScanExecutor::UnLockRow(const RID &rid) {
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto oid = plan_->GetTableOid();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->IsRowSharedLocked(oid, rid)) {
      lock_mgr->UnlockRow(txn, oid, rid);
    }
  }
}

}  // namespace bustub
