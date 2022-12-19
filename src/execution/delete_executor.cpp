//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

#define DELETENLOCK

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  LockTable();
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (deleted_) {
    return false;
  }
  deleted_ = true;

  Tuple child_tuple{};
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &oid = plan_->table_oid_;
  int cnt = 0;

  while (child_executor_->Next(&child_tuple, rid)) {
    LockRow(*rid);
    auto status = table_info_->table_->MarkDelete(*rid, txn);

    if (status) {
      cnt++;
      std::vector<IndexInfo *> index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto index_info : index_infos) {
        auto new_key = child_tuple.KeyFromTuple(table_info_->schema_, *index_info->index_->GetKeySchema(),
                                                index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(new_key, *rid, txn);
        txn->GetIndexWriteSet()->emplace_back(*rid, oid, WType::DELETE, child_tuple, index_info->index_oid_,
                                              exec_ctx_->GetCatalog());
      }
    }
  }

  *tuple = Tuple{{ValueFactory::GetIntegerValue(cnt)}, &plan_->OutputSchema()};
  return true;
}

void DeleteExecutor::LockTable() {
#ifndef DELETENLOCK
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &oid = plan_->table_oid_;
  try {
    bool res = true;
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      res = lock_mgr->LockTable(txn, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
    }
    if (!res) {
      assert(txn->GetState() == TransactionState::ABORTED);
      throw ExecutionException("DeleteExecutor::LockTable fail");
    }
  } catch (TransactionAbortException &e) {
    assert(txn->GetState() == TransactionState::ABORTED);
    throw ExecutionException("DeleteExecutor::LockTable fail");
  }
#endif
}

void DeleteExecutor::LockRow(const RID &rid) {
#ifndef DELETENLOCK
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &oid = plan_->table_oid_;
  bool res = true;
  try {
    res = lock_mgr->LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, rid);
    if (!res) {
      txn->SetState(TransactionState::ABORTED);
      throw ExecutionException("DeleteExecutor::LockRow fail");
    }
  } catch (TransactionAbortException &e) {
    assert(txn->GetState() == TransactionState::ABORTED);
    throw ExecutionException("DeleteExecutor::LockRow fail");
  }
#endif
}

}  // namespace bustub

/*
Hint: You only need to get a RID from the child executor and call TableHeap::MarkDelete() to effectively delete the
tuple. All deletes will be applied upon transaction commit.
*/
