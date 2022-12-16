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

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

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
  // const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto oid = plan_->table_oid_;
  const auto &table_info = exec_ctx_->GetCatalog()->GetTable(oid);

  int cnt = 0;

  while (child_executor_->Next(&child_tuple, rid)) {
    LockRow(*rid);
    auto status = table_info->table_->MarkDelete(*rid, txn);

    if (status) {
      cnt++;
      std::vector<IndexInfo *> index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

      for (auto index_info : index_infos) {
        auto new_key = child_tuple.KeyFromTuple(table_info->schema_, *index_info->index_->GetKeySchema(),
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
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &txn = exec_ctx_->GetTransaction();
  const auto oid = plan_->table_oid_;
  try {
    bool res = true;
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      res = lock_mgr->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
    }
    if (!res) {
      assert(txn->GetState() == TransactionState::ABORTED);
      throw ExecutionException("DeleteExecutor::Init() lock fail");
    }
  } catch (TransactionAbortException &e) {
    assert(txn->GetState() == TransactionState::ABORTED);
    throw ExecutionException("DeleteExecutor::Init() lock fail");
  }
}

void DeleteExecutor::LockRow(const RID &rid) {
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto oid = plan_->table_oid_;
  bool res = true;
  res = lock_mgr->LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, rid);
  if (!res) {
    txn->SetState(TransactionState::ABORTED);
    throw ExecutionException("InsertExecutor::Next() lock fail");
  }
}

}  // namespace bustub

/*
Hint: You only need to get a RID from the child executor and call TableHeap::MarkDelete() to effectively delete the
tuple. All deletes will be applied upon transaction commit.
*/
