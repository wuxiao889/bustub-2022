//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/exception.h"
#include "common/logger.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

// #define INSERTNLOCK

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  LockTable();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (inserted_) {
    return false;
  }
  inserted_ = true;

  Tuple child_tuple{};
  const auto &ctx = GetExecutorContext();
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &oid = plan_->table_oid_;
  const auto &table_info = ctx->GetCatalog()->GetTable(oid);

  int cnt = 0;

  while (child_executor_->Next(&child_tuple, rid)) {
    const auto status = table_info->table_->InsertTuple(child_tuple, rid, txn);
    LockRow(*rid);
    if (status) {
      cnt++;
      std::vector<IndexInfo *> index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      for (const auto &index_info : index_infos) {
        auto new_key = child_tuple.KeyFromTuple(table_info->schema_, *index_info->index_->GetKeySchema(),
                                                index_info->index_->GetKeyAttrs());
        // fmt::print("key {}\n", new_key.ToString(&index_info->key_schema_));
        index_info->index_->InsertEntry(new_key, *rid, txn);
        txn->GetIndexWriteSet()->emplace_back(*rid, oid, WType::INSERT, child_tuple, index_info->index_oid_,
                                              exec_ctx_->GetCatalog());
      }
    }
  }

  *tuple = Tuple{{ValueFactory::GetIntegerValue(cnt)}, &plan_->OutputSchema()};
  return true;
}

void InsertExecutor::LockTable() {
#ifndef INSERTNLOCK
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &oid = plan_->table_oid_;
  try {
    bool res = true;
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        res = lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
      }
    } else {
      res = lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
    }
    if (!res) {
      assert(txn->GetState() == TransactionState::ABORTED);
      throw ExecutionException("InsertExecutor::LockTable fail");
    }
  } catch (TransactionAbortException &e) {
    assert(txn->GetState() == TransactionState::ABORTED);
    throw ExecutionException("InsertExecutor::LockTable fail");
  }
#endif
}

void InsertExecutor::LockRow(const RID &rid) {
#ifndef INSERTNLOCK
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &oid = plan_->table_oid_;
  try {
    bool res = true;
    res = lock_mgr->LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, rid);
    if (!res) {
      txn->SetState(TransactionState::ABORTED);
      throw ExecutionException("InsertExecutor::LockRow fail");
    }
  } catch (TransactionAbortException &e) {
    assert(txn->GetState() == TransactionState::ABORTED);
    throw ExecutionException("InsertExecutor::LockRow fail");
  }
#endif
}

}  // namespace bustub
