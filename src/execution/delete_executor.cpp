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

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (deleted_) {
    return false;
  }
  deleted_ = true;
  Tuple child_tuple{};
  ExecutorContext *ctx = GetExecutorContext();
  Transaction *txn = ctx->GetTransaction();
  TableInfo *table_info = ctx->GetCatalog()->GetTable(plan_->table_oid_);

  int cnt = 0;

  while (child_executor_->Next(&child_tuple, rid)) {
    auto ok = table_info->table_->MarkDelete(*rid, txn);

    if (ok) {
      cnt++;
      std::vector<IndexInfo *> index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

      for (auto index_info : index_infos) {
        auto new_key = child_tuple.KeyFromTuple(table_info->schema_, *index_info->index_->GetKeySchema(),
                                                index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(new_key, *rid, txn);
      }
    }
  }

  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, cnt);
  *tuple = Tuple{values, &plan_->OutputSchema()};
  return true;
}

}  // namespace bustub

/*

Hint: You only need to get a RID from the child executor and call TableHeap::MarkDelete() to effectively delete the
tuple. All deletes will be applied upon transaction commit.

*/