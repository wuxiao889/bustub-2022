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

#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  ExecutorContext *ctx = GetExecutorContext();
  Transaction *txn = ctx->GetTransaction();
  TableInfo *table_info = ctx->GetCatalog()->GetTable(plan_->table_oid_);

  int cnt = 0;

  while (child_executor_->Next(&child_tuple, rid)) {
    auto ok = table_info->table_->InsertTuple(child_tuple, rid, txn);

    if (ok) {
      cnt++;
      std::vector<IndexInfo *> index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

      for (auto index_info : index_infos) {
        auto new_key = child_tuple.KeyFromTuple(table_info->schema_, *index_info->index_->GetKeySchema(),
                                                index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(new_key, *rid, txn);
      }
    }
  }

  std::vector<Value> values;
  if (cnt != 0) {
    values.emplace_back(TypeId::INTEGER, cnt);
    *tuple = Tuple{values, &plan_->OutputSchema()};
    return true;
  }

  return false;
}

}  // namespace bustub
