//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "fmt/core.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto *bpm = exec_ctx_->GetBufferPoolManager();
  auto *txn = exec_ctx_->GetTransaction();
  auto &outer_schema = child_executor_->GetOutputSchema();
  auto &inner_schema = plan_->InnerTableSchema();
  auto key_predicate = plan_->KeyPredicate();

  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  auto *tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  TablePage *table_page{};

  while (true) {
    Tuple outer_tuple;

    const bool status = child_executor_->Next(&outer_tuple, rid);

    if (!status) {
      return false;
    }

    // fmt::print("{}\n", outer_tuple.ToString(&child_executor_->GetOutputSchema()));

    std::vector<RID> result;
    auto value = key_predicate->Evaluate(&outer_tuple, outer_schema);
    Tuple join_key{{value}, &index_info->key_schema_};

    tree->ScanKey(join_key, &result, txn);

    if (result.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      auto vec = GenerateValue(&outer_tuple, outer_schema, nullptr, inner_schema);
      *tuple = Tuple{std::move(vec), &plan_->OutputSchema()};  // avoid copy
      return true;
    }

    for (auto inner_rid : result) {
      // result.size() == 1
      Tuple inner_tuple;
      table_page = reinterpret_cast<TablePage *>(bpm->FetchPage(inner_rid.GetPageId())->GetData());
      table_page->GetTuple(inner_rid, &inner_tuple, txn, exec_ctx_->GetLockManager());
      auto vec = GenerateValue(&outer_tuple, outer_schema, &inner_tuple, inner_schema);
      *tuple = Tuple{std::move(vec), &plan_->OutputSchema()};
      bpm->UnpinPage(rid->GetPageId(), false);

      return true;
    }
  }
}

auto NestIndexJoinExecutor::GenerateValue(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                                          const Schema &right_schema) -> std::vector<Value> {
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());

  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {  // left_tuple.GetLegth() xxxxxx
    values.push_back(left_tuple->GetValue(&left_schema, i));
  }

  if (right_tuple != nullptr) {
    for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
      values.push_back(right_tuple->GetValue(&right_schema, i));
    }
  } else {
    for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
      auto type_id = right_schema.GetColumn(i).GetType();
      values.push_back(ValueFactory::GetNullValueByType(type_id));
    }
  }

  return values;
}

}  // namespace bustub
