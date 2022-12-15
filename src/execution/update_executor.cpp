//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "fmt/core.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)),
      child_executor_(std::move(child_executor)),
      updated_(false) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (updated_) {
    return false;
  }
  updated_ = true;

  size_t cnt = 0;
  Tuple child_tuple;
  const auto &schema = child_executor_->GetOutputSchema();

  std::vector<Value> values;
  values.reserve(plan_->target_expressions_.size());
  while (child_executor_->Next(&child_tuple, rid)) {
    // fmt::print("tuple {}\n", child_tuple.ToString(&schema));
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, schema));
    }
    Tuple new_tuple(std::move(values), &schema);
    // fmt::print("new tuple {}\n", new_tuple.ToString(&schema));
    bool res = table_info_->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction());
    cnt += res ? 1 : 0;
    values.clear();
  }

  *tuple = Tuple{{ValueFactory::GetIntegerValue(cnt)}, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
