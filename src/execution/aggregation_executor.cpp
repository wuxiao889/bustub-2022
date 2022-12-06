//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/aggregation_executor.h"
#include <cstdint>
#include <memory>
#include <vector>

#include "fmt/core.h"
#include "fmt/ranges.h"
#include "storage/table/tuple.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {
  // fmt::print("{}\n", plan_->ToString());
}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();
  Tuple child_tuple;
  RID rid;
  // fmt::print("Agg group_by size:{}\n", plan_->GetGroupBys().size());
  // fmt::print("Agg OutputSchema:{}\n", plan_->output_schema_);
  while (child_executor_->Next(&child_tuple, &rid)) {
    auto agg_key = MakeAggregateKey(&child_tuple);
    auto agg_value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(agg_key, agg_value);
  }
  if (aht_.Empty() && plan_->GetGroupBys().empty()) {
    aht_.MakeEmpty({});
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ != aht_.End()) {
    std::vector<Value> values;
    if (!plan_->GetGroupBys().empty()) {
      values = aht_iterator_.Key().group_bys_;
    }
    values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
    *tuple = Tuple{values, &plan_->OutputSchema()};
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
