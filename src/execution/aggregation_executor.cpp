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
  fmt::print("{}\n", plan_->ToString());
}

void AggregationExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple;
  RID rid;
  while (child_executor_->Next(&child_tuple, &rid)) {
    LOG_DEBUG("%s\n", child_tuple.ToString(&child_executor_->GetOutputSchema()).c_str());
    auto agg_key = MakeAggregateKey(&child_tuple);
    auto agg_value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(agg_key, agg_value);
  }
  if (aht_.Empty()) {
    auto agg_key = MakeAggregateKey(&child_tuple);
    aht_.MakeEmpty(agg_key);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ != aht_.End()) {
    *tuple = Tuple{aht_iterator_.Val().aggregates_, &plan_->OutputSchema()};
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
