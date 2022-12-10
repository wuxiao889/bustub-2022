//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cassert>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/rid.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "optimizer/optimizer.h"
#include "type/value_factory.h"
// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_child)),
      right_child_executor_(std::move(right_child)),
      build_(false),
      right_finished_(false),
      reorded_(false),
      cur_index_(0),
      cur_vec_(nullptr) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  right_child_executor_->Init();

  if (build_) {
    return;
  }

  build_ = true;
  left_child_executor_->Init();

  if (plan_->GetJoinType() == JoinType::INNER) {
    reorded_ =
        plan_->GetLeftPlan()->output_schema_->GetColumn(0).GetName() != plan_->output_schema_->GetColumn(0).GetName();
  }

  Tuple tuple;
  RID rid;

  const auto left_size = Optimizer::EstimatePlan(plan_->GetLeftPlan());
  if (left_size) {
    left_ht_.reserve(left_size.value() / 2);
  }

  while (left_child_executor_->Next(&tuple, &rid)) {
    Insert(tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto &left_schema = left_child_executor_->GetOutputSchema();
  const auto &right_schema = right_child_executor_->GetOutputSchema();

  while (true) {
    if (cur_vec_ != nullptr && cur_index_ < cur_vec_->size()) {
      const auto &left_tupe = cur_vec_->at(cur_index_);
      std::vector<Value> values;

      if (right_finished_) {
        values = GenerateValue(&left_tupe, left_schema, nullptr, right_schema);
      } else {
        values = GenerateValue(&left_tupe, left_schema, &right_tuple_, right_schema);
      }

      *tuple = {std::move(values), &plan_->OutputSchema()}; // avoid copy
      cur_index_++;
      return true;
    }

    right_tuple_ = {};
    cur_vec_ = nullptr;
    cur_index_ = 0;

    JoinKey key;

    const auto status = right_child_executor_->Next(&right_tuple_, rid);
    if (!status) {
      if (plan_->join_type_ == JoinType::INNER) {
        return false;
      }

      right_finished_ = true;
      const auto not_joined_it = not_joined_.begin();
      if (not_joined_it != not_joined_.end()) {
        key = *not_joined_it;
      } else {
        return false;
      }
    }

    if (!right_finished_) {
      key = MakeRightJoinKey(&right_tuple_);
    }

    const auto left_ht_it = left_ht_.find(key);

    if (left_ht_it != left_ht_.end()) {
      not_joined_.erase(key);
      cur_vec_ = &(left_ht_it->second);
    }
  }
}

auto HashJoinExecutor::GenerateValue(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                                     const Schema &right_schema) -> std::vector<Value> {
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());

  if (reorded_) {
    assert(right_tuple);
    for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
      values.push_back(right_tuple->GetValue(&right_schema, i));
    }
    for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {  // left_tuple.GetLegth() xxxxxx
      values.push_back(left_tuple->GetValue(&left_schema, i));
    }
  } else {
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
  }
  return values;
}

}  // namespace bustub
