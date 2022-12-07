#include "execution/executors/sort_executor.h"
#include <cstdint>
#include "binder/bound_order_by.h"
#include "fmt/core.h"
#include "pg_definitions.hpp"
#include "type/type.h"
#include "type/value.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), sorted_(false) {}

void SortExecutor::Init() {
  cur_ = 0;

  if (sorted_) {
    return;
  }

  child_executor_->Init();

  Tuple tuple;
  RID rid;
  auto &schema = child_executor_->GetOutputSchema();
  auto &order_bys = plan_->GetOrderBy();

  while (child_executor_->Next(&tuple, &rid)) {
    result_.emplace_back(tuple, rid);
  }

  std::sort(result_.begin(), result_.end(), [&](auto &left, auto &right) {
    Tuple &left_tuple = left.first;
    Tuple &right_tuple = right.first;
    // fmt::print("{} {}\n", left_tuple.ToString(&schema), right_tuple.ToString(&schema));
    for (auto &[order_type, expr] : order_bys) {
      Assert(order_type != OrderByType::INVALID);
      Value left_key = expr->Evaluate(&left_tuple, schema);
      Value right_key = expr->Evaluate(&right_tuple, schema);

      auto equal = left_key.CompareEquals(right_key);
      if (equal == CmpBool::CmpTrue) {
        continue;
      }
      // fmt::print("\t{} {} less? {}\n", left_key, right_key, less);
      return order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT
                 ? left_key.CompareLessThan(right_key) == CmpBool::CmpTrue
                 : left_key.CompareGreaterThan(right_key) == CmpBool::CmpTrue;
    }
    return true;
  });
  sorted_ = true;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_ >= result_.size()) {
    return false;
  }
  auto &p = result_[cur_];
  *tuple = p.first;
  *rid = p.second;
  cur_++;
  // fmt::print("SortExecutor::Next {}\n", tuple->ToString(&plan_->OutputSchema()));
  return true;
}

}  // namespace bustub
