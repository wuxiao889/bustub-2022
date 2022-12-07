#include "execution/executors/topn_executor.h"
#include <algorithm>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), sorted_(false) {}

void TopNExecutor::Init() {
  cur_ = 0;

  if (sorted_) {
    return;
  }

  child_executor_->Init();
  fmt::print("{}\n", plan_->GetN());
  const auto &schema = plan_->OutputSchema();
  const auto &order_bys = plan_->GetOrderBy();

  auto pred = [&](const auto &left, const auto &right) -> bool {
    const Tuple &left_tuple = left.first;
    const Tuple &right_tuple = right.first;
    for (auto &[order_type, expr] : order_bys) {
      Value left_key = expr->Evaluate(&left_tuple, schema);
      Value right_key = expr->Evaluate(&right_tuple, schema);

      const auto equal = left_key.CompareEquals(right_key);
      if (equal == CmpBool::CmpTrue) {
        continue;
      }
      // 升序用大根堆(less)，降序小根堆(greater)
      return order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT
                 ? left_key.CompareLessThan(right_key) == CmpBool::CmpTrue
                 : left_key.CompareGreaterThan(right_key) == CmpBool::CmpTrue;
    }
    return true;
  };

  Tuple tuple;
  RID rid;
  const auto limit = plan_->GetN();
  heap_.reserve(limit);
  while (child_executor_->Next(&tuple, &rid)) {
    auto tmp = std::make_pair(tuple, rid);
    if (heap_.size() == limit) {
      if (pred(tmp, heap_[0])) {
        std::pop_heap(heap_.begin(), heap_.end(), pred);
        heap_[limit - 1] = tmp;
        std::push_heap(heap_.begin(), heap_.end(), pred);
      }
    } else {
      heap_.emplace_back(tuple, rid);
      std::push_heap(heap_.begin(), heap_.end(), pred);
    }
  }
  std::sort_heap(heap_.begin(), heap_.end(), pred);
  sorted_ = true;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_ >= heap_.size()) {
    return false;
  }
  auto &p = heap_[cur_];
  *tuple = p.first;
  *rid = p.second;
  cur_++;
  //   fmt::print("TopNExecutor::Next {}\n", tuple->ToString(&plan_->OutputSchema()));
  return true;
}

}  // namespace bustub
