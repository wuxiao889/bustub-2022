#include "execution/plans/abstract_plan.h"
#include "optimizer/optimizer.h"

// Note for 2022 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file. Note
// that for some test cases, we force using starter rules, so that the configuration here won't take effects. Starter
// rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizeNLJAsHashJoin(p);  // Enable this rule after you have implemented hash join.
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  return p;
}

auto Optimizer::OptimizeJoinOrder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeProjection(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  return optimized_plan;
}

}  // namespace bustub
