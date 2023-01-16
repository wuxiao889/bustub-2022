#include <memory>
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = static_cast<const LimitPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(limit_plan.children_.size() == 1, "sort should have 1 children");
    const auto &child_plan = limit_plan.GetChildPlan();
    const auto limit = limit_plan.GetLimit();
    if (child_plan->GetType() == PlanType::Sort) {
      const auto &sort_plan = static_cast<const SortPlanNode &>(*child_plan);
      const auto &order_bys = sort_plan.GetOrderBy();
      return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan.GetChildPlan(), order_bys, limit);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
