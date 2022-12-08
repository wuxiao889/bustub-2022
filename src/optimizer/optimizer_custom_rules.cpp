#include <algorithm>
#include <memory>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/column.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/values_plan.h"
#include "fmt/color.h"
#include "optimizer/optimizer.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

// Note for 2022 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file. Note
// that for some test cases, we force using starter rules, so that the configuration here won't take effects. Starter
// rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeFilter(p);
  p = OptimizeEliminateTrueFalseFilter(p);
  p = OptimizeMergeFilterNLJ(p);
  // fmt::print(fg(fmt::color::brown), "OptimizeMergeFilterNLJ()\n{}\n\n", *p);
  // p = OptimizeFilterPushDown(p);
  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizeNLJAsHashJoin(p);  // Enable this rule after you have implemented hash join.
  // p = OptimizeMergeFilterScan(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  return p;
}

auto Optimizer::OptimizeJoinOrder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeJoinOrder(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &hash_join_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(hash_join_plan.children_.size() == 2, "hash_join_plan should have exactly 2 children.");
  }
  return optimized_plan;
}

auto Optimizer::OptimizeFilterPushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeFilterPushDown(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  std::vector<const ComparisonExpression *> cmp_exprs;

  std::function<void(const AbstractExpression *)> find_expr = [&](const AbstractExpression *pred) {
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(pred); expr != nullptr) {
      if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(0).get());
          left_expr != nullptr) {
        if (const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(expr->GetChildAt(1).get());
            right_expr != nullptr) {
          cmp_exprs.push_back(expr);
        }
      } else if (const auto *left_expr = dynamic_cast<const ConstantValueExpression *>(expr->GetChildAt(0).get());
                 left_expr != nullptr) {
        if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(1).get());
            right_expr != nullptr) {
          cmp_exprs.push_back(expr);
        }
      }
    }

    if (const auto *expr = dynamic_cast<const LogicExpression *>(pred); expr != nullptr) {
      // fmt::print("{} {}\n", *expr, expr->logic_type_);
      for (const auto &cexpr : expr->GetChildren()) {
        find_expr(cexpr.get());
      }
    }
  };

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    // Check if expr is equal condition where one is for the left table, and one is for the right table.
    // if (const auto *expr = dynamic_cast<const LogicExpression *>(&nlj_plan.Predicate()); expr != nullptr) {
    //   fmt::print("{} {}\n", *expr, expr->GetChildren().size());
    // }
    if (find_expr(nlj_plan.predicate_.get()); cmp_exprs.size() > 1) {
      fmt::print("\n");
      for (const auto &expr : cmp_exprs) {
        fmt::print("{}\n", *expr);
      }
      // 找到第一个equal
      // auto it = std::find(cmp_exprs.begin(), cmp_exprs.end(),
      //                     [](const ComparisonExpression *expr) { return expr->comp_type_ == ComparisonType::Equal;
      //                     });

      // if (it != cmp_exprs.end()) {
      //   const auto *equal_expr = *it;
      //   cmp_exprs.erase(it);
      //   auto filter = std::make_shared<FilterPlanNode>(
      //       nlj_plan.output_schema_, std::make_shared<AbstractExpression>(equal_expr), std::move(optimized_plan));
      //   return filter;
      // }
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeEliminateTrueFalseFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeEliminateTrueFalseFilter(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(filter_plan.predicate_.get());
        const_expr != nullptr) {
      if (!const_expr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>()) {
        return std::make_shared<ValuesPlanNode>(optimized_plan->output_schema_,
                                                std::vector<std::vector<AbstractExpressionRef>>());
      }
      return optimized_plan->children_[0];
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;

  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeFilter(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    fmt::print("{}\n\n", filter_plan);
    return std::make_shared<FilterPlanNode>(filter_plan.output_schema_, OptimizeFilterExpr(filter_plan.predicate_),
                                            filter_plan.GetChildPlan());
  }
  return optimized_plan;
}

auto Optimizer::OptimizeFilterExpr(const AbstractExpressionRef &pred) -> AbstractExpressionRef {
  if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(pred.get()); cmp_expr != nullptr) {
    if (const auto &left_value_expr = dynamic_cast<const ConstantValueExpression *>(cmp_expr->GetChildAt(0).get());
        left_value_expr != nullptr) {
      if (const auto &right_value_expr = dynamic_cast<const ConstantValueExpression *>(cmp_expr->GetChildAt(1).get());
          right_value_expr != nullptr) {
        CmpBool value;
        switch (cmp_expr->comp_type_) {
          case ComparisonType::Equal:
            value = left_value_expr->val_.CompareEquals(right_value_expr->val_);
            break;
          case ComparisonType::NotEqual:
            value = left_value_expr->val_.CompareNotEquals(right_value_expr->val_);
            break;
          case ComparisonType::LessThan:
            value = left_value_expr->val_.CompareLessThan(right_value_expr->val_);
            break;
          case ComparisonType::LessThanOrEqual:
            value = left_value_expr->val_.CompareLessThanEquals(right_value_expr->val_);
            break;
          case ComparisonType::GreaterThan:
            value = left_value_expr->val_.CompareGreaterThan(right_value_expr->val_);
            break;
          case ComparisonType::GreaterThanOrEqual:
            value = left_value_expr->val_.CompareGreaterThanEquals(right_value_expr->val_);
            break;
        }
        // fmt::print("{} {} {}\n", *left_value_expr, *right_value_expr, value == CmpBool::CmpTrue);
        if (value != CmpBool::CmpNull) {
          if (value == CmpBool::CmpTrue) {
            return std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true));
          }
          return std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(false));
        }
      }
    }
    return pred;
  }

  auto left_expr = OptimizeFilterExpr(pred->GetChildAt(0));
  auto right_expr = OptimizeFilterExpr(pred->GetChildAt(1));

  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(pred.get()); logic_expr != nullptr) {
    const auto &logic_type = logic_expr->logic_type_;

    if (const auto *left_const_expr = dynamic_cast<const ConstantValueExpression *>(left_expr.get());
        left_const_expr != nullptr) {
      // logic_expr 两个孩子如果是constvalue,一定是bool类型
      const auto &is_true_logic = left_const_expr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>();
      switch (logic_type) {
        case LogicType::And:
          return is_true_logic ? right_expr : std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(false));
        case LogicType::Or:
          return is_true_logic ? std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true)) : right_expr;
      }
    }

    if (const auto *right_const_expr = dynamic_cast<const ConstantValueExpression *>(right_expr.get());
        right_const_expr != nullptr) {
      const auto &is_true_logic = right_const_expr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>();
      switch (logic_type) {
        case LogicType::And:
          return is_true_logic ? left_expr : std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(false));
        case LogicType::Or:
          return is_true_logic ? std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true)) : left_expr;
      }
    }
  }
  return pred;
}

}  // namespace bustub
