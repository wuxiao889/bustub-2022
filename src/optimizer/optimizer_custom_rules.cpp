#include <math.h>
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include <optional>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/values_plan.h"
#include "fmt/color.h"
#include "fmt/core.h"
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
  // fmt::print("OptimizeFilter(p)\n{}\n\n", *p);
  p = OptimizeJoinOrder(p);
  // fmt::print("OptimizeJoinOrder(p)\n{}\n\n", *p);
  p = OptimizeMergeFilterNLJ(p);
  // fmt::print("OptimizeMergeFilterNLJ(p)\n{}\n\n", *p);
  p = OptimizeMergeFilterScan(p);

  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizeNLJAsHashJoin(p);

  p = OptimizeMergeFilterScan(p);
  p = OptimizeOrderByAsIndexScan(p);

  p = OptimizeSortLimitAsTopN(p);
  return p;
}

auto Optimizer::ReversePredict(const AbstractExpressionRef &pred) -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  if (const auto *const_value_expr = dynamic_cast<const ConstantValueExpression *>(pred.get())) {
    return pred;
  }
  const auto *expr = dynamic_cast<const ComparisonExpression *>(pred.get());
  assert(expr);
  for (const auto &child : pred->GetChildren()) {
    const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(child.get());
    children.emplace_back(std::make_shared<ColumnValueExpression>(
        column_value_expr->GetTupleIdx() ^ 1, column_value_expr->GetColIdx(), column_value_expr->GetReturnType()));
  }
  return expr->CloneWithChildren(children);
}

auto Optimizer::OptimizeJoinOrder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeJoinOrder(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "nls_plan should have 2 chlid");

    if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(&nlj_plan.Predicate()); cmp_expr != nullptr) {
      if (cmp_expr->comp_type_ == ComparisonType::Equal && nlj_plan.join_type_ == JoinType::INNER) {
        const auto left_size = EstimatePlan(plan->GetChildAt(0));
        const auto right_size = EstimatePlan(plan->GetChildAt(1));
        if (left_size && right_size && left_size > right_size) {
          return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetRightPlan(),
                                                          nlj_plan.GetLeftPlan(), ReversePredict(nlj_plan.predicate_),
                                                          nlj_plan.join_type_);
        }
      }
    }

    if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(&nlj_plan.Predicate());
        const_expr != nullptr) {
      if (const_expr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>()) {
        const auto left_size = EstimatePlan(plan->GetChildAt(0));
        const auto right_size = EstimatePlan(plan->GetChildAt(1));
        if (left_size && right_size && left_size > right_size) {
          return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetRightPlan(),
                                                          nlj_plan.GetLeftPlan(), ReversePredict(nlj_plan.predicate_),
                                                          nlj_plan.join_type_);
        }
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizePredictPushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizePredictPushDown(child));
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
    // if(const auto* const_expr = dynamic_cast<const ConstantValueExpression*>(nlj_plan.predicate_.get()); const_expr
    // != nullptr){

    // }

    if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.predicate_.get());
        cmp_expr != nullptr) {
    }

    if (find_expr(nlj_plan.predicate_.get()); cmp_exprs.size() > 1) {
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
    return std::make_shared<FilterPlanNode>(filter_plan.output_schema_, OptimizeFilterExpr(filter_plan.predicate_),
                                            filter_plan.GetChildPlan());
  }
  return optimized_plan;
}

auto Optimizer::OptimizeFilterExpr(const AbstractExpressionRef &pred) -> AbstractExpressionRef {
  /*
  FilterExpr有三种情况 ConstValue Cmp Logic
  ConstValue {true, false}
  cmp {1 = 2, #1.0 = #0.0}
    等号两边有三种情况constvalue, arithmetic, colvalue
  logic {cmp and cmp and cmp or cmp}
  */
  if (const auto *const_expr = dynamic_cast<ConstantValueExpression *>(pred.get()); const_expr != nullptr) {
    return pred;
  }

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
          return is_true_logic ? right_expr
                               : std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(false));
        case LogicType::Or:
          return is_true_logic ? std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true))
                               : right_expr;
      }
    }

    if (const auto *right_const_expr = dynamic_cast<const ConstantValueExpression *>(right_expr.get());
        right_const_expr != nullptr) {
      const auto &is_true_logic = right_const_expr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>();
      switch (logic_type) {
        case LogicType::And:
          return is_true_logic ? left_expr
                               : std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(false));
        case LogicType::Or:
          return is_true_logic ? std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true))
                               : left_expr;
      }
    }

    return std::make_shared<LogicExpression>(left_expr, right_expr, logic_expr->logic_type_);
  }
  assert(false);  // ?
  return pred;
}

auto Optimizer::EstimatePlan(const AbstractPlanNodeRef &plan) -> std::optional<size_t> {
  if (plan->GetType() == PlanType::Filter) {
    return EstimatePlan(plan->GetChildAt(0));
  }

  auto get_child_size = [&](const AbstractPlanNodeRef &pla) -> std::optional<size_t> {
    auto left_size = EstimatePlan(plan->GetChildAt(0));
    auto right_size = EstimatePlan(plan->GetChildAt(1));
    if (left_size && right_size) {
      return std::make_optional(left_size.value() + right_size.value());
    }
    return std::nullopt;
  };

  if (plan->GetType() == PlanType::NestedLoopJoin || plan->GetType() == PlanType::NestedLoopJoin) {
    return get_child_size(plan);
  }

  if (plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*plan);
    return EstimatedCardinality(seq_scan.table_name_);
  }

  if (plan->GetType() == PlanType::MockScan) {
    const auto &mock_scan = dynamic_cast<const MockScanPlanNode &>(*plan);
    return EstimatedCardinality(mock_scan.GetTable());
  }
  return std::nullopt;
}

}  // namespace bustub
