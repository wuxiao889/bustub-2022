#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>
#include "binder/bound_expression.h"
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "execution/plans/values_plan.h"
#include "fmt/color.h"
#include "fmt/core.h"
#include "fmt/ranges.h"
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
  // fmt::print("p\n{}\n\n", *p);
  p = OptimizeMergeProjection(p);
  // fmt::print("OptimizeMergeProjection(p)\n{}\n\n", *p);
  p = OptimizeColumnPruning(p);
  // fmt::print("OptimizeColumnPruning(p)\n{}\n\n", *p);
  // TODO(wxx) merge once?
  p = OptimizeMergeProjection(p);
  // fmt::print("OptimizeMergeProjection(p)1\n{}\n\n", *p);
  p = OptimizeFilter(p);
  p = OptimizeEliminateTrueFalseFilter(p);
  // fmt::print("OptimizeFilter(p)\n{}\n\n", *p);
  p = OptimizeMergeFilterNLJ(p);  // 不能先调整order在merger filter nlj， filter pred顺序会不对
  // fmt::print("OptimizeMergeFilterNLJ(p)\n{}\n\n", *p);
  p = OptimizeJoinOrder(p);
  // fmt::print("OptimizeJoinOrder(p)\n{}\n\n", *p);
  p = OptimizeNLJPredicate(p);
  // fmt::print("OptimizePredictPushDown(p)\n{}\n\n", *p);
  p = OptimizeMergeFilterScan(p);
  // fmt::print("OptimizeMergeFilterScan(p)\n{}\n\n", *p);
  p = OptimizeSeqScanAsIndexScan(p);
  // fmt::print("OptimizeSeqScanAsIndexScan(p)\n{}\n\n", *p);
  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizeNLJAsHashJoin(p);

  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  return p;
}

// TODO(wxx) bug in column pruning, if we first column pruning than merge projection
// select colA from (select colC, colA, colB, from temp_2 order by colC - colB + colA, colA limit 20);
auto Optimizer::OptimizeColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto optimized_plan = plan;
  if (plan->GetType() == PlanType::Projection) {
    const auto &projection_plan = static_cast<const ProjectionPlanNode &>(*plan);
    BUSTUB_ENSURE(plan->children_.size() == 1, "Projection with multiple children?? That's weird!");
    // If the schema is the same (except column name)
    const auto &child_plan = plan->children_[0];
    const auto child_plan_type = child_plan->GetType();
    if (child_plan_type == PlanType::Projection || child_plan_type == PlanType::Aggregation) {
      const auto &projection_exprs = projection_plan.GetExpressions();

      std::unordered_set<size_t> needed_columns;

      // col_value / arithmetic / const_val ?
      std::function<void(const AbstractExpressionRef &)> find_needed_column = [&](const AbstractExpressionRef &expr) {
        // fmt::print("{}\n", *expr);
        if (auto arithmetic_expr = dynamic_cast<const ArithmeticExpression *>(expr.get()); arithmetic_expr != nullptr) {
          find_needed_column(expr->GetChildAt(0));
          find_needed_column(expr->GetChildAt(1));
        } else if (auto column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
                   column_value_expr != nullptr) {
          BUSTUB_ENSURE(column_value_expr->GetTupleIdx() == 0, "projection plan tuple index must be 0");
          needed_columns.insert(column_value_expr->GetColIdx());
        }
      };

      for (const auto &expr : projection_exprs) {
        if (auto column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
            column_value_expr != nullptr) {
          BUSTUB_ENSURE(column_value_expr->GetTupleIdx() == 0, "projection plan tuple index must be 0");
          needed_columns.insert(column_value_expr->GetColIdx());

        } else if (auto arithmetic_expr = dynamic_cast<const ArithmeticExpression *>(expr.get());
                   arithmetic_expr != nullptr) {
          find_needed_column(expr);
        }
      }

      // fmt::print("{}\n", needed_col);

      std::vector<AbstractExpressionRef> needed_exprs;

      if (child_plan_type == PlanType::Projection) {
        const auto &child_projection_plan = static_cast<const ProjectionPlanNode &>(*child_plan);
        const auto &child_projection_exprs = child_projection_plan.GetExpressions();

        for (size_t idx = 0; idx < child_projection_exprs.size(); ++idx) {
          if (needed_columns.find(idx) != needed_columns.end()) {
            needed_exprs.push_back(child_projection_exprs[idx]);
          }
        }

        if (needed_exprs.size() != child_projection_exprs.size()) {
          auto new_projection_schema =
              std::make_shared<Schema>(ProjectionPlanNode::InferProjectionSchema(needed_exprs));
          auto new_child_projection_plan = std::make_shared<ProjectionPlanNode>(
              std::move(new_projection_schema), std::move(needed_exprs), child_plan->GetChildAt(0));
          optimized_plan = plan->CloneWithChildren({new_child_projection_plan});
        }

      } else {
        const auto &child_agg_plan = dynamic_cast<const AggregationPlanNode &>(*child_plan);
        const auto &child_agg_exprs = child_agg_plan.GetAggregates();
        const auto &chld_agg_types = child_agg_plan.GetAggregateTypes();
        const auto &group_bys = child_agg_plan.GetGroupBys();
        const auto offset = group_bys.empty() ? 0 : 1;

        std::vector<AggregationType> needed_types;

        for (size_t idx = 0; idx < child_agg_exprs.size(); ++idx) {
          // 如果有group_by, schema 0 列是 group by key , agg_expr[i] 对应的是 agg_schema的 i+1 列，
          // 如果没有，agg_expr[i] 就对应 agg_schema[i]
          if (needed_columns.find(idx + offset) != needed_columns.end()) {
            needed_exprs.push_back(child_agg_exprs[idx]);
            needed_types.push_back(chld_agg_types[idx]);
          }
        }

        if (needed_exprs.size() != child_agg_exprs.size()) {
          auto new_agg_schema = std::make_shared<Schema>(
              AggregationPlanNode::InferAggSchema(child_agg_plan.group_bys_, needed_exprs, needed_types));
          auto new_child_agg_plan = std::make_shared<AggregationPlanNode>(
              std::move(new_agg_schema), child_agg_plan.GetChildPlan(), child_agg_plan.group_bys_,
              std::move(needed_exprs), std::move(needed_types));
          optimized_plan = plan->CloneWithChildren({new_child_agg_plan});
        }
      }
    }
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : optimized_plan->GetChildren()) {
    children.emplace_back(OptimizeColumnPruning(child));
  }
  optimized_plan = optimized_plan->CloneWithChildren(std::move(children));
  return optimized_plan;
}

auto Optimizer::RewriteTupleIndex(const AbstractExpressionRef &pred) -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : pred->GetChildren()) {
    children.emplace_back(RewriteTupleIndex(child));
  }
  auto new_pred = pred->CloneWithChildren(std::move(children));

  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(pred.get());
      column_value_expr != nullptr) {
    // 0 -> 1 , 1 -> 0
    return std::make_shared<ColumnValueExpression>(column_value_expr->GetTupleIdx() ^ 1, column_value_expr->GetColIdx(),
                                                   column_value_expr->GetReturnType());
  }
  return new_pred;
}

auto Optimizer::OptimizeJoinOrder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeJoinOrder(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = static_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "nls_plan should have 2 chlid");
    if (nlj_plan.join_type_ == JoinType::INNER) {
      const auto left_size = EstimatePlan(plan->GetChildAt(0));
      const auto right_size = EstimatePlan(plan->GetChildAt(1));
      // std::optional 可以直接当作bool判断
      if (left_size && right_size && left_size > right_size) {
        // 在join中通过对比schema第一列的表名判断是否reorder
        return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetRightPlan(),
                                                        nlj_plan.GetLeftPlan(), RewriteTupleIndex(nlj_plan.predicate_),
                                                        nlj_plan.join_type_);
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeNLJPredicate(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto optimized_plan = plan;

  std::vector<AbstractExpressionRef> left_cmp_exprs;
  std::vector<AbstractExpressionRef> right_cmp_exprs;
  std::vector<AbstractExpressionRef> col_equal_exprs;

  std::function<bool(const AbstractExpressionRef &)> find_exprs = [&](const AbstractExpressionRef &pred) -> bool {
    if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(pred.get()); logic_expr != nullptr) {
      if (logic_expr->logic_type_ == LogicType::Or) {
        return false;
      }
      find_exprs(pred->GetChildAt(0));
      find_exprs(pred->GetChildAt(1));
    }

    if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(pred.get()); cmp_expr != nullptr) {
      if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
          left_expr != nullptr) {
        if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(1).get());
            right_expr != nullptr) {
          col_equal_exprs.emplace_back(std::make_shared<ComparisonExpression>(
              cmp_expr->GetChildAt(0), cmp_expr->GetChildAt(1), cmp_expr->comp_type_));
          return true;
        }
      }

      if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
          left_expr != nullptr) {
        auto tuple_index = left_expr->GetTupleIdx();
        if (tuple_index == 0) {
          left_cmp_exprs.emplace_back(std::make_shared<ComparisonExpression>(
              cmp_expr->GetChildAt(0), cmp_expr->GetChildAt(1), cmp_expr->comp_type_));
        } else {
          auto expr_tuple_0 =
              std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
          right_cmp_exprs.emplace_back(
              std::make_shared<ComparisonExpression>(expr_tuple_0, cmp_expr->GetChildAt(1), cmp_expr->comp_type_));
          return true;
        }
      }

      if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(1).get());
          right_expr != nullptr) {
        auto tuple_index = right_expr->GetTupleIdx();
        if (tuple_index == 0) {
          left_cmp_exprs.emplace_back(std::make_shared<ComparisonExpression>(
              cmp_expr->GetChildAt(0), cmp_expr->GetChildAt(1), cmp_expr->comp_type_));
        } else {
          auto expr_tuple_0 =
              std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());
          right_cmp_exprs.emplace_back(
              std::make_shared<ComparisonExpression>(expr_tuple_0, cmp_expr->GetChildAt(1), cmp_expr->comp_type_));
        }
      }
    }  // cmp_expr
    return true;
  };

  if (plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = static_cast<const NestedLoopJoinPlanNode &>(*plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    // 复杂逻辑表达式才要下推
    if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(nlj_plan.predicate_.get());
        logic_expr != nullptr) {
      if (find_exprs(nlj_plan.predicate_)) {
        // auto show_expr = [](const auto &exprs) {
        //   for (auto &expr : exprs) {
        //     fmt::print("{}\n", *expr);
        //   }
        // };

        // show_expr(col_equal_exprs);
        // show_expr(left_cmp_exprs);
        // show_expr(right_cmp_exprs);

        if (!left_cmp_exprs.empty() || !right_cmp_exprs.empty()) {
          // nlj , mock_seq, seq,
          const auto join_type = nlj_plan.join_type_;
          auto left_plan = plan->GetChildAt(0);
          auto right_plan = plan->GetChildAt(1);
          AbstractPlanNodeRef new_left_plan = left_plan;
          AbstractPlanNodeRef new_right_plan = right_plan;

          const auto left_plan_type = left_plan->GetType();
          const auto right_plan_type = right_plan->GetType();

          auto new_nlj_expr = BuildExprTree(col_equal_exprs);

          if (!left_cmp_exprs.empty()) {
            AbstractExpressionRef new_expr = BuildExprTree(left_cmp_exprs);
            if (left_plan_type == PlanType::SeqScan || left_plan_type == PlanType::MockScan) {
              // 在中间插入filter, 后续optimize会下推
              new_left_plan = std::make_shared<FilterPlanNode>(left_plan->output_schema_, new_expr, left_plan);
            } else if (left_plan_type == PlanType::NestedLoopJoin) {
              const auto *left_nlj_plan = static_cast<const NestedLoopJoinPlanNode *>(left_plan.get());
              // 构造新表达式
              auto new_left_nlj_expr =
                  std::make_shared<LogicExpression>(new_expr, left_nlj_plan->predicate_, LogicType::And);

              new_left_plan = std::make_shared<NestedLoopJoinPlanNode>(
                  left_nlj_plan->output_schema_, left_nlj_plan->GetLeftPlan(), left_nlj_plan->GetRightPlan(),
                  std::move(new_left_nlj_expr), left_nlj_plan->join_type_);
            }
          }

          if (!right_cmp_exprs.empty()) {
            AbstractExpressionRef new_expr = BuildExprTree(right_cmp_exprs);
            if (right_plan_type == PlanType::SeqScan || right_plan_type == PlanType::MockScan) {
              new_right_plan = std::make_shared<FilterPlanNode>(right_plan->output_schema_, new_expr, right_plan);

            } else if (right_plan_type == PlanType::NestedLoopJoin) {
              const auto *right_nlj_plan = static_cast<const NestedLoopJoinPlanNode *>(right_plan.get());
              auto new_right_nlj_expr =
                  std::make_shared<LogicExpression>(new_expr, right_nlj_plan->predicate_, LogicType::And);

              new_right_plan = std::make_shared<NestedLoopJoinPlanNode>(
                  right_nlj_plan->output_schema_, right_nlj_plan->GetLeftPlan(), right_nlj_plan->GetRightPlan(),
                  std::move(new_right_nlj_expr), right_nlj_plan->join_type_);
            }
          }
          optimized_plan =
              std::make_shared<NestedLoopJoinPlanNode>(plan->output_schema_, std::move(new_left_plan),
                                                       std::move(new_right_plan), std::move(new_nlj_expr), join_type);
        }
      }
    }
  }
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : optimized_plan->GetChildren()) {
    children.emplace_back(OptimizeNLJPredicate(child));
  }
  optimized_plan = optimized_plan->CloneWithChildren(std::move(children));
  return optimized_plan;
}

auto Optimizer::OptimizeEliminateTrueFalseFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeEliminateTrueFalseFilter(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = static_cast<const FilterPlanNode &>(*optimized_plan);
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
    const auto &filter_plan = static_cast<const FilterPlanNode &>(*optimized_plan);
    return std::make_shared<FilterPlanNode>(filter_plan.output_schema_, OptimizeFilterExpr(filter_plan.predicate_),
                                            filter_plan.GetChildPlan());
  }
  return optimized_plan;
}

/*
FilterExpr有三种情况 ConstValue Cmp Logic
ConstValue {true, false}
cmp {1 = 2, #1.0 = #0.0}
  等号两边有三种情况constvalue, arithmetic, colvalue
logic {cmp and cmp and cmp or cmp}
*/
auto Optimizer::OptimizeFilterExpr(const AbstractExpressionRef &pred) -> AbstractExpressionRef {
  if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(pred.get()); const_expr != nullptr) {
    return pred;
  }

  if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(pred.get()); cmp_expr != nullptr) {
    const auto &left_child = cmp_expr->GetChildAt(0);
    const auto &right_child = cmp_expr->GetChildAt(1);

    if (const auto &left_value_expr = dynamic_cast<const ConstantValueExpression *>(left_child.get());
        left_value_expr != nullptr) {
      if (const auto &right_value_expr = dynamic_cast<const ConstantValueExpression *>(right_child.get());
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

  assert(pred->GetReturnType() == TypeId::BOOLEAN);
  const auto *logic_expr = static_cast<const LogicExpression *>(pred.get());
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

auto Optimizer::EstimatePlan(const AbstractPlanNodeRef &plan) -> std::optional<size_t> {
  auto get_child_size = [&](const AbstractPlanNodeRef &pla) -> std::optional<size_t> {
    auto left_size = EstimatePlan(plan->GetChildAt(0));
    auto right_size = EstimatePlan(plan->GetChildAt(1));
    if (left_size && right_size) {
      return std::make_optional(left_size.value() + right_size.value());
    }
    return std::nullopt;
  };

  std::optional<size_t> plan_size = std::nullopt;
  const auto plan_type = plan->GetType();
  switch (plan_type) {
    case PlanType::NestedLoopJoin:
    case PlanType::HashJoin:
      plan_size = get_child_size(plan);
      break;
    case PlanType::SeqScan:
      plan_size = EstimatedCardinality(static_cast<const SeqScanPlanNode &>(*plan).table_name_);
      break;
    case PlanType::MockScan:
      plan_size = EstimatedCardinality(static_cast<const MockScanPlanNode &>(*plan).GetTable());
      break;
    case PlanType::Values:
      plan_size = static_cast<const ValuesPlanNode &>(*plan).GetValues().size();
      break;
    case PlanType::Projection:
    case PlanType::Filter:
    case PlanType::Aggregation:
    case PlanType::Sort:
      plan_size = EstimatePlan(plan->GetChildAt(0));
      break;
    case PlanType::Limit:
      plan_size = static_cast<const LimitPlanNode &>(*plan).GetLimit();
      break;
    case PlanType::TopN:
      plan_size = static_cast<const TopNPlanNode &>(*plan).GetN();
      break;
    case PlanType::IndexScan:
    case PlanType::Insert:
    case PlanType::Update:
    case PlanType::Delete:
    case PlanType::NestedIndexJoin:
      break;
  }
  return plan_size;
}

auto Optimizer::OptimizeSeqScanAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_plan = static_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_plan.filter_predicate_ != nullptr) {
      if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(seq_plan.filter_predicate_.get());
          cmp_expr != nullptr) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(cmp_expr->children_[1].get());
              right_expr != nullptr) {
            if (auto index = MatchIndex(seq_plan.table_name_, left_expr->GetColIdx()); index != std::nullopt) {
              assert(index);
              auto [index_oid, index_name] = *index;
              return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, index_oid,
                                                         seq_plan.filter_predicate_);
            }
          }
        }

        if (const auto *left_expr = dynamic_cast<const ConstantValueExpression *>(cmp_expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[1].get());
              right_expr != nullptr) {
            if (auto index = MatchIndex(seq_plan.table_name_, right_expr->GetColIdx()); index != std::nullopt) {
              assert(index);
              auto [index_oid, index_name] = *index;
              auto new_cmp_expr = std::make_shared<ComparisonExpression>(cmp_expr->GetChildAt(1),
                                                                         cmp_expr->GetChildAt(0), cmp_expr->comp_type_);
              return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, index_oid, std::move(new_cmp_expr));
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::BuildExprTree(const std::vector<AbstractExpressionRef> &exprs) -> AbstractExpressionRef {
  if (exprs.size() == 1) {
    // fmt::print("{}\n", exprs[0]);
    return exprs[0];
  }
  auto logic_root = std::make_shared<LogicExpression>(exprs[0], exprs[1], LogicType::And);
  for (uint64_t i = 2; i < exprs.size(); i++) {
    logic_root = std::make_shared<LogicExpression>(logic_root, exprs[i], LogicType::And);
  }
  // fmt::print("{}\n", logic_root);
  return logic_root;
}

}  // namespace bustub
