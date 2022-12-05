//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// values_plan.h
//
// Identification: src/include/execution/plans/values_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * The ValuesPlanNode represents rows of values. For example,
 * `INSERT INTO table VALUES ((0, 1), (1, 2))`, where we will have
 * `(0, 1)` and `(1, 2)` as the output of this executor.
 */
class ValuesPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new ValuesPlanNode instance.
   * @param output The output schema of this values plan node
   * @param values The values produced by this plan node
   */
  explicit ValuesPlanNode(SchemaRef output, std::vector<std::vector<AbstractExpressionRef>> values)
      : AbstractPlanNode(std::move(output), {}), values_(std::move(values)) {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::Values; }

  auto GetValues() const -> const std::vector<std::vector<AbstractExpressionRef>> & { return values_; }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(ValuesPlanNode);

  std::vector<std::vector<AbstractExpressionRef>> values_;

 protected:
  auto PlanNodeToString() const -> std::string override { return fmt::format("Values {{ rows={} }}", values_.size()); }
};

}  // namespace bustub

/*
bustub> EXPLAIN values (1, 2, 'a'), (3, 4, 'b');
 === BINDER ===
 BoundSelect {
   table=BoundExpressionListRef { identifier=__values#0, values=[[1, 2, a], [3, 4, b]] },
   columns=[__values#0.0, __values#0.1, __values#0.2],
   groupBy=[],
   having=,
   where=,
   limit=,
   offset=,
   order_by=[],
   is_distinct=false,
   ctes=,
 }
 === PLANNER ===
 Projection { exprs=[#0.0, #0.1, #0.2] } | (__values#0.0:INTEGER, __values#0.1:INTEGER, __values#0.2:VARCHAR)
   Values { rows=2 } | (__values#0.0:INTEGER, __values#0.1:INTEGER, __values#0.2:VARCHAR)
 === OPTIMIZER ===
 Values { rows=2 } | (__values#0.0:INTEGER, __values#0.1:INTEGER, __values#0.2:VARCHAR)

bustub> values (1, 2, 'a'), (3, 4, 'b');
+--------------+--------------+--------------+
| __values#0.0 | __values#0.1 | __values#0.2 |
+--------------+--------------+--------------+
| 1            | 2            | a            |
| 3            | 4            | b            |
+--------------+--------------+--------------+
bustub> CREATE TABLE table1(v1 INT, v2 INT, v3 VARCHAR(128));
 Table created with id = 23
bustub> EXPLAIN INSERT INTO table1 VALUES (1, 2, 'a'), (3, 4, 'b');
 === BINDER ===
 BoundInsert {
   table=BoundBaseTableRef { table=table1, oid=23 },
   select=  BoundSelect {
     table=BoundExpressionListRef { identifier=__values#0, values=[[1, 2, a], [3, 4, b]] },
     columns=[__values#0.0, __values#0.1, __values#0.2],
     groupBy=[],
     having=,
     where=,
     limit=,
     offset=,
     order_by=[],
     is_distinct=false,
     ctes=,
   }
 }
 === PLANNER ===
 Insert { table_oid=23 } | (__bustub_internal.insert_rows:INTEGER)
   Projection { exprs=[#0.0, #0.1, #0.2] } | (__values#0.0:INTEGER, __values#0.1:INTEGER, __values#0.2:VARCHAR)
     Values { rows=2 } | (__values#0.0:INTEGER, __values#0.1:INTEGER, __values#0.2:VARCHAR)
 === OPTIMIZER ===
 Insert { table_oid=23 } | (__bustub_internal.insert_rows:INTEGER)
   Values { rows=2 } | (__values#0.0:INTEGER, __values#0.1:INTEGER, __values#0.2:VARCHAR)
*/