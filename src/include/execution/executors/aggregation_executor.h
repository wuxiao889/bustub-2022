//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.h
//
// Identification: src/include/execution/executors/aggregation_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class SimpleAggregationHashTable {
 public:
  /**
   * Construct a new SimpleAggregationHashTable instance.
   * @param agg_exprs the aggregation expressions
   * @param agg_types the types of aggregations
   */
  SimpleAggregationHashTable(const std::vector<AbstractExpressionRef> &agg_exprs,
                             const std::vector<AggregationType> &agg_types)
      : agg_exprs_{agg_exprs}, agg_types_{agg_types} {}

  // When performing aggregation on an empty table, CountStarAggregate
  // should return zero and all other aggregate types should return integer_null.
  // This is why GenerateInitialAggregateValue initializes most aggregate values as NULL.

  /** @return The initial aggregrate value for this aggregation executor */
  auto GenerateInitialAggregateValue() -> AggregateValue {
    std::vector<Value> values{};
    for (const auto &agg_type : agg_types_) {
      switch (agg_type) {
        case AggregationType::CountStarAggregate:
          // Count start starts at zero.
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
        case AggregationType::CountAggregate:
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          // Others starts at null.
          values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break;
      }
    }
    return {values};
  }

  /**
   * TODO(Student)
   *
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   */
  void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
    for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
      auto &result_agg = result->aggregates_[i];
      auto &input_agg = input.aggregates_[i];

      fmt::print("{}:{} aggtype:{:12} key:{:8} value:{:8}\n", "CombineAggregateValues()", __LINE__, agg_types_[i],
                 result_agg, input_agg);

      if (input_agg.IsNull()) {
        continue;
      }

      if (result_agg.IsNull()) {
        if (agg_types_[i] == AggregationType::CountAggregate) {
          result_agg = ValueFactory::GetIntegerValue(1);
        } else {
          result_agg = input_agg;
        }
        continue;
      }

      switch (agg_types_[i]) {
        case AggregationType::CountStarAggregate:
          result_agg = result_agg.Add(ValueFactory::GetIntegerValue(1));
          break;
        case AggregationType::CountAggregate:
          result_agg = result_agg.Add(ValueFactory::GetIntegerValue(1));
          break;
        case AggregationType::SumAggregate:
          result_agg = result_agg.Add(input_agg);
          break;
        case AggregationType::MinAggregate:
          result_agg = result_agg.Min(input_agg);
          break;
        case AggregationType::MaxAggregate:
          result_agg = result_agg.Max(input_agg);
          break;
      }
    }
  }

  void MakeEmpty(const AggregateKey &agg_key) { ht_.insert({agg_key, GenerateInitialAggregateValue()}); }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  void InsertCombine(const AggregateKey &agg_key, const AggregateValue &agg_val) {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
    CombineAggregateValues(&ht_[agg_key], agg_val);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const AggregateKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const AggregateValue & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

  auto Empty() -> bool { return ht_.empty(); }

  auto Size() -> int { return ht_.size(); }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<AggregateKey, AggregateValue> ht_{};
  /** The aggregate expressions that we have */
  const std::vector<AbstractExpressionRef> &agg_exprs_;
  /** The types of aggregations that we have */
  const std::vector<AggregationType> &agg_types_;
};

/**
 * AggregationExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX)
 * over the tuples produced by a child executor.
 */
class AggregationExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new AggregationExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
   */
  AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                      std::unique_ptr<AbstractExecutor> &&child);

  /** Initialize the aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the insert.
   * @param[out] tuple The next tuple produced by the aggregation
   * @param[out] rid The next tuple RID produced by the aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the aggregation */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  /** Do not use or remove this function, otherwise you will get zero points. */
  auto GetChildExecutor() const -> const AbstractExecutor *;

 private:
  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple) -> AggregateKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->GetGroupBys()) {
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an AggregateValue */
  auto MakeAggregateValue(const Tuple *tuple) -> AggregateValue {
    std::vector<Value> vals;
    for (const auto &expr : plan_->GetAggregates()) {
      vals.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {vals};
  }

 private:
  /** The aggregation plan node */
  const AggregationPlanNode *plan_;
  /** The child executor that produces tuples over which the aggregation is computed */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** Simple aggregation hash table */
  SimpleAggregationHashTable aht_;
  /** Simple aggregation hash table iterator */
  SimpleAggregationHashTable::Iterator aht_iterator_;
};
}  // namespace bustub

/*

1.  The planner will plan having as a FilterPlanNode. Aggregation executor only needs to do aggregation for each group
of input. It has exactly one child.

2. The schema of aggregation is group-by columns, followed by aggregation columns. ???
3. we make the simplifying assumption that the aggregation hash table fits entirely in memory. You can also assume that
all of your aggregation results can reside in an in-memory hash table

Recall that, in the context of a query plan, aggregations are pipeline breakers.
This may influence the way that you use the AggregationExecutor::Init() and AggregationExecutor::Next() functions in
your implementation. In particular, think about whether the Build phase of the aggregation should be performed in
AggregationExecutor::Init() or AggregationExecutor::Next(). Init()

You must consider how to handle NULLs in the input of the aggregation functions
(i.e., a tuple may have a NULL value for the attribute used in the aggregation function).
See test cases for expected behavior. Group-by columns will never be NULL.

When performing aggregation on an empty table, CountStarAggregate should return zero and all other aggregate types
should return integer_null.
This is why GenerateInitialAggregateValue initializes most aggregate values as NULL.

group by/ distinct
agg join算子，pipeline breaker




bustub> EXPLAIN SELECT colA, MIN(colB) FROM __mock_table_1 GROUP BY colA;
 === PLANNER ===
 Projection { exprs=[#0.0, #0.1] } | (__mock_table_1.colA:INTEGER, <unnamed>:INTEGER)
   Agg { types=[min], aggregates=[#0.1], group_by=[#0.0] } | (__mock_table_1.colA:INTEGER, agg#0:INTEGER)
     MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
 === OPTIMIZER ===
 Agg { types=[min], aggregates=[#0.1], group_by=[#0.0] } | (__mock_table_1.colA:INTEGER, <unnamed>:INTEGER)
   MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)

bustub> EXPLAIN SELECT COUNT(colA), min(colB) FROM __mock_table_1;
 === PLANNER ===
 Projection { exprs=[#0.0, #0.1] } | (<unnamed>:INTEGER, <unnamed>:INTEGER)
   Agg { types=[count, min], aggregates=[#0.0, #0.1], group_by=[] } | (agg#0:INTEGER, agg#1:INTEGER)
     MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
 === OPTIMIZER ===
 Agg { types=[count, min], aggregates=[#0.0, #0.1], group_by=[] } | (<unnamed>:INTEGER, <unnamed>:INTEGER)
   MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)

bustub> EXPLAIN SELECT colA, MIN(colB) FROM __mock_table_1 GROUP BY colA HAVING MAX(colB) > 10;
 === PLANNER ===
 Projection { exprs=[#0.0, #0.2] } | (__mock_table_1.colA:INTEGER, <unnamed>:INTEGER)
   Filter { predicate=(#0.1>10) } | (__mock_table_1.colA:INTEGER, agg#0:INTEGER, agg#1:INTEGER)
     Agg { types=[max, min], aggregates=[#0.1, #0.1], group_by=[#0.0] } | (__mock_table_1.colA:INTEGER, agg#0:INTEGER,
agg#1:INTEGER) MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
 === OPTIMIZER ===
 Projection { exprs=[#0.0, #0.2] } | (__mock_table_1.colA:INTEGER, <unnamed>:INTEGER)
   Filter { predicate=(#0.1>10) } | (__mock_table_1.colA:INTEGER, agg#0:INTEGER, agg#1:INTEGER)
     Agg { types=[max, min], aggregates=[#0.1, #0.1], group_by=[#0.0] } | (__mock_table_1.colA:INTEGER, agg#0:INTEGER,
agg#1:INTEGER) MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)

bustub> EXPLAIN SELECT DISTINCT colA, colB FROM __mock_table_1;
 === PLANNER ===
 Agg { types=[], aggregates=[], group_by=[#0.0, #0.1] } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
   Projection { exprs=[#0.0, #0.1] } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
     MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
 === OPTIMIZER ===
 Agg { types=[], aggregates=[], group_by=[#0.0, #0.1] } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
   MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
*/