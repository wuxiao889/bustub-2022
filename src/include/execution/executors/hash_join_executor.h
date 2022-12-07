//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct JoinKey {
  Value key_;
  JoinKey() = default;
  explicit JoinKey(const Value &key) : key_(key) {}
  /**
   * Compares two join keys for equality.
   * @param other the other join key to be compared with
   * @return `true` if both join keys have equivalent join-key expressions, `false` otherwise
   */
  auto operator==(const JoinKey &other) const -> bool { return key_.CompareEquals(other.key_) == CmpBool::CmpTrue; }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on JoinKey */
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &join_key) const -> std::size_t {
    return bustub::HashUtil::HashValue(&join_key.key_);
  }
};

}  // namespace std

namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_executor_;
  std::unique_ptr<AbstractExecutor> right_child_executor_;

  std::unordered_map<JoinKey, std::vector<Tuple>> left_ht_;
  std::unordered_set<JoinKey> not_joined_;

  bool build_;
  bool right_finished_;

  uint64_t cur_index_;
  std::vector<Tuple> *cur_vec_;
  Tuple right_tuple_;

  auto Insert(const Tuple &tuple) {
    JoinKey key = MakeLeftJoinKey(&tuple);
    left_ht_[key].emplace_back(tuple);
    not_joined_.insert(key);
  }

  auto MakeLeftJoinKey(const Tuple *tuple) -> JoinKey {
    JoinKey value{plan_->LeftJoinKeyExpression().Evaluate(tuple, left_child_executor_->GetOutputSchema())};
    // fmt::print("left:{} {{{}}} {}\n", tuple->ToString(&left_child_executor_->GetOutputSchema()), values[0],
    //            std::hash<JoinKey>()({values}));
    return value;
  }

  auto MakeRightJoinKey(const Tuple *tuple) -> JoinKey {
    JoinKey value{plan_->RightJoinKeyExpression().Evaluate(tuple, right_child_executor_->GetOutputSchema())};
    return value;
  }

  auto GenerateValue(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                     const Schema &right_schema) -> std::vector<Value>;
};

}  // namespace bustub

/*

The high-level idea of the hash join algorithm is to use a hash table to split up the tuples into smaller chunks
based on their join attribute(s).

This reduces the number of comparisons that the DBMS needs to perform per tuple to compute the join.
Hash joins can only be used for equi-joins on the complete join key.

If tuple r ∈ R and a tuple s ∈ S satisfy the join condition, then they have the same value for the join
attributes.
If that value is hashed to some value i, the R tuple has to be in bucket ri , and the S tuple has to be in bucket si.

Thus, the R tuples in bucket ri need only to be compared with the S tuples in bucket si.


Basic Hash Join
• Phase #1 – Build: First, scan the outer relation and populate a hash table using the hash function
h1 on the join attributes. The key in the hash table is the join attributes. The value depends on the
implementation (can be full tuple values or a tuple id).

• Phase #2 – Probe: Scan the inner relation and use the hash function h1 on each tuple’s join attributes
to jump to the corresponding location in the hash table and find a matching tuple. Since there may be
collisions in the hash table, the DBMS will need to examine the original values of the join attribute(s)
to determine whether tuples are truly matching.

If the DBMS knows the size of the outer table, the join can use a static hash table. If it does not know the
size, then the join has to use a dynamic hash table or allow for overflow page

join reorder 重新排列plan left right join顺序，选择索引。 根据大小重排left right

bustub> EXPLAIN SELECT * FROM (t1 INNER JOIN t2 ON t1.x = t2.x) INNER JOIN t3 ON t2.y = t3.y;
=== PLANNER ===
Projection { exprs=[#0.0, #0.1, #0.2, #0.3, #0.4, #0.5] } | (t1.x:INTEGER, t1.y:INTEGER, t2.x:INTEGER, t2.y:INTEGER,
t3.x:INTEGER, t3.y:INTEGER) NestedLoopJoin { type=Inner, predicate=(#0.3=#1.1) } | (t1.x:INTEGER, t1.y:INTEGER,
t2.x:INTEGER, t2.y:INTEGER, t3.x:INTEGER, t3.y:INTEGER) NestedLoopJoin { type=Inner, predicate=(#0.0=#1.0) } |
(t1.x:INTEGER, t1.y:INTEGER, t2.x:INTEGER, t2.y:INTEGER) SeqScan { table=t1 } | (t1.x:INTEGER, t1.y:INTEGER) SeqScan {
table=t2 } | (t2.x:INTEGER, t2.y:INTEGER) SeqScan { table=t3 } | (t3.x:INTEGER, t3.y:INTEGER)
=== OPTIMIZER ===
HashJoin { type=Inner, left_key=#0.3, right_key=#0.1 } | (t1.x:INTEGER, t1.y:INTEGER, t2.x:INTEGER, t2.y:INTEGER,
t3.x:INTEGER, t3.y:INTEGER) HashJoin { type=Inner, left_key=#0.0, right_key=#0.0 } | (t1.x:INTEGER, t1.y:INTEGER,
t2.x:INTEGER, t2.y:INTEGER) SeqScan { table=t1 } | (t1.x:INTEGER, t1.y:INTEGER) SeqScan { table=t2 } | (t2.x:INTEGER,
t2.y:INTEGER) SeqScan { table=t3 } | (t3.x:INTEGER, t3.y:INTEGER)bustub> EXPLAIN SELECT * FROM (t1 INNER JOIN t2 ON t1.x
= t2.x) INNER JOIN t3 ON t2.y = t3.y;

*/
