
## Q1 Where's the Index?
```
Welcome to the BusTub shell! Type \help to learn more.

bustub> CREATE TABLE t1(x INT, y INT);
 Table created with id = 22 
bustub> CREATE TABLE t2(x INT, y INT);
 Table created with id = 23 
bustub> CREATE TABLE t3(x INT, y INT);
 Table created with id = 24 

bustub> CREATE INDEX t1x ON t1(x);
 Index created with id = 0 

bustub> SELECT * FROM (t1 INNER JOIN t2 ON t1.x = t2.x) INNER JOIN t3 ON t2.y = t3.y;
SELECT * FROM (t1 INNER JOIN t2 ON t1.x = t2.x) INNER JOIN t3 ON t2.y = t3.y;
NestedLoopJoin { type=Inner, predicate=(#0.3=#1.1) } | (t1.x:INTEGER, t1.y:INTEGER, t2.x:INTEGER, t2.y:INTEGER, t3.x:INTEGER, t3.y:INTEGER)
  NestedLoopJoin { type=Inner, predicate=(#0.0=#1.0) } | (t1.x:INTEGER, t1.y:INTEGER, t2.x:INTEGER, t2.y:INTEGER)
    SeqScan { table=t1 } | (t1.x:INTEGER, t1.y:INTEGER)
    SeqScan { table=t2 } | (t2.x:INTEGER, t2.y:INTEGER)
  SeqScan { table=t3 } | (t3.x:INTEGER, t3.y:INTEGER)
```

Even though there is an index on t1.x, BusTub does not pick it for the join! 

What a 💩 database system! Furthermore, there are two nested loop joins, which is extremely inefficient! Oops!

因为t1不是inner_table，不会用索引

Recommended Optimizations: Use hash join to handle equi-condition; 
- join reordering to pick the index for t1; join t2 and t3 first based on the cardinality (use EstimatedCardinality function). 
- We also have an existing rule for converting NLJ into HashJoin and you will need to manually enable it. See optimizer_custom_rules.cpp for more information.



## Q2 Too Many Joins!
```
bustub> CREATE TABLE t4(x int, y int);
 Table created with id = 22 
bustub> CREATE TABLE t5(x int, y int);
 Table created with id = 23 
bustub> CREATE TABLE t6(x int, y int);
 Table created with id = 24 

bustub> SELECT * FROM t4, t5, t6
...   WHERE (t4.x = t5.x) AND (t5.y = t6.y) AND (t4.y >= 1000000)
...     AND (t4.y < 1500000) AND (t6.x >= 100000) AND (t6.x < 150000);

SELECT * FROM t4, t5, t6   WHERE (t4.x = t5.x) AND (t5.y = t6.y) AND (t4.y >= 1000000)     AND (t4.y < 1500000) AND (t6.x >= 100000) AND (t6.x < 150000);
bool bustub::BustubInstance::ExecuteSqlTxn(const std::string &, bustub::ResultWriter &, bustub::Transaction *):328
NestedLoopJoin { type=Inner, predicate=((((((#0.0=#0.2)and(#0.3=#1.1))and(#0.1>=1000000))and(#0.1<1500000))and(#1.0>=100000))and(#1.0<150000)) } | (t4.x:INTEGER, t4.y:INTEGER, t5.x:INTEGER, t5.y:INTEGER, t6.x:INTEGER, t6.y:INTEGER)
  NestedLoopJoin { type=Inner, predicate=true } | (t4.x:INTEGER, t4.y:INTEGER, t5.x:INTEGER, t5.y:INTEGER)
    SeqScan { table=t4 } | (t4.x:INTEGER, t4.y:INTEGER)
    SeqScan { table=t5 } | (t5.x:INTEGER, t5.y:INTEGER)
  SeqScan { table=t6 } | (t6.x:INTEGER, t6.y:INTEGER)
```

Recommended Optimizations: Decompose filter condition to extract hash join keys, 

push down filter below hash join to reduce data from the table scan.

## Q3  The Mad Data Scientist

```
CREATE TABLE t7(v1 int, v2 int);
CREATE TABLE t8(v4 int);

SELECT v, d1, d2 FROM (
  SELECT v,
         MAX(v1) AS d1, MIN(v1), MAX(v2), MIN(v2),
         MAX(v1) + MIN(v1), MAX(v2) + MIN(v2),
         MAX(v1) + MAX(v1) + MAX(v2) AS d2
    FROM t7 LEFT JOIN (SELECT v4 FROM t8 WHERE 1 == 2) ON v < v4
    GROUP BY v
);

bool bustub::BustubInstance::ExecuteSqlTxn(const std::string &, bustub::ResultWriter &, bustub::Transaction *):328
Projection { exprs=[#0.0, #0.1, #0.7] } | (__subquery#0.t7.v:INTEGER, __subquery#0.d1:INTEGER, __subquery#0.d2:INTEGER)
  Projection { exprs=[#0.0, #0.1, #0.2, #0.3, #0.4, (#0.5+#0.6), (#0.7+#0.8), ((#0.9+#0.10)+#0.11)] } | (__subquery#0.t7.v:INTEGER, __subquery#0.d1:INTEGER, __subquery#0.__item#2:INTEGER, __subquery#0.__item#3:INTEGER, __subquery#0.__item#4:INTEGER, __subquery#0.__item#5:INTEGER, __subquery#0.__item#6:INTEGER, __subquery#0.d2:INTEGER)
    Agg { types=[max, min, max, min, max, min, max, min, max, max, max], aggregates=[#0.1, #0.1, #0.2, #0.2, #0.1, #0.1, #0.2, #0.2, #0.1, #0.1, #0.2], group_by=[#0.0] } | (t7.v:INTEGER, agg#0:INTEGER, agg#1:INTEGER, agg#2:INTEGER, agg#3:INTEGER, agg#4:INTEGER, agg#5:INTEGER, agg#6:INTEGER, agg#7:INTEGER, agg#8:INTEGER, agg#9:INTEGER, agg#10:INTEGER)
      NestedLoopJoin { type=Left, predicate=(#0.0<#1.0) } | (t7.v:INTEGER, t7.v1:INTEGER, t7.v2:INTEGER, __subquery#1.t8.v4:INTEGER)
        SeqScan { table=t7 } | (t7.v:INTEGER, t7.v1:INTEGER, t7.v2:INTEGER)
        Filter { predicate=(1=2) } | (__subquery#1.t8.v4:INTEGER)
          SeqScan { table=t8 } | (t8.v4:INTEGER)
```



```
updating: src/include/execution/executors/aggregation_executor.h (deflated 74%)
updating: src/include/execution/executors/delete_executor.h (deflated 61%)
updating: src/include/execution/executors/filter_executor.h (deflated 63%)
updating: src/include/execution/executors/hash_join_executor.h (deflated 64%)
updating: src/include/execution/executors/index_scan_executor.h (deflated 58%)
updating: src/include/execution/executors/insert_executor.h (deflated 61%)
updating: src/include/execution/executors/limit_executor.h (deflated 61%)
updating: src/include/execution/executors/nested_index_join_executor.h (deflated 60%)
updating: src/include/execution/executors/nested_loop_join_executor.h (deflated 76%)
updating: src/include/execution/executors/seq_scan_executor.h (deflated 62%)
updating: src/include/execution/executors/sort_executor.h (deflated 62%)
updating: src/include/execution/executors/topn_executor.h (deflated 57%)
updating: src/execution/aggregation_executor.cpp (deflated 63%)
updating: src/execution/delete_executor.cpp (deflated 58%)
updating: src/execution/filter_executor.cpp (deflated 53%)
updating: src/execution/hash_join_executor.cpp (deflated 66%)
updating: src/execution/index_scan_executor.cpp (deflated 59%)
updating: src/execution/insert_executor.cpp (deflated 62%)
updating: src/execution/limit_executor.cpp (deflated 58%)
updating: src/execution/nested_index_join_executor.cpp (deflated 64%)
updating: src/execution/nested_loop_join_executor.cpp (deflated 68%)
updating: src/execution/seq_scan_executor.cpp (deflated 56%)
updating: src/execution/sort_executor.cpp (deflated 59%)
updating: src/execution/topn_executor.cpp (deflated 59%)
updating: src/include/optimizer/optimizer.h (deflated 65%)
updating: src/optimizer/optimizer_custom_rules.cpp (deflated 47%)
updating: src/optimizer/sort_limit_as_topn.cpp (deflated 61%)
updating: src/include/storage/page/b_plus_tree_page.h (deflated 63%)
updating: src/storage/page/b_plus_tree_page.cpp (deflated 67%)
updating: src/include/storage/page/b_plus_tree_internal_page.h (deflated 61%)
updating: src/storage/page/b_plus_tree_internal_page.cpp (deflated 72%)
updating: src/include/storage/page/b_plus_tree_leaf_page.h (deflated 64%)
updating: src/storage/page/b_plus_tree_leaf_page.cpp (deflated 71%)
updating: src/include/storage/index/index_iterator.h (deflated 63%)
updating: src/storage/index/index_iterator.cpp (deflated 71%)
updating: src/include/storage/index/b_plus_tree.h (deflated 69%)
updating: src/storage/index/b_plus_tree.cpp (deflated 81%)
updating: src/include/container/hash/extendible_hash_table.h (deflated 73%)
updating: src/container/hash/extendible_hash_table.cpp (deflated 73%)
updating: src/include/buffer/lru_k_replacer.h (deflated 65%)
updating: src/buffer/lru_k_replacer.cpp (deflated 74%)
updating: src/include/buffer/buffer_pool_manager_instance.h (deflated 67%)
updating: src/buffer/buffer_pool_manager_instance.cpp (deflated 74%)
Built target submit-p3
```