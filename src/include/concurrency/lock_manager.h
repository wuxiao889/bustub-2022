//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /** Whether the lock has been granted or not */
    bool granted_{false};

    auto ToString() -> std::string;
  };

  /*
      We can avoid starvation of transactions by granting locks in the following manner:
    When a transaction Ti requests a lock on a data item Q in a particular mode M, the
    concurrency-control manager grants the lock provided that:

        • There is no other transaction holding a lock on Q in a mode that conflicts with M.
        • There is no other transaction that is waiting for a lock on Q and that made its lock
        request before Ti.

      Note that a transaction attempting to upgrade a lock on an item Q may be forced
    to wait. This enforced wait occurs if Q is currently locked by another transaction in
    shared mode.


      A simple but widely used scheme automatically generates the appropriate lock and
    unlock instructions for a transaction, on the basis of read and write requests from the
    transaction:
      • When a transaction Ti issues a read(Q) operation, the system issues a lock-S(Q)
      instruction followed by the read(Q) instruction.
      • When Ti issues a write(Q) operation, the system checks to see whether Ti already
      holds a shared lock on Q. If it does, then the system issues an upgrade(Q) in-
      struction, followed by the write(Q) instruction. Otherwise, the system issues a
      lock-X(Q) instruction, followed by the write(Q) instruction.
      • All locks obtained by a transaction are unlocked after that transaction commits or
      aborts.


    1. When a lock request message arrives, it adds a record to the end of the linked list
      for the data item, if the linked list is present. Otherwise it creates a new linked list,
      containing only the record for the request.

    2. It always grants a lock request on a data item that is not currently locked. But
      if the transaction requests a lock on an item on which a lock is currently held,
      the lock manager grants the request only if it is compatible with the locks that are
      currently held, and all earlier requests have been granted already. Otherwise the
      request has to wait.

    3. When the lock manager receives an unlock message from a transaction, it deletes
      the record for that data item in the linked list corresponding to that transaction. It
      tests the record that follows, if any, as described in the previous paragraph, to see
      if that request can now be granted. If it can, the lock manager grants that request
      and processes the record following it, if any, similarly, and so on.

    4. If a transaction aborts, the lock manager deletes any waiting request made by the
      transaction. Once the database system has taken appropriate actions to undo the
      transaction (see Section 19.3), it releases all locks held by the aborted transaction.
  */

  class LockRequestQueue {
   public:
    using ListType = std::list<std::shared_ptr<LockRequest>>;
    /** List of lock requests for the same resource (table or row) */
    ListType request_queue_;
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    std::mutex latch_;

    auto CheckCompatibility(LockMode lock_mode, ListType::iterator ite) -> bool;
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
  }

  /*
   The multiple-granularity locking protocol uses these lock modes to ensure serializ-
     ability. It requires that a transaction Ti that attempts to lock a node Q must follow these
     rules:
     • Transaction Ti must observe the lock-compatibility function of Figure 18.16.
     • Transaction Ti must lock the root of the tree first and can lock it in any mode.
     • Transaction Ti can lock a node Q in S or IS mode only if Ti currently has the parent
     of Q locked in either IX or IS mode.
     • Transaction Ti can lock a node Q in X, SIX, or IX mode only if Ti currently has the
     parent of Q locked in either IX or SIX mode.
     • Transaction Ti can lock a node only if Ti has not previously unlocked any node
     (i.e., Ti is two phase).
     • Transaction Ti can unlock a node Q only if Ti currently has none of the children
     of Q locked.
  */

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *
   *
   * MULTIPLE TRANSACTIONS: 只要遵守FIFO，所有兼容的请求应该同时授予
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *
   * SUPPORTED LOCK MODES: 行不能有意向锁
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *
   *
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *
   *    REPEATABLE_READ: phantoms may happen
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *
   *    READ_COMMITTED: phantoms, unrepeatable-read
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *
   *    READ_UNCOMMITTED: phantoms, unrepeatable reads, dirty reads
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *
   *
   * MULTILEVEL LOCKING: 行锁与表锁要对应
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *
   *    A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
   *
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *
   *    REPEATABLE_READ: repeatable reads，no dirty reads
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:  no repeatable reads
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state. (unrepeatable read)
   *
   *   READ_UNCOMMITTED: (dirty read)
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool;

  /*** Graph API ***/

  /*
  NOTES

  Your background thread should build the graph on the fly every time it wakes up. You should not be maintaining a
  graph, it should be built and destroyed every time the thread wakes up.

  Your DFS Cycle detection algorithm must be deterministic. In order to do achieve this, you must always choose to
  explore the lowest transaction id first.
  This means when choosing which unexplored node to run DFS from, always choose the node with the lowest transaction id.
  This also means when exploring neighbors, explore them in sorted order from lowest to highest.

  When you find a cycle, you should abort the youngest/newest transaction to break the cycle by setting that
  transactions state to ABORTED.

  When your detection thread wakes up, it is responsible for breaking all cycles that exist.
  If you follow the above requirements, you will always find the cycles in a deterministic order.
  This also means that when you are building your graph, you should not add nodes
   for aborted transactions or draw edges to aborted transactions.

  Your background cycle detection algorithm may need to get a pointer to a transaction using a txn_id. We have added a
  static method Transaction* GetTransaction(txn_id_t txn_id) to let you do that.

  You can use std::this_thread::sleep_for to order threads to write test cases. You can also tweak
  CYCLE_DETECTION_INTERVAL in common/config.h in your test cases.
  */

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * Adds an edge in your graph from t1 to t2. If the edge already exists, you don't have to do anything.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * Removes edge t1 to t2 from your graph. If no such edge exists, you don't have to do anything.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

 private:
  /** Fall 2022 */
  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  /** Waits-for graph representation. */
  // t1 <- t2
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;  //

  std::vector<txn_id_t> waits_;
  std::unordered_map<txn_id_t, std::variant<table_oid_t, RID>> txn_variant_map_;
  std::mutex txn_variant_map_latch_;  //
};

}  // namespace bustub
