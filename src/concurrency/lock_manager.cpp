//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <fmt/printf.h>
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>  // NOLINT
#include "catalog/column.h"
#include "catalog/table_generator.h"
#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "primer/p0_trie.h"

namespace bustub {

auto LockModeToString(bustub::LockManager::LockMode lock_mode) -> std::string {
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      return "SHARED";
    case LockManager::LockMode::EXCLUSIVE:
      return "EXCLUSIVE";
    case LockManager::LockMode::INTENTION_SHARED:
      return "INTENTION_SHARED";
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      return "INTENTION_EXCLUSIVE";
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return "SHARED_INTENTION_EXCLUSIVE";
  }
  return "";
}

/*
    Compatibility Matrix
        IS   IX   S   SIX   X
  IS    √    √    √    √    ×
  IX    √    √    ×    ×    ×
  S     √    ×    √    ×    ×
  SIX   √    ×    ×    ×    ×
  X     ×    ×    ×    ×    ×
*/

auto LockManager::LockRequestQueue::CheckRunnable(LockMode lock_mode, ListType::iterator ite) -> bool {
  // all earlier requests should have been granted already
  // check compatible with the locks that are currently held

  // 检查链表ite前面request.lock_mode是否和*ite兼容
  // 不需要关心前面是否granted

  //  [S S S] X S S X X
  //  [X] S S X X
  //  [S S] X X
  //  [X] X
  //  [X]
  for (auto it = request_queue_.begin(); it != ite; it++) {
    const auto &request = *it;
    const auto mode = request->lock_mode_;
    switch (lock_mode) {
      case LockMode::INTENTION_SHARED:
        if (mode == LockMode::INTENTION_EXCLUSIVE) {
          return false;
        }
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        if (mode == LockMode::SHARED || mode == LockMode::SHARED_INTENTION_EXCLUSIVE || mode == LockMode::EXCLUSIVE) {
          return false;
        }
        break;
      case LockMode::SHARED:
        if (mode == LockMode::INTENTION_EXCLUSIVE || mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
            mode == LockMode::EXCLUSIVE) {
          return false;
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (mode != LockMode::INTENTION_SHARED) {
          return false;
        }
        break;
      case LockMode::EXCLUSIVE:
        return false;
    }
  }
  return true;
};

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  const auto &txn_id = txn->GetTransactionId();
  const auto &txn_level = txn->GetIsolationLevel();
  const auto &txn_state = txn->GetState();

  switch (txn_level) {
    case IsolationLevel::REPEATABLE_READ:
      // All locks are allowed in the GROWING state
      // No locks are allowed in the SHRINKING state
      if (txn_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      // All locks are allowed in the GROWING state
      // Only IS, S locks are allowed in the SHRINKING state
      if (txn_state == TransactionState::SHRINKING) {
        if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      // The transaction is required to take only IX, X locks.
      // X, IX locks are allowed in the GROWING state.
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
  }

  std::shared_ptr<LockRequestQueue> lrque;
  // std::lock_guard lock(table_lock_map_latch_);
  table_lock_map_latch_.lock();
  auto table_lock_map_it = table_lock_map_.find(oid);
  LOG_DEBUG("\033[0;34m%d: txn %d locktable %d %s\033[0m\n", ::gettid(), txn_id, oid,
            LockModeToString(lock_mode).c_str());

  bool upgrade = false;
  LockMode old_lock_mode;

  auto lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
  // txn_time_map_[lock_request->txn_id_] = lock_request->timestamp_;
  txn_variant_map_[txn_id] = oid;
  if (table_lock_map_it != table_lock_map_.end()) {
    lrque = table_lock_map_it->second;
    table_lock_map_latch_.unlock();
    std::unique_lock lock(lrque->latch_);

    auto &request_queue = lrque->request_queue_;
    decltype(request_queue.begin()) request_it;
    // check upgrade
    for (request_it = request_queue.begin(); request_it != request_queue.end(); ++request_it) {
      const auto &request = *request_it;

      if (!request->granted_) {
        break;
      }

      if (request->txn_id_ == txn_id) {
        old_lock_mode = request->lock_mode_;
        if (old_lock_mode == lock_mode) {
          return true;
        }
        switch (old_lock_mode) {
            //  IS -> [S, X, IX, SIX]
            //  S -> [X, SIX]
            //  IX -> [X, SIX]
            //  SIX -> [X]
          case LockMode::SHARED:
          case LockMode::INTENTION_EXCLUSIVE:
            if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
              txn->SetState(TransactionState::ABORTED);
              throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
            }
            break;
          case LockMode::SHARED_INTENTION_EXCLUSIVE:
            if (lock_mode != LockMode::EXCLUSIVE) {
              txn->SetState(TransactionState::ABORTED);
              throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
            }
            break;
          case LockMode::EXCLUSIVE:
          case LockMode::INTENTION_SHARED:
            break;
        }
        upgrade = true;
        break;
      }
    }

    if (upgrade) {
      if (lrque->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      }

      lrque->upgrading_ = lock_request->txn_id_;

      auto it = request_it;
      for (; it != request_queue.end(); ++it) {
        const auto &request = *request_it;
        if (!request->granted_) {
          break;
        }
      }
      // A lock request being upgraded should be
      // prioritised over other waiting lock requests on the same resource.
      request_queue.erase(request_it);
      request_it = request_queue.insert(it, lock_request);
    } else {
      request_it = request_queue.insert(request_queue.end(), lock_request);
    }
    // void wait( std::unique_lock<std::mutex>& lock, Predicate stop_waiting );
    lrque->cv_.wait(lock, [&]() {
      if (txn->GetState() == TransactionState::ABORTED) {
        return true;
      }
      return lrque->CheckRunnable(lock_mode, request_it);
    });

    if (txn->GetState() == TransactionState::ABORTED) {
      LOG_DEBUG("\033[0;31m%d: txn %d locktable %d %s abort\033[0m\n", ::gettid(), txn_id, oid,
                LockModeToString(lock_mode).c_str());
      request_queue.erase(request_it);
      return false;
    }

    lock_request->granted_ = true;
    if (upgrade) {
      lrque->upgrading_ = INVALID_TXN_ID;
    }

  } else {
    lrque = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = lrque;
    lrque->request_queue_.push_back(lock_request);
    lock_request->granted_ = true;
    table_lock_map_latch_.unlock();
  }

  LOG_DEBUG("\033[0;44m%d: txn %d locktable %d %s success\033[0m\n", ::gettid(), txn_id, oid,
            LockModeToString(lock_mode).c_str());

  if (upgrade) {
    switch (old_lock_mode) {
      case LockMode::INTENTION_SHARED:
        txn->GetIntentionSharedTableLockSet()->erase(oid);
        break;
      case LockMode::SHARED:
        txn->GetSharedTableLockSet()->erase(oid);
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      case LockMode::EXCLUSIVE:
        break;
    }
  }

  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  const auto txn_id = txn->GetTransactionId();
  const auto txn_level = txn->GetIsolationLevel();
  LockMode lock_mode;

  table_lock_map_latch_.lock();
  LOG_DEBUG("\033[0;32m%d: txn %d unlocktable %d\033[0m\n", ::gettid(), txn_id, oid);
  auto table_lock_map_it = table_lock_map_.find(oid);
  // Both should ensure that the transaction currently
  // holds a lock on the resource it is attempting to unlock.
  if (table_lock_map_it == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto &lrque = table_lock_map_it->second;
  table_lock_map_latch_.unlock();

  {
    std::unique_lock lock(lrque->latch_);

    auto &request_queue = lrque->request_queue_;
    auto it = std::find_if(request_queue.begin(), request_queue.end(),
                           [txn_id](const auto &lock_request) { return lock_request->txn_id_ == txn_id; });
    if (it == request_queue.end() || !(*it)->granted_) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
    const auto &lock_request = *it;
    lock_mode = lock_request->lock_mode_;

    std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> lock_set;
    switch (lock_mode) {
      case LockMode::INTENTION_SHARED:
      case LockMode::SHARED:
        lock_set = txn->GetSharedRowLockSet();
        break;
      case LockMode::INTENTION_EXCLUSIVE:
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
      case LockMode::EXCLUSIVE:
        lock_set = txn->GetExclusiveRowLockSet();
    }

    // unlocking a table should only be allowed if the transaction does not hold locks on any
    // row on that table.
    if (lock_set->find(oid) != lock_set->end() && !lock_set->at(oid).empty()) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    }

    // Finally, unlocking a resource should also grant any new lock requests
    // for the resource (if possible).
    request_queue.erase(it);
    lrque->cv_.notify_all();
  }
  LOG_DEBUG("\033[0;42m%d: txn %d unlocktable %d success\033[0m\n", ::gettid(), txn_id, oid);

  if (txn->GetState() == TransactionState::GROWING) {
    // TRANSACTION STATE UPDATE
    switch (txn_level) {
      case IsolationLevel::REPEATABLE_READ:
        if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_UNCOMMITTED:
      case IsolationLevel::READ_COMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
    }
  }

  // After a resource is unlocked, lock manager should update the transaction's
  // lock sets appropriately
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  const auto &txn_id = txn->GetTransactionId();
  const auto &txn_level = txn->GetIsolationLevel();
  const auto &txn_state = txn->GetState();
  LOG_DEBUG("\033[0;33m%d: txn %d table %d lockrow [%d,%d] %s\033[0m\n", ::gettid(), txn_id, oid, rid.GetPageId(),
            rid.GetSlotNum(), LockModeToString(lock_mode).c_str());
  // Row locking should not support Intention locks.
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  switch (txn_level) {
    case IsolationLevel::REPEATABLE_READ:
      // All locks are allowed in the GROWING state
      // No locks are allowed in the SHRINKING state
      if (txn_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      // All locks are allowed in the GROWING state
      // Only IS, S locks are allowed in the SHRINKING state
      if (txn_state == TransactionState::SHRINKING) {
        if (lock_mode != LockMode::SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      // The transaction is required to take only IX, X locks.
      // X, IX locks are allowed in the GROWING state.
      if (lock_mode == LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
  }

  if (lock_mode == LockMode::SHARED) {
    if (!txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  } else {
    if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  std::shared_ptr<LockRequestQueue> lrque;

  row_lock_map_latch_.lock();
  auto row_lock_map_it = row_lock_map_.find(rid);

  bool upgrade = false;
  LockMode old_lock_mode;

  auto lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
  // txn_time_map_[lock_request->txn_id_] = lock_request->timestamp_;
  txn_variant_map_[txn_id] = rid;
  if (row_lock_map_it != row_lock_map_.end()) {
    lrque = row_lock_map_it->second;
    row_lock_map_latch_.unlock();
    std::unique_lock lock(lrque->latch_);

    auto &request_queue = lrque->request_queue_;
    decltype(request_queue.begin()) request_it;
    for (request_it = request_queue.begin(); request_it != request_queue.end(); ++request_it) {
      const auto &request = *request_it;
      if (!request->granted_) {
        break;
      }
      if (request->txn_id_ == txn_id) {
        old_lock_mode = request->lock_mode_;
        if (old_lock_mode == lock_mode) {
          return true;
        }
        assert(lock_mode == LockMode::EXCLUSIVE && old_lock_mode == LockMode::SHARED);
        upgrade = true;
        break;
      }
    }

    if (upgrade) {
      LOG_DEBUG("\033[0;33m%d: txn %d table %d lockrow [%d,%d] %s upgrade\033[0m\n", ::gettid(), txn_id, oid,
                rid.GetPageId(), rid.GetSlotNum(), LockModeToString(lock_mode).c_str());
      if (lrque->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      }

      lrque->upgrading_ = lock_request->txn_id_;

      auto it = request_it;
      for (; it != request_queue.end(); ++it) {
        const auto &request = *it;
        if (!request->granted_) {
          break;
        }
      }

      request_queue.erase(request_it);
      request_it = request_queue.insert(it, lock_request);
    } else {
      request_it = request_queue.insert(request_queue.end(), lock_request);
    }

    lrque->cv_.wait(lock, [&]() {
      if (txn->GetState() == TransactionState::ABORTED) {
        return true;
      }
      return lrque->CheckRunnable(lock_mode, request_it);
    });

    if (txn->GetState() == TransactionState::ABORTED) {
      LOG_DEBUG("\033[0;31m%d: txn %d table %d lockrow [%d,%d] %s abort\033[0m\n", ::gettid(), txn_id, oid,
                rid.GetPageId(), rid.GetSlotNum(), LockModeToString(lock_mode).c_str());
      request_queue.erase(request_it);
      return false;
    }

    lock_request->granted_ = true;
    if (upgrade) {
      lrque->upgrading_ = INVALID_TXN_ID;
    }
  } else {
    lrque = std::make_shared<LockRequestQueue>();
    row_lock_map_[rid] = lrque;
    lrque->request_queue_.push_back(lock_request);
    lock_request->granted_ = true;
    row_lock_map_latch_.unlock();
  }

  LOG_DEBUG("\033[0;43m%d: txn %d table %d lockrow [%d,%d] %s success\033[0m\n", ::gettid(), txn_id, oid,
            rid.GetPageId(), rid.GetSlotNum(), LockModeToString(lock_mode).c_str());

  if (upgrade) {
    txn->GetSharedLockSet()->erase(rid);
    txn->GetSharedRowLockSet()->at(oid).erase(rid);
  }

  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> lock_set;
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedLockSet()->insert(rid);
    lock_set = txn->GetSharedRowLockSet();
  } else {
    txn->GetExclusiveLockSet()->insert(rid);
    lock_set = txn->GetExclusiveRowLockSet();
  }
  (*lock_set)[oid].insert(rid);

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  const auto txn_id = txn->GetTransactionId();
  const auto txn_level = txn->GetIsolationLevel();
  LockMode lock_mode;

  row_lock_map_latch_.lock();
  auto row_lock_map_it = row_lock_map_.find(rid);
  LOG_DEBUG("\033[0;35m%d: txn %d table %d unlockrow [%d,%d]\033[0m\n", ::gettid(), txn_id, oid, rid.GetPageId(),
            rid.GetSlotNum());
  if (row_lock_map_it == row_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto &lrque = row_lock_map_it->second;
  row_lock_map_latch_.unlock();

  {
    std::unique_lock lock(lrque->latch_);
    auto &request_queue = lrque->request_queue_;
    auto it = std::find_if(request_queue.begin(), request_queue.end(),
                           [txn_id](const auto &lock_request) { return lock_request->txn_id_ == txn_id; });
    if (it == request_queue.end() || !(*it)->granted_) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }

    lock_mode = it->get()->lock_mode_;
    request_queue.erase(it);
    lrque->cv_.notify_all();
    LOG_DEBUG("\033[0;45m%d: txn %d table %d unlockrow [%d,%d] success\033[0m\n", ::gettid(), txn_id, oid,
              rid.GetPageId(), rid.GetSlotNum());
  }

  if (txn->GetState() == TransactionState::GROWING) {
    // TRANSACTION STATE UPDATE
    switch (txn_level) {
      case IsolationLevel::REPEATABLE_READ:
        txn->SetState(TransactionState::SHRINKING);
      case IsolationLevel::READ_UNCOMMITTED:
      case IsolationLevel::READ_COMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
    }
  }

  // After a resource is unlocked, lock manager should update the transaction's lock sets
  // appropriately (check transaction.h)
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> lock_set;
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedLockSet()->erase(rid);
    lock_set = txn->GetSharedRowLockSet();
  } else {
    txn->GetExclusiveLockSet()->erase(rid);
    lock_set = txn->GetExclusiveRowLockSet();
  }
  lock_set->at(oid).erase(rid);

  return true;
}

// t1 wait for t2
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto &vec = waits_for_[t2];
  auto it = std::find(vec.begin(), vec.end(), t1);
  if (it == vec.end()) {
    // LOG_DEBUG("add edge %d -> %d\n", t1, t2);
    vec.push_back(t1);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto &vec = waits_for_[t2];
  auto it = std::find(vec.begin(), vec.end(), t1);
  if (it != vec.end()) {
    // LOG_DEBUG("remove edge %d -> %d\n", t1, t2);
    vec.erase(it);
  }
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto &[t2, vec] : waits_for_) {
    for (auto t1 : vec) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  if (waits_.empty()) {
    return false;
  }

  std::unordered_set<txn_id_t> visted;

  std::function<bool(txn_id_t)> helper = [&](txn_id_t tid) -> bool {
    auto it = waits_for_.find(tid);
    if (it == waits_for_.end()) {
      return false;
    }
    const auto &waits_for = waits_for_[tid];

    for (const auto &waits_for_txn_id : waits_for) {
      if (visted.count(waits_for_txn_id) == 1) {
        *txn_id = waits_for_txn_id;
        for (const auto &visted_txn_id : visted) {
          *txn_id = std::max(*txn_id, visted_txn_id);
        }
        return true;
      }

      visted.insert(waits_for_txn_id);
      if (helper(waits_for_txn_id)) {
        return true;
      }
      visted.erase(waits_for_txn_id);
    }
    return false;
  };

  return helper(waits_.front());
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::lock_guard lock(waits_for_latch_);
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      LOG_DEBUG("\033[0;41m RunCycleDetection\033[0m\n");

      auto add_edges = [&](const auto &map) {
        for (const auto &[_, que] : map) {
          std::lock_guard lock(que->latch_);
          std::vector<txn_id_t> granted;
          std::vector<txn_id_t> waited;
          for (const auto &lock_request : que->request_queue_) {
            if (lock_request->granted_) {
              granted.push_back(lock_request->txn_id_);
            } else {
              waited.push_back(lock_request->txn_id_);
            }
          }
          for (auto t2 : granted) {
            for (auto t1 : waited) {
              AddEdge(t1, t2);
            }
          }
        }
      };

      add_edges(table_lock_map_);
      add_edges(row_lock_map_);

      // This means when choosing which unexplored node to run DFS from, always choose the node with the lowest
      // transaction id.
      // This also means when exploring neighbors, explore them in sorted order from lowest to highest.
      waits_.reserve(waits_for_.size());
      for (auto &[t, vec] : waits_for_) {
        std::sort(vec.begin(), vec.end());
        waits_.push_back(t);
      }
      std::sort(waits_.begin(), waits_.end());

      auto remove_edges = [&](const auto &que, const auto &t1) {
        std::vector<txn_id_t> granted;
        for (const auto &lock_request : que->request_queue_) {
          if (!lock_request->granted_) {
            break;
          }
          granted.push_back(lock_request->txn_id_);
        }
        for (auto t2 : granted) {
          RemoveEdge(t1, t2);
        }
      };

      txn_id_t txn_id = INVALID_TXN_ID;

      while (HasCycle(&txn_id)) {
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);

        if (auto *p = std::get_if<table_oid_t>(&txn_variant_map_[txn_id])) {
          table_oid_t tid = *p;
          // LOG_DEBUG("\033[0;31m abort txn %d waits for table %d\033[0m;\n", txn_id, tid);
          auto &lock_request_que = table_lock_map_[tid];
          remove_edges(lock_request_que, txn_id);
          lock_request_que->cv_.notify_all();
        } else {
          RID rid = std::get<RID>(txn_variant_map_[txn_id]);
          // LOG_DEBUG("\033[0;31m abort txn %d waits for tuple [%d,%d]\033[0m;\n", txn_id, rid.GetPageId(),
          // rid.GetSlotNum());
          auto &lock_request_que = row_lock_map_[rid];
          remove_edges(lock_request_que, txn_id);
          lock_request_que->cv_.notify_all();
        }
      }

      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock();

      waits_.clear();
      waits_for_.clear();
    }
  }
}

}  // namespace bustub
