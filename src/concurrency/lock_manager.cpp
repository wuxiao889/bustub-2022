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
#include <cassert>
#include <memory>
#include <mutex>  // NOLINT
#include "catalog/column.h"
#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

/*

    Compatibility Matrix
        IS   IX   S   SIX   X
  IS    √    √    √    √    ×
  IX    √    √    ×    ×    ×
  S     √    ×    √    ×    ×
  SIX   √    ×    ×    ×    ×
  X     ×    ×    ×    ×    ×
*/

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

auto LockManager::LockRequestQueue::CheckRunnable(LockMode lock_mode, ListType::iterator ite) -> bool {
  // int mask = 0;
  // all earlier requests should have been granted already
  // check compatible with the locks that are currently held
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

  // switch (lock_mode) {
  //   case LockMode::INTENTION_SHARED:
  //     if ((mask & 1 << static_cast<int>(LockMode::INTENTION_EXCLUSIVE)) == 1) {
  //       return false;
  //     }
  //     break;
  //   case LockMode::INTENTION_EXCLUSIVE:
  //     if (((mask & 1 << static_cast<int>(LockMode::SHARED)) == 1) ||
  //         (mask & 1 << static_cast<int>(LockMode::SHARED_INTENTION_EXCLUSIVE)) == 1 ||
  //         ((mask & 1 << static_cast<int>(LockMode::EXCLUSIVE)) == 1)) {
  //       return false;
  //     }
  //     break;
  //   case LockMode::SHARED:
  //     if (((mask & 1 << static_cast<int>(LockMode::INTENTION_EXCLUSIVE)) == 1) ||
  //         (mask & 1 << static_cast<int>(LockMode::SHARED_INTENTION_EXCLUSIVE)) == 1 ||
  //         ((mask & 1 << static_cast<int>(LockMode::EXCLUSIVE)) == 1)) {
  //       return false;
  //     }
  //     break;
  //   case LockMode::SHARED_INTENTION_EXCLUSIVE:
  //     if ((mask & ~(1 << static_cast<int>(LockMode::SHARED_INTENTION_EXCLUSIVE))) == 1) {
  //       return false;
  //     }
  //     break;
  //   case LockMode::EXCLUSIVE:
  //     if (mask != 0) {
  //       return false;
  //     }
  //     break;
  // }
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
  LOG_DEBUG("\033[0;34m%d: txn %d locktable %d %s\033[0m\n", ::gettid(), txn_id, oid,
            LockModeToString(lock_mode).c_str());
  auto lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
  auto table_lock_map_it = table_lock_map_.find(oid);

  bool upgrade = false;
  LockMode old_lock_mode;

  if (table_lock_map_it != table_lock_map_.end()) {
    lrque = table_lock_map_it->second;
    std::unique_lock lock(lrque->latch_);
    table_lock_map_latch_.unlock();

    decltype(lrque->request_queue_.begin()) request_queue_it;
    for (request_queue_it = lrque->request_queue_.begin(); request_queue_it != lrque->request_queue_.end();
         ++request_queue_it) {
      const auto &request = *request_queue_it;

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

      auto it = request_queue_it;
      for (; it != lrque->request_queue_.end(); ++it) {
        const auto &request = *request_queue_it;
        if (!request->granted_) {
          break;
        }
      }
      // A lock request being upgraded should be
      // prioritised over other waiting lock requests on the same resource.
      lrque->request_queue_.erase(request_queue_it);
      request_queue_it = lrque->request_queue_.insert(it, lock_request);
    } else {
      request_queue_it = lrque->request_queue_.insert(lrque->request_queue_.end(), lock_request);
    }
    // void wait( std::unique_lock<std::mutex>& lock, Predicate stop_waiting );
    lrque->cv_.wait(lock, [&]() {
      auto run = lrque->CheckRunnable(lock_mode, request_queue_it);
      return run;
    });

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

  auto lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
  std::shared_ptr<LockRequestQueue> lrque;

  row_lock_map_latch_.lock();
  auto row_lock_map_it = row_lock_map_.find(rid);

  bool upgrade = false;
  LockMode old_lock_mode;

  if (row_lock_map_it != row_lock_map_.end()) {
    lrque = row_lock_map_it->second;
    std::unique_lock lock(lrque->latch_);
    row_lock_map_latch_.unlock();

    auto &request_queue = lrque->request_queue_;
    decltype(request_queue.begin()) que_it;
    for (que_it = request_queue.begin(); que_it != request_queue.end(); ++que_it) {
      const auto &request = *que_it;
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

      auto it = que_it;
      for (; it != request_queue.end(); ++it) {
        const auto &request = *it;
        if (!request->granted_) {
          break;
        }
      }

      request_queue.erase(que_it);
      que_it = request_queue.insert(it, lock_request);
    } else {
      que_it = request_queue.insert(request_queue.end(), lock_request);
    }
    lrque->cv_.wait(lock, [&]() { return lrque->CheckRunnable(lock_mode, que_it); });
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

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
