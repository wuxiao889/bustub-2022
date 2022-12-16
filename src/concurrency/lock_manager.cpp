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
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <bitset>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <mutex>  // NOLINT
#include <vector>

#include "catalog/column.h"
#include "catalog/table_generator.h"
#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/color.h"
#include "fmt/core.h"
#include "fmt/ranges.h"

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

auto LockManager::LockRequestQueue::CheckCompatibility(LockMode lock_mode, ListType::iterator ite) -> bool {
  // all earlier requests should have been granted already
  // check compatible with the locks that are currently held
  // dp
  int mask = 0;
  ite++;
  // fmt::print(fmt::fg(fmt::color::orange_red), "check\n");
  for (auto it = request_queue_.begin(); it != ite; it++) {
    const auto &request = *it;
    const auto mode = request->lock_mode_;
    auto *txn = TransactionManager::GetTransaction(request->txn_id_);
    if (txn->GetState() == TransactionState::ABORTED) {
      continue;
    }

    auto confilct = [](int mask, std::vector<LockMode> lock_modes) -> bool {
      return std::any_of(lock_modes.begin(), lock_modes.end(),
                         [mask](const auto lock_mode) { return (mask & 1 << static_cast<int>(lock_mode)) != 0; });
    };

    // fmt::print(fmt::fg(fmt::color::orange_red), "{} \n", mode);
    switch (mode) {
      case LockMode::INTENTION_SHARED:
        if (confilct(mask, {LockMode::EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        if (confilct(mask, {LockMode::SHARED, LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::SHARED:
        if (confilct(mask,
                     {LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE, LockMode::EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (confilct(mask, {LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED, LockMode::SHARED_INTENTION_EXCLUSIVE,
                            LockMode::EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::EXCLUSIVE:
        if (mask != 0) {
          return false;
        }
        break;
    }
    mask |= 1 << static_cast<int>(mode);
    // fmt::print("{}\n", std::bitset<6>(mask).to_string());
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
        LOG_WARN("LOCK_ON_SHRINKING\n");
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      // All locks are allowed in the GROWING state
      // Only IS, S locks are allowed in the SHRINKING state
      if (txn_state == TransactionState::SHRINKING) {
        if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
          txn->SetState(TransactionState::ABORTED);
          LOG_WARN("LOCK_ON_SHRINKING\n");
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
        LOG_WARN("LOCK_SHARED_ON_READ_UNCOMMITTED\n");
        throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        LOG_WARN("LOCK_ON_SHRINKINGE\n");
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
  }

  table_lock_map_latch_.lock();
  auto table_lock_map_it = table_lock_map_.find(oid);
  bool upgrade = false;
  LockMode old_lock_mode;
  std::shared_ptr<LockRequestQueue> lrque;

  auto lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
  // txn_time_map_[lock_request->txn_id_] = lock_request->timestamp_;
  txn_variant_map_latch_.lock();
  txn_variant_map_[txn_id] = oid;
  txn_variant_map_latch_.unlock();

  // LOG_DEBUG("\033[0;34m%d: txn %d locktable %d %s\033[0m\n", ::gettid(), txn_id, oid,
  //           LockModeToString(lock_mode).c_str());

  if (table_lock_map_it != table_lock_map_.end()) {
    lrque = table_lock_map_it->second;
    // 注意顺序，否则会出现txn乱序
    std::unique_lock lock(lrque->latch_);
    table_lock_map_latch_.unlock();

    auto &request_queue = lrque->request_queue_;
    decltype(request_queue.begin()) request_it;
    // check upgrade
    for (request_it = request_queue.begin(); request_it != request_queue.end(); ++request_it) {
      const auto &request = *request_it;

      if (request->txn_id_ == txn_id) {
        old_lock_mode = request->lock_mode_;
        if (old_lock_mode == lock_mode) {
          return true;
        }
        //  IS -> [S, X, IX, SIX]
        //  S -> [X, SIX]
        //  IX -> [X, SIX]
        //  SIX -> [X]
        switch (old_lock_mode) {
          case LockMode::INTENTION_SHARED:
            break;
          case LockMode::SHARED:
          case LockMode::INTENTION_EXCLUSIVE:
            if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
              txn->SetState(TransactionState::ABORTED);
              LOG_WARN("INCOMPATIBLE_UPGRADE\n");
              throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
            }
            break;
          case LockMode::SHARED_INTENTION_EXCLUSIVE:
            if (lock_mode != LockMode::EXCLUSIVE) {
              txn->SetState(TransactionState::ABORTED);
              LOG_WARN("INCOMPATIBLE_UPGRADE\n");
              throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
            }
            break;
          case LockMode::EXCLUSIVE:
            txn->SetState(TransactionState::ABORTED);
            LOG_WARN("INCOMPATIBLE_UPGRADE\n");
            throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
        }
        upgrade = true;
        break;
      }
    }

    if (upgrade) {
      if (lrque->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        LOG_WARN("UPGRADE_CONFLICT\n");
        throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      }

      // LOG_DEBUG("\033[0;34m%d: txn %d locktable %d  %s -> %s \033[0m\n", ::gettid(), txn_id, oid,
      //           LockModeToString(old_lock_mode).c_str(), LockModeToString(lock_mode).c_str());
      lrque->upgrading_ = txn_id;
      // A lock request being upgraded should be
      // prioritised over other waiting lock requests on the same resource.
      auto it = std::find_if(request_it, request_queue.end(), [](const auto &request) { return !request->granted_; });
      // 相当于释放了原来的锁
      request_queue.erase(request_it);
      request_it = request_queue.insert(it, lock_request);

      switch (old_lock_mode) {
        case LockMode::SHARED:
          txn->GetSharedTableLockSet()->erase(oid);
          break;
        case LockMode::EXCLUSIVE:
          break;
        case LockMode::INTENTION_SHARED:
          txn->GetIntentionSharedTableLockSet()->erase(oid);
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          txn->GetIntentionExclusiveTableLockSet()->erase(oid);
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
          break;
      }

    } else {
      request_it = request_queue.insert(request_queue.end(), lock_request);
    }
    // void wait( std::unique_lock<std::mutex>& lock, Predicate stop_waiting );
    lrque->cv_.wait(lock, [&]() {
      return txn->GetState() == TransactionState::ABORTED || lrque->CheckCompatibility(lock_mode, request_it);
    });

    if (txn->GetState() == TransactionState::ABORTED) {
      // LOG_DEBUG("\033[0;41m%d: txn %d locktable %d %s abort\033[0m\n", ::gettid(), txn_id, oid,
      //           LockModeToString(lock_mode).c_str());
      request_queue.erase(request_it);
      return false;
    }
    lock_request->granted_ = true;
    if (upgrade) {
      lrque->upgrading_ = INVALID_TXN_ID;
    }

    // fmt::print(fmt::fg(fmt::color::blue) | fmt::emphasis::bold, "{} {} granted {}\n", ::gettid(), txn_id,
    //            request_queue);

  } else {
    lrque = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = lrque;
    lrque->request_queue_.push_back(lock_request);
    lock_request->granted_ = true;
    table_lock_map_latch_.unlock();
  }

  // LOG_DEBUG("\033[0;44m%d: txn %d locktable %d %s success\033[0m\n", ::gettid(), txn_id, oid,
  //           LockModeToString(lock_mode).c_str());

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

  // unlocking a table should only be allowed if the transaction does not hold locks on any
  // row on that table.
  const auto &s_lock_set = txn->GetSharedRowLockSet();
  const auto &x_lock_set = txn->GetExclusiveRowLockSet();
  if ((s_lock_set->find(oid) != s_lock_set->end() && !s_lock_set->at(oid).empty()) ||
      (x_lock_set->find(oid) != x_lock_set->end() && !x_lock_set->at(oid).empty())) {
    txn->SetState(TransactionState::ABORTED);
    LOG_WARN("TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS\n");
    throw TransactionAbortException(txn_id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  table_lock_map_latch_.lock();
  // LOG_DEBUG("\033[0;32m%d: txn %d unlocktable %d\033[0m\n", ::gettid(), txn_id, oid);
  auto table_lock_map_it = table_lock_map_.find(oid);
  // Both should ensure that the transaction currently
  // holds a lock on the resource it is attempting to unlock.
  if (table_lock_map_it == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    LOG_WARN("ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD\n");
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  {
    auto &lrque = table_lock_map_it->second;
    std::unique_lock lock(lrque->latch_);
    table_lock_map_latch_.unlock();
    auto &request_queue = lrque->request_queue_;
    auto it = std::find_if(request_queue.begin(), request_queue.end(),
                           [txn_id](const auto &lock_request) { return lock_request->txn_id_ == txn_id; });

    if (it == request_queue.end() || !(*it)->granted_) {
      txn->SetState(TransactionState::ABORTED);
      LOG_WARN("ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD\n");
      throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }

    const auto &lock_request = *it;
    lock_mode = lock_request->lock_mode_;

    // Finally, unlocking a resource should also grant any new lock requests
    // for the resource (if possible).
    request_queue.erase(it);
    lrque->cv_.notify_all();
  }

  // LOG_DEBUG("\033[0;42m%d: txn %d unlocktable %d success\033[0m\n", ::gettid(), txn_id, oid);

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
  // LOG_DEBUG("\033[0;33m%d: txn %d table %d lockrow [%d,%d] %s\033[0m\n", ::gettid(), txn_id, oid, rid.GetPageId(),
  // rid.GetSlotNum(), LockModeToString(lock_mode).c_str());
  // Row locking should not support Intention locks.
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    LOG_WARN("ATTEMPTED_INTENTION_LOCK_ON_ROW\n");
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  switch (txn_level) {
    case IsolationLevel::REPEATABLE_READ:
      // All locks are allowed in the GROWING state
      // No locks are allowed in the SHRINKING state
      if (txn_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        LOG_WARN("LOCK_ON_SHRINKING\n");
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      // All locks are allowed in the GROWING state
      // Only IS, S locks are allowed in the SHRINKING state
      if (txn_state == TransactionState::SHRINKING) {
        if (lock_mode != LockMode::SHARED) {
          txn->SetState(TransactionState::ABORTED);
          LOG_WARN("LOCK_ON_SHRINKING\n");
          throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      // The transaction is required to take only IX, X locks.
      // X, IX locks are allowed in the GROWING state.
      if (lock_mode == LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        LOG_WARN("LOCK_SHARED_ON_READ_UNCOMMITTED\n");
        throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        LOG_WARN("LOCK_ON_SHRINKING\n");
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
  }

  if (lock_mode == LockMode::SHARED) {
    if (txn->IsRowSharedLocked(oid, rid)) {
      return true;
    }
    if (txn->IsRowExclusiveLocked(oid, rid)) {
      txn->SetState(TransactionState::ABORTED);
      LOG_WARN("INCOMPATIBLE_UPGRADE\n");
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
    }
    if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
        !txn->IsTableExclusiveLocked(oid) && !txn->IsTableSharedLocked(oid) && !txn->IsTableSharedLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      LOG_WARN("TABLE_LOCK_NOT_PRESENT\n");
      throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  } else {
    if (txn->IsRowExclusiveLocked(oid, rid)) {
      return true;
    }
    if (!txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
        !txn->IsTableExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      LOG_WARN("TABLE_LOCK_NOT_PRESENT\n");
      throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  //  if an exclusive lock is attempted on a row, the transaction must hold either
  //       X, IX, or SIX on the table.

  row_lock_map_latch_.lock();
  auto row_lock_map_it = row_lock_map_.find(rid);

  bool upgrade = false;
  std::shared_ptr<LockRequestQueue> lrque;

  auto lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
  assert(txn->GetState() != TransactionState::ABORTED);
  txn_variant_map_latch_.lock();
  txn_variant_map_[txn_id] = rid;
  txn_variant_map_latch_.unlock();

  if (row_lock_map_it != row_lock_map_.end()) {
    lrque = row_lock_map_it->second;
    std::unique_lock lock(lrque->latch_);
    row_lock_map_latch_.unlock();

    auto &request_queue = lrque->request_queue_;
    decltype(request_queue.begin()) request_it;
    for (request_it = request_queue.begin(); request_it != request_queue.end(); ++request_it) {
      const auto &request = *request_it;

      if (request->txn_id_ == txn_id) {
        assert(request->lock_mode_ == LockMode::SHARED && lock_mode == LockMode::EXCLUSIVE);
        upgrade = true;
        break;
      }
    }

    if (upgrade) {
      if (lrque->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        LOG_WARN("UPGRADE_CONFLICT\n");
        throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      }

      // LOG_DEBUG("\033[0;33m%d: txn %d table %d lockrow [%d,%d] %s -> %s\033[0m\n", ::gettid(), txn_id, oid,
      // rid.GetPageId(), rid.GetSlotNum(), LockModeToString(old_lock_mode).c_str(),
      // LockModeToString(lock_mode).c_str());
      lrque->upgrading_ = lock_request->txn_id_;

      auto it = std::find_if(request_it, request_queue.end(), [](const auto &request) { return !request->granted_; });

      request_queue.erase(request_it);
      request_it = request_queue.insert(it, lock_request);

      txn->GetSharedLockSet()->erase(rid);
      txn->GetSharedRowLockSet()->at(oid).erase(rid);
    } else {
      request_it = request_queue.insert(request_queue.end(), lock_request);
    }

    lrque->cv_.wait(lock, [&]() {
      return txn->GetState() == TransactionState::ABORTED || lrque->CheckCompatibility(lock_mode, request_it);
    });

    if (txn->GetState() == TransactionState::ABORTED) {
      // LOG_DEBUG("\033[0;41m%d: txn %d table %d lockrow [%d,%d] %s abort\033[0m\n", ::gettid(), txn_id, oid,
      // rid.GetPageId(), rid.GetSlotNum(), LockModeToString(lock_mode).c_str());
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

  // LOG_DEBUG("\033[0;43m%d: txn %d table %d lockrow [%d,%d] %s success\033[0m\n", ::gettid(), txn_id, oid,
  // rid.GetPageId(), rid.GetSlotNum(), LockModeToString(lock_mode).c_str());

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

  if (!txn->IsRowExclusiveLocked(oid, rid) && !txn->IsRowSharedLocked(oid, rid)) {
    txn->SetState(TransactionState::ABORTED);
    LOG_WARN("ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD\n");
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  row_lock_map_latch_.lock();
  auto row_lock_map_it = row_lock_map_.find(rid);
  // LOG_DEBUG("\033[0;35m%d: txn %d table %d unlockrow [%d,%d]\033[0m\n", ::gettid(), txn_id, oid, rid.GetPageId(),
  // rid.GetSlotNum());
  assert(row_lock_map_it != row_lock_map_.end());
  LockMode lock_mode;

  {
    auto &lrque = row_lock_map_it->second;
    std::unique_lock lock(lrque->latch_);
    row_lock_map_latch_.unlock();
    auto &request_queue = lrque->request_queue_;
    auto it = std::find_if(request_queue.begin(), request_queue.end(),
                           [txn_id](const auto &lock_request) { return lock_request->txn_id_ == txn_id; });
    assert(it != request_queue.end() && (*it)->granted_);
    // LOG_DEBUG("\033[0;45m%d: txn %d table %d unlockrow [%d,%d] success\033[0m\n", ::gettid(), txn_id, oid,
    // rid.GetPageId(), rid.GetSlotNum());
    lock_mode = it->get()->lock_mode_;
    request_queue.erase(it);
    lrque->cv_.notify_all();
  }

  if (txn->GetState() == TransactionState::GROWING) {
    // TRANSACTION STATE UPDATE
    switch (txn_level) {
      case IsolationLevel::REPEATABLE_READ:
        txn->SetState(TransactionState::SHRINKING);
      case IsolationLevel::READ_COMMITTED:
      case IsolationLevel::READ_UNCOMMITTED:
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
  if (t1 == t2) {
    return;
  }
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
  // if (vec.empty()) {
  //   waits_for_.erase(t2);
  // }
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
  if (waits_.size() != waits_for_.size()) {
    waits_.reserve(waits_for_.size());
    for (auto &[t, vec] : waits_for_) {
      std::sort(vec.begin(), vec.end());
      waits_.push_back(t);
    }
    std::sort(waits_.begin(), waits_.end());
  }

  if (waits_.empty()) {
    return false;
  }

  std::unordered_set<txn_id_t> visted;

  std::function<bool(txn_id_t)> has_cycle = [&](txn_id_t tid) -> bool {
    if (waits_for_.find(tid) == waits_for_.end()) {
      return false;
    }

    for (const auto &waits_for_txn_id : waits_for_[tid]) {
      if (visted.count(waits_for_txn_id) == 1) {
        *txn_id = waits_for_txn_id;
        for (const auto &visted_txn_id : visted) {
          *txn_id = std::max(*txn_id, visted_txn_id);
        }
        return true;
      }

      visted.insert(waits_for_txn_id);
      if (has_cycle(waits_for_txn_id)) {
        return true;
      }
      visted.erase(waits_for_txn_id);
    }

    return false;
  };

  return std::any_of(waits_.begin(), waits_.end(), [&](const auto &tid) { return has_cycle(tid); });
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::lock_guard lock(waits_for_latch_);
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      // LOG_DEBUG("\033[0;31m %d RunCycleDetection\033[0m\n", ::gettid());

      auto add_edges = [&](const auto &map) {
        for (const auto &[_, que] : map) {
          std::vector<txn_id_t> granted;
          std::vector<txn_id_t> waited;

          {
            std::lock_guard lock(que->latch_);
            for (const auto &lock_request : que->request_queue_) {
              if (lock_request->granted_) {
                granted.push_back(lock_request->txn_id_);
              } else {
                waited.push_back(lock_request->txn_id_);
              }
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

        txn_variant_map_latch_.lock();
        if (auto *p = std::get_if<table_oid_t>(&txn_variant_map_[txn_id])) {
          txn_variant_map_latch_.unlock();
          auto tid = *p;
          // LOG_WARN("\033[0;31m abort txn %d waits for table %d\033[0m;\n", txn_id, tid);
          auto &lock_request_que = table_lock_map_[tid];
          remove_edges(lock_request_que, txn_id);
          if (waits_for_.find(txn_id) != waits_for_.end()) {
            waits_for_.erase(txn_id);
          }
          lock_request_que->cv_.notify_all();
        } else {
          auto rid = std::get<RID>(txn_variant_map_[txn_id]);
          txn_variant_map_latch_.unlock();
          // LOG_WARN("\033[0;31m abort txn %d waits for tuple [%d,%d]\033[0m;\n", txn_id, rid.GetPageId(),
          //          rid.GetSlotNum());
          auto &lock_request_que = row_lock_map_[rid];
          remove_edges(lock_request_que, txn_id);
          if (waits_for_.find(txn_id) != waits_for_.end()) {
            waits_for_.erase(txn_id);
          }
          lock_request_que->cv_.notify_all();
        }
      }

      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock();

      waits_.clear();
      waits_for_.clear();
      // LOG_DEBUG("\033[0;31m %d CycleDetection finish\033[0m\n", ::gettid());
    }
  }
}

auto LockManager::LockRequest::ToString() -> std::string {
  return fmt::format("[{} {} {}] ", txn_id_, lock_mode_, granted_);
}

}  // namespace bustub

template <>
struct fmt::formatter<bustub::LockManager::LockRequest> : formatter<string_view> {
  template <typename FormatContext>
  auto format(bustub::LockManager::LockRequest lock_request, FormatContext &ctx) const {
    return formatter<string_view>::format(lock_request.ToString(), ctx);
  }
};

template <typename T>
struct fmt::formatter<std::shared_ptr<T>,
                      std::enable_if_t<std::is_base_of<bustub::LockManager::LockRequest, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::shared_ptr<T> &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x->ToString(), ctx);
  }
};

auto LockModeToString(bustub::LockManager::LockMode lock_mode) -> std::string {
  switch (lock_mode) {
    case bustub::LockManager::LockMode::SHARED:
      return "SHARED";
    case bustub::LockManager::LockMode::EXCLUSIVE:
      return "EXCLUSIVE";
    case bustub::LockManager::LockMode::INTENTION_SHARED:
      return "INTENTION_SHARED";
    case bustub::LockManager::LockMode::INTENTION_EXCLUSIVE:
      return "INTENTION_EXCLUSIVE";
    case bustub::LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return "SHARED_INTENTION_EXCLUSIVE";
  }
  return "";
}

template <>
struct fmt::formatter<bustub::LockManager::LockMode> : formatter<string_view> {
  template <typename FormatContext>
  auto format(bustub::LockManager::LockMode lock_mode, FormatContext &ctx) const {
    return formatter<string_view>::format(::LockModeToString(lock_mode), ctx);
  }
};