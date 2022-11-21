//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/extendible_hash_table.h"
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <shared_mutex>
#include <utility>
#include <vector>
#include "common/logger.h"
#include "storage/page/page.h"
namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size_, global_depth_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::CalIndex(size_t &idx, int depth) -> size_t {
  return idx ^ 1 << (depth - 1);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::shared_lock lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::shared_lock lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::shared_lock lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::shared_lock lock(latch_);
  size_t idx = IndexOf(key);
  return dir_[idx]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::unique_lock lock(latch_);
  size_t idx = IndexOf(key);
  return dir_[idx]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::unique_lock lock(latch_);
  while (true) {
    size_t idx;
    bool ok;
    {
      idx = IndexOf(key);
      ok = dir_[idx]->Insert(key, value);
      if (ok) {
        return;
      }
    }
    if (dir_[idx]->GetDepth() == global_depth_) {
      Expansion();
      RedistributeBucket(idx);
    } else {
      RedistributeBucket(idx);
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Expansion() -> void {
  // LOG_DEBUG("Expansion to depth %d ", global_depth_+1);
  size_t old_size = 1 << global_depth_++;
  size_t new_size = 1 << global_depth_;
  dir_.reserve(new_size);
  for (size_t i = old_size; i < new_size; i++) {
    dir_.push_back(dir_[CalIndex(i, global_depth_)]);
  }
}

// localDepth < globalDepth 多个dir_指向一个同一个新桶
// 只会分裂出一个新桶
// 注意把多个dir_分成两种指向
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(size_t idx) -> void {
  int old_depth = dir_[idx]->GetDepth();

  size_t old_idx = idx & ((1 << old_depth) - 1);  // 取低old_depth位就是old_bucket idx
  auto &old_bucket = dir_[old_idx];
  auto &new_bucket = dir_[CalIndex(old_idx, old_depth + 1)];
  new_bucket.reset(new Bucket(bucket_size_, old_depth));

  old_bucket->IncrementDepth();
  new_bucket->IncrementDepth();

  size_t diff = global_depth_ - old_depth;
  int n = (1 << diff) - 1;

  for (int i = 2; i <= n; ++i) {  // i = 0 old_buckt i = 1 new_bucket
    size_t idx = (i << old_depth) + old_idx;
    dir_[idx] = static_cast<bool>(i & 1) ? new_bucket : old_bucket;
  }

  num_buckets_ += 1;
  auto list = std::move(old_bucket->GetItems());

  for (auto &[k, v] : list) {
    dir_[IndexOf(k)]->Insert(k, v);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  std::shared_lock lock(mutex_);
  for (auto &[k, v] : list_) {
    if (k == key) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  std::unique_lock lock(mutex_);
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  std::unique_lock lock(mutex_);
  for (auto &[k, v] : list_) {
    if (k == key) {
      v = value;
      return true;
    }
  }
  if (IsFullWithGuard()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
// template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
