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

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.resize((1 << global_depth_));
  dir_[0] = std::make_shared<Bucket>(bucket_size);  // 初始时只有一个桶(local depth == 0)，并设置桶的大小（固定大小）
}

// 索引为取其哈希值的前n位
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth(bool use_mutex) const -> int {
  if (use_mutex) {
    return GetGlobalDepthInternal();
  }
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index, bool use_mutex) const -> int {
  if (use_mutex) {
    return GetLocalDepthInternal(dir_index);
  }
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets(bool use_mutex) const -> int {
  if (use_mutex) {
    return GetNumBucketsInternal();
  }
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  return (*dir_[idx]).Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  return (*dir_[idx]).Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  // 如果桶已满
  while ((*dir_[idx]).IsFull()) {
    if (global_depth_ == GetLocalDepth(idx, true)) {  // 如果local_depth == global_depth
      ++global_depth_;
      dir_.resize(1 << global_depth_, nullptr);
      for (int i = (1 << (global_depth_ - 1)); i < (1 << global_depth_); i++) {
        if (!dir_[i]) {
          dir_[i] = dir_[OtherBucket(i)];
        }
      }
    }
    RePoint(idx);
    idx = IndexOf(key);  // 获得新的下标
  }
  (*dir_[idx]).Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto it = Find(key);
  if (it != list_.end()) {
    value = (*it).second;
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = Find(key);
  if (it != list_.end()) {
    list_.erase(it);
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  auto it = Find(key);
  if (it != list_.end()) {
    if (value != (*it).second) {
      (*it).second = value;
    }
    return true;
  }
  list_.push_front(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
