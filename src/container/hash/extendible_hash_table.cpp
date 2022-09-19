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
#include "common/logger.h"
#include <thread>
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(new Bucket(bucket_size_,global_depth_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::CalIndex(size_t &idx) -> size_t {
  return idx ^ 1 << (global_depth_ - 1);
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
  return dir_[idx]->Find(key,value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::shared_lock lock(latch_);
  size_t idx = IndexOf(key);
  return dir_[idx]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  while(true){
    size_t idx;
    bool ok;
    {
      std::shared_lock lock(latch_);
//      LOG_INFO("try to insert key %d", const_cast<int&>(key));
      idx = IndexOf(key);
      ok = dir_[idx]->Insert(key,value);
      if(ok){
//        LOG_INFO("success to insert key %d", const_cast<int&>(key));
        break;}
    }
    std::unique_lock lock(latch_);
    ok = dir_[idx]->Insert(key,value);
    if(ok){break;}
    if(dir_[idx]->GetDepth() == global_depth_){
      Expansion();
      RedistributeBucket(idx);
    }else{
      RedistributeBucket(idx);
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Expansion() -> void {
//  std::scoped_lock<std::mutex> lock(latch_);
//  LOG_INFO("Expansion to depth %d ", global_depth_+1);
  num_buckets_ = 1 << ++global_depth_;
  dir_.resize(num_buckets_);
  for(size_t i = 1 << (global_depth_ - 1) ; (int)i < num_buckets_ ; i++){
    dir_[i] = dir_[CalIndex(i)];
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(size_t idx) -> void {
  std::shared_ptr<Bucket> &old_bucket = dir_[idx];
//  std::unique_lock lock1(old_bucket->mutex_);
  std::shared_ptr<Bucket> &new_bucket = dir_[CalIndex(idx)];
  old_bucket->IncrementDepth();
  new_bucket.reset(new Bucket(bucket_size_,old_bucket->GetDepth()));
//  std::unique_lock lock2(new_bucket->mutex_);
//  LOG_INFO("RedistributeBucket %zu ", idx);
  auto list = old_bucket->GetItems();
  old_bucket->GetItems().clear();
  for(auto& [k,v] : list){
    dir_[IndexOf(k)]->GetItems().emplace_back(k,v);
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
  for(auto &[k,v] : list_){
    if(k == key){
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  std::unique_lock lock(mutex_);
  for(auto it = list_.begin(); it != list_.end() ; ++it){
    if(it->first == key){
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  std::unique_lock lock(mutex_);
  list_.remove_if([&](std::pair<K,V>& p){return key == p.first;});
  if(IsFull()){
    return false;};
  list_.emplace_back(key,value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
//template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
