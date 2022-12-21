//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.h
//
// Identification: src/include/container/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * extendible_hash_table.h
 *
 * Implementation of in-memory hash table using extendible hashing
 */

#pragma once
#include <iostream>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <utility>
#include <vector>

#include "container/hash/hash_table.h"

namespace bustub {

/**
 * ExtendibleHashTable implements a hash table using the extendible hashing algorithm.
 * @tparam K key type
 * @tparam V value type
 */
template <typename K, typename V>
class ExtendibleHashTable : public HashTable<K, V> {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Create a new ExtendibleHashTable.
   * @param bucket_size: fixed size for each bucket
   */
  explicit ExtendibleHashTable(size_t bucket_size);

  /**
   * @brief Get the global depth of the directory.
   * @return The global depth of the directory.
   */
  auto GetGlobalDepth(bool use_mutex = false) const -> int;

  /**
   * @brief Get the local depth of the bucket that the given directory index points to.
   * @param dir_index The index in the directory.
   * @return The local depth of the bucket.
   */
  auto GetLocalDepth(int dir_index, bool use_mutex = false) const -> int;

  /**
   * @brief Get the number of buckets in the directory.
   * @return The number of buckets in the directory.
   */
  auto GetNumBuckets(bool use_mutex = false) const -> int;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Find the value associated with the given key.
   *
   * Use IndexOf(key) to find the directory index the key hashes to.
   *
   * @param key The key to be searched.
   * @param[out] value The value associated with the key.
   * @return True if the key is found, false otherwise.
   */
  auto Find(const K &key, V &value) -> bool override;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Insert the given key-value pair into the hash table.
   * If a key already exists, the value should be updated.
   * If the bucket is full and can't be inserted, do the following steps before retrying:
   *    1. If the local depth of the bucket is equal to the global depth,
   *        increment the global depth and double the size of the directory.
   *    2. Increment the local depth of the bucket.
   *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
   *
   * @param key The key to be inserted.
   * @param value The value to be inserted.
   */
  void Insert(const K &key, const V &value) override;

  void Display(int idx) { (*dir_[idx]).Display(); }

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Given the key, remove the corresponding key-value pair in the hash table.
   * Shrink & Combination is not required for this project
   * @param key The key to be deleted.
   * @return True if the key exists, false otherwise.
   */
  auto Remove(const K &key) -> bool override;

  /**
   * Bucket class for each hash table bucket that the directory points to.
   */
  class Bucket {
   public:
    explicit Bucket(size_t size, int depth = 0);

    /** @brief Check if a bucket is full. */
    inline auto IsFull() const -> bool { return list_.size() == size_; }

    /** @brief Get the local depth of the bucket. */
    inline auto GetDepth() const -> int { return depth_; }

    /** @brief Increment the local depth of a bucket. */
    inline void IncrementDepth() { depth_++; }

    inline auto GetItems() -> std::list<std::pair<K, V>> & { return list_; }

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Find the value associated with the given key in the bucket.
     * @param key The key to be searched.
     * @param[out] value The value associated with the key.
     * @return True if the key is found, false otherwise.
     */
    auto Find(const K &key, V &value) -> bool;

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Given the key, remove the corresponding key-value pair in the bucket.
     * @param key The key to be deleted.
     * @return True if the key exists, false otherwise.
     */
    auto Remove(const K &key) -> bool;

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Insert the given key-value pair into the bucket.
     *      1. If a key already exists, the value should be updated.
     *      2. If the bucket is full, do nothing and return false.
     * @param key The key to be inserted.
     * @param value The value to be inserted.
     * @return True if the key-value pair is inserted, false otherwise.
     */
    auto Insert(const K &key, const V &value) -> bool;

    void Display() {
      for (auto it = list_.begin(); it != list_.end(); it++) {
        std::cout << (*it).first << "   ";
      }
      std::cout << std::endl;
    }

   private:
    auto Find(const K &key) -> typename std::list<std::pair<K, V>>::iterator {
      for (auto it = list_.begin(); it != list_.end(); it++) {
        if ((*it).first == key) {
          return it;
        }
      }
      return list_.end();
    }

   private:
    // TODO(student): You may add additional private members and helper functions

    size_t size_;
    int depth_;  // local depth
    std::list<std::pair<K, V>> list_;
  };

 private:
  void RePoint(size_t idx) {
    std::vector<size_t> grape = GetIndex(idx, GetLocalDepth(idx, true));  // 1. 获得兄弟下标
    (*dir_[idx]).IncrementDepth();                                        // ++local_depth
    ++num_buckets_;                                                       // 桶数量+1
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, GetLocalDepth(idx, true));
    size_t local_idx = GetLocalDepth(idx, true);
    auto &bucket_list = (*dir_[idx]).GetItems();
    for (auto i : grape) {
      if ((i & (1 << (local_idx - 1))) != 0U) {
        dir_[i] = new_bucket;
      }
    }
    std::vector<std::pair<K, V>> inset;  // 是不是可以存迭代器？
    for (auto it = bucket_list.begin(); it != bucket_list.end(); it++) {
      inset.push_back(std::move(std::make_pair((*it).first, (*it).second)));
    }
    bucket_list.clear();
    for (auto &it : inset) {
      (*dir_[IndexOf(it.first)]).Insert(it.first, it.second);
    }
  }

  auto OtherBucket(size_t x) -> size_t {
    return (x & (1 << (global_depth_ - 1))) == 0 ? (x | (1 << (global_depth_ - 1)))
                                                 : (x & (~(1 << (global_depth_ - 1))));
  }

  auto GetIndex(size_t idx, size_t local_depth) -> std::vector<size_t> {
    std::vector<size_t> temp;
    size_t x = (idx & ((1 << local_depth) - 1));
    GetIndexInternal(local_depth, global_depth_, 0, x, temp);
    return temp;
  }

  void GetIndexInternal(size_t local_depth, size_t global_depth, size_t cnt, size_t x, std::vector<size_t> &temp) {
    if (local_depth + cnt > global_depth - 1) {
      temp.push_back(x);
      return;
    }
    GetIndexInternal(local_depth, global_depth, cnt + 1, x & (~(1 << (local_depth + cnt))), temp);
    GetIndexInternal(local_depth, global_depth, cnt + 1, (x | (1 << (local_depth + cnt))), temp);
  }

 private:
  // TODO(student): You may add additional private members and helper functions and remove the ones
  // you don't need.

  int global_depth_;    // The global depth of the directory
  size_t bucket_size_;  // The size of a bucket
  int num_buckets_;     // The number of buckets in the hash table
  mutable std::mutex latch_;
  std::vector<std::shared_ptr<Bucket>> dir_;  // The directory of the hash table

  // The following functions are completely optional, you can delete them if you have your own ideas.

  /**
   * @brief Redistribute the kv pairs in a full bucket.
   * @param bucket The bucket to be redistributed.
   */
  auto RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void;

  /*****************************************************************
   * Must acquire latch_ first before calling the below functions. *
   *****************************************************************/

  /**
   * @brief For the given key, return the entry index in the directory where the key hashes to.
   * @param key The key to be hashed.
   * @return The entry index in the directory.
   */
  auto IndexOf(const K &key) -> size_t;

  auto GetGlobalDepthInternal() const -> int;
  auto GetLocalDepthInternal(int dir_index) const -> int;
  auto GetNumBucketsInternal() const -> int;
};

}  // namespace bustub
