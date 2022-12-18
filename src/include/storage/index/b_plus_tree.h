//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <mutex>  // NOLINT
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
// INDEX_TEMPLATE_ARGUMENTS
template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using Iterator = IndexIterator<KeyType, ValueType, KeyComparator>;

  friend class IndexIterator<KeyType, ValueType, KeyComparator>;
  enum class OpeType { FIND, INSERT, REMOVE };

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  auto GetLevel() const -> int;

 private:
  auto IsSafe(BPlusTreePage *node, OpeType operation) -> bool;

  auto FindLeafPage(const KeyType &key, OpeType operation, bool optimistic, bool &root_locked, Transaction *transaction)
      -> Page *;

  auto InsertPessimistic(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool;
  void InsertInParent(BPlusTreePage *left_node, BPlusTreePage *right_node, const KeyType &key, page_id_t value,
                      Transaction *transaction = nullptr);

  auto RemovePessimistic(const KeyType &key, Transaction *transaction);
  auto RemoveInParent(Page *page, bool &root_locked, Transaction *transaction) -> bool;
  void BorrowFromLeft(BPlusTreePage *node, BPlusTreePage *left_node, int left_position, Transaction *transaction);
  void BorrowFromRight(BPlusTreePage *node, BPlusTreePage *right_node, int left_position, Transaction *transaction);
  // always merge right to left
  auto Merge(BPlusTreePage *left_node, BPlusTreePage *right_node, int posOfLeftPage, bool &root_locked,
             Transaction *transaction) -> bool;

  void UnlockRoot(bool &root_locked);

  auto FetchChildPage(InternalPage *node, int index) const -> Page *;

  void UpdateChild(InternalPage *node, int first, int last);

  void ClearAndUnlock(Transaction *transaction, bool &root_locked);
  void ClearPageSet(Transaction *transaction);

  auto CastBPlusPage(Page *page) const -> BPlusTreePage * { return reinterpret_cast<BPlusTreePage *>(page->GetData()); }
  auto CastLeafPage(Page *page) const -> LeafPage * { return reinterpret_cast<LeafPage *>(page->GetData()); }
  auto CastInternalPage(Page *page) const -> InternalPage * {
    return reinterpret_cast<InternalPage *>(page->GetData());
  }
  auto CastLeafPage(BPlusTreePage *node) const -> LeafPage * { return static_cast<LeafPage *>(node); }
  auto CastInternalPage(BPlusTreePage *node) const -> InternalPage * { return static_cast<InternalPage *>(node); }

  auto NewLeafPage(page_id_t *page_id, page_id_t parent_id) -> Page *;
  auto NewLeafRootPage(page_id_t *page_id) -> Page *;
  auto NewInternalPage(page_id_t *page_id, page_id_t parent_id) -> Page *;
  auto NewInternalRootPage(page_id_t *page_id) -> Page *;

  void UpdateRootPageId(int insert_record = 0);
  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;

  page_id_t left_most_;
  page_id_t right_most_;

  class RootMutex {
   public:
    inline void Latch();
    inline void UnLatch();

   private:
    std::mutex latch_;
  };

  RootMutex root_latch_;  // guard root_page / root_page_id
};

}  // namespace bustub
