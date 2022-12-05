//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTree;

template <typename KeyType, typename ValueType, typename KeyComparator>
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using Tree = BPlusTree<KeyType,ValueType,KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(Page *page, int position, BufferPoolManager *buffer_pool_manager);
  IndexIterator(const IndexIterator &x);

  ~IndexIterator();

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { return page_ == itr.page_ && position_ == itr.position_; }

  auto operator!=(const IndexIterator &itr) const -> bool { return !operator==(itr); }

 private:
  // add your own private member variables here
  Tree* tree_{};
  Page *page_{};
  int position_{};
  BufferPoolManager *buffer_pool_manager_{};
};

}  // namespace bustub
