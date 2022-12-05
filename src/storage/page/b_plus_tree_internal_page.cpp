//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <iostream>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size >= static_cast<int>(INTERNAL_PAGE_SIZE) ? INTERNAL_PAGE_SIZE : max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // TODO(wxx): assert(index > 0 && index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index < GetSize());
  assert(array_[index].second != INVALID_PAGE_ID);
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  assert(value != INVALID_PAGE_ID);
  assert(value >= 0);
  array_[index].second = value;
}

/**
 * @brief find the first element >= key
 *
 * @param key
 * @param cmp
 * @return int
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LowerBound(const KeyType &key, const KeyComparator &cmp) -> int {
  // int res = std::lower_bound(array_ + 1, array_ + GetSize(), key,
  //                            [&](const MappingType &x, const KeyType &key) { return cmp(x.first, key) == -1; }) -
  //           array_;
  int left = 1;
  int right = GetSize();
  while (left < right) {
    int mid = (left + right) / 2;
    if (cmp(array_[mid].first, key) >= 0) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ShiftLeft(int i) {
  std::copy(array_ + i + 1, array_ + GetSize(), array_ + i);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ShiftRight() {
  std::copy_backward(array_, array_ + GetSize(), array_ + 1 + GetSize());
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Copy(const BPlusTreeInternalPage *src, int result, int first, int last) {
  std::copy(src->array_ + first, src->array_ + last, array_ + result);
  IncreaseSize(last - first);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Copy(const std::vector<MappingType> &src, int result, int first, int last) {
  std::copy(src.data() + first, src.data() + last, array_ + result);
  IncreaseSize(last - first);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpperBound(const KeyType &key, const KeyComparator &cmp) -> int {
  int left = 1;
  int right = GetSize();
  while (left < right) {
    int mid = (left + right) / 2;
    if (cmp(array_[mid].first, key) <= 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Search(const KeyType &key, const KeyComparator &cmp) -> int {
  return UpperBound(key, cmp) - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindIndex(const ValueType &value) -> int {
  int i;
  for (i = 0; i < GetSize(); ++i) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return i;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(int index, const KeyType &key, const ValueType &value) -> void {
  assert(index <= GetSize());
  array_[GetSize()] = MappingType{key, value};
  for (int j = GetSize(); j > index; --j) {
    std::swap(array_[j], array_[j - 1]);
  }
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(BPlusTreeInternalPage *new_node, const KeyType &key, const ValueType &value,
                                           const KeyComparator &comparator) {
  // 找一个更大的空间存储，二分 然后插入
  std::vector<MappingType> vec(array_, array_ + GetMaxSize());
  vec.reserve(GetMaxSize() + 1);
  SetSize(0);
  int index = std::lower_bound(vec.begin() + 1, vec.end(), key,
                               [&](const std::pair<KeyType, page_id_t> &l, const KeyType &r) {
                                 return comparator(l.first, r) < 0;
                               }) -
              vec.begin();
  vec.insert(vec.begin() + index, {key, value});

  int x = GetMinSize();
  Copy(vec, 0, 0, x);
  new_node->Copy(vec, 0, x, vec.size());
  assert(new_node->GetSize() == GetMaxSize() - x + 1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
template class BPlusTreeInternalPage<int, int, std::less<>>;

}  // namespace bustub
