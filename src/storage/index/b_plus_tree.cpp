#include "storage/index/b_plus_tree.h"
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/hash_table_page_defs.h"
#include "storage/page/header_page.h"

namespace bustub {

int step_cont = 0;

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // LOG_INFO("\033[1;34mGetValue %ld\033[0m", key.ToString());
  Page *page;
  {
    std::lock_guard lock(latch_);
    if (IsEmpty()) {
      return false;
    }
    page = buffer_pool_manager_->FetchPage(root_page_id_);
  }
  BPlusTreePage *node = CastBPlusPage(page);

  bool find = false;
  page->RLatch();

  if (!node->IsRootPage()) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return GetValue(key, result);
  }

  while (!node->IsLeafPage()) {
    InternalPage *internal_node = CastInternalPage(node);
    int index = internal_node->UpperBound(key, comparator_);
    // LOG_INFO("lookup to page %d at index %d", page->GetPageId(), idx - 1);
    Page *next_page = FetchChildPage(internal_node, index - 1);
    next_page->RLatch();

    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

    BPlusTreePage *next_node = CastInternalPage(next_page);
    node = next_node;
    page = next_page;
  }

  LeafPage *leaf_node = CastLeafPage(page);
  int index = leaf_node->LowerBound(key, comparator_);

  // LOG_INFO("lookup int leaf page %d at index %d", page->GetPageId(), index);
  assert(index <= leaf_node->GetSize());

  if (index < leaf_node->GetSize() && comparator_(leaf_node->KeyAt(index), key) == 0) {
    // LOG_INFO("found");
    result->emplace_back(leaf_node->ValueAt(index));
    find = true;
  } else {
    // LOG_INFO("\033[1;31m key %ld not found leaf page %d at index %d \033[0m", key.ToString(), node->GetPageId(),
    // index);
  }

  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

  // buffer_pool_manager_->CheckPinCount();
  return find;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Transaction *transaction) -> Page * {
  Page *current_page = buffer_pool_manager_->FetchPage(root_page_id_);
  current_page->RLatch();

  BPlusTreePage *node = CastBPlusPage(current_page);

  while (!node->IsLeafPage()) {
    InternalPage *i_node = CastInternalPage(node);
    int idx = i_node->UpperBound(key, comparator_);
    // LOG_INFO("lookup to page %d at index %d", page->GetPageId(), idx - 1);
    Page *next_page = FetchChildPage(i_node, idx - 1);
    next_page->RLatch();

    current_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(current_page->GetPageId(), false);

    BPlusTreePage *next_node = CastInternalPage(next_page);
    node = next_node;
    current_page = next_page;
  }

  return current_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageOptimistic(const KeyType &key, bool optimistic, bool getValue, bool &root_locked,
                                            Transaction *transaction) -> Page * {
  Page *page;
  BPlusTreePage *node;
  while (true) {
    {
      std::lock_guard lock(latch_);
      page = buffer_pool_manager_->FetchPage(root_page_id_);
      node = CastBPlusPage(page);
    }

    if (optimistic) {
      page->RLatch();
      if (!node->IsRootPage()) {
        page->RUnlatch();
      }
    } else {
      page->WLatch();
    }
  }

  return page;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // LOG_INFO("\033[1;34m %d insert %ld\033[0m", ::gettid(), key.ToString());
  Page *page{};
  BPlusTreePage *node;

  // root_page 安全前不能释放锁！！！！获取rootpage时一定要是安全的
  // 乐观insert, root Rlatch ,
  // 悲观insert, root Wlatch ,
  {
    latch_.lock();
    if (IsEmpty()) {
      page = NewLeafRootPage(&root_page_id_);
    } else {
      page = buffer_pool_manager_->FetchPage(root_page_id_);
    }

    node = CastBPlusPage(page);
    if (node->IsLeafPage()) {
      // 当root 是leaf时，判断root安全插入才能释放锁
      // 否则提前释放了锁，root在这一个insert中分裂， 而别的线程依旧在这个旧的root中插入
      page->WLatch();
      if (!IsSafeInsert(node)) {
        page->WUnlatch();
        latch_.unlock();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        return InsertPessimistic(key, value, transaction);
      }
    } else {
      page->RLatch();
    }
    latch_.unlock();
  }

  while (!node->IsLeafPage()) {
    InternalPage *internal_node = CastInternalPage(node);
    // TODO(wxx) UpperBound 改 Lookup ， index-1放在函数里面
    int index = internal_node->UpperBound(key, comparator_);

    Page *next_page = FetchChildPage(internal_node, index - 1);
    BPlusTreePage *next_node = CastInternalPage(next_page);

    if (next_node->IsLeafPage()) {
      next_page->WLatch();
    } else {
      next_page->RLatch();
    }

    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

    node = next_node;
    page = next_page;
  }

  LeafPage *leaf_node = CastLeafPage(page);

  if (IsSafeInsert(leaf_node)) {
    bool inserted = InsertInLeaf(leaf_node, key, value);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), inserted);
    // buffer_pool_manager_->CheckPinCount();
    return inserted;
  }

  // restart pessimistic
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

  // buffer_pool_manager_->CheckPinCount();
  return InsertPessimistic(key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertPessimistic(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // LOG_INFO("%d InsertPessimistic", ::gettid());
  Page *page;
  BPlusTreePage *node;

  latch_.lock();
  bool root_locked = true;
  page = buffer_pool_manager_->FetchPage(root_page_id_);
  node = CastBPlusPage(page);
  page->WLatch();

  while (!node->IsLeafPage()) {
    InternalPage *internal_node = CastInternalPage(node);
    int index = internal_node->UpperBound(key, comparator_);

    Page *next_page = FetchChildPage(internal_node, index - 1);
    BPlusTreePage *next_node = CastInternalPage(next_page);
    // LOG_INFO("lookup to page %d at index %d", page->GetPageId(), idx - 1);

    next_page->WLatch();
    transaction->AddIntoPageSet(page);

    if (IsSafeInsert(next_node)) {
      FreePageSet(transaction, false);
      if (root_locked) {
        root_locked = false;
        latch_.unlock();
      }
    }

    node = next_node;
    page = next_page;
  }

  LeafPage *leaf_node = CastLeafPage(page);

  if (IsSafeInsert(leaf_node)) {
    if (leaf_node->IsRootPage()) {  // 如果叶子节点就是root
      latch_.unlock();
      root_locked = false;
    }
    FreePageSet(transaction, false);
    bool inserted = InsertInLeaf(leaf_node, key, value);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), inserted);
    // buffer_pool_manager_->CheckPinCount();
    assert(!root_locked);
    return inserted;
  }

  // 直接插入，空间是足够的
  bool inserted = InsertInLeaf(leaf_node, key, value);
  // 插入失败，重复了
  if (!inserted) {
    if (root_locked) {
      latch_.unlock();
      root_locked = false;
    }
    FreePageSet(transaction, false);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    // buffer_pool_manager_->CheckPinCount();
    assert(!root_locked);
    return false;
  }

  // split leaf
  int x = leaf_node->GetMaxSize() / 2;

  int right_page_id;
  Page *right_page = NewLeafPage(&right_page_id, leaf_node->GetParentPageId());
  LeafPage *right_node = CastLeafPage(right_page);

  right_node->Copy(leaf_node, 0, x, leaf_node->GetMaxSize());

  leaf_node->SetSize(x);
  right_node->SetSize(leaf_node->GetMaxSize() - x);

  if (right_most_ == leaf_node->GetPageId()) {
    right_most_ = right_page_id;
  }

  KeyType key0 = right_node->KeyAt(0);
  InsertInParent(leaf_node, right_node, key0, right_page_id, root_locked, transaction);

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);

  auto vec = transaction->GetPageSet();
  while (!vec->empty()) {
    auto parent_page = vec->front();
    vec->pop_front();
    parent_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);  // ?
  }
  // buffer_pool_manager_->CheckPinCount();
  assert(!root_locked);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInLeaf(LeafPage *leaf_node, const KeyType &key, const ValueType &value) -> bool {
  int index = leaf_node->LowerBound(key, comparator_);
  if (index < leaf_node->GetSize() && comparator_(leaf_node->KeyAt(index), key) == 0) {
    // LOG_INFO("\033[1;31m %d key %ld insert fail !\033[0m", ::gettid(), key.ToString());
    return false;
  }
  leaf_node->InsertAt(index, key, value);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *left_node, BPlusTreePage *right_node, const KeyType &key,
                                    page_id_t value, bool &root_locked, Transaction *transaction) {
  if (left_node->IsRootPage()) {
    assert(root_locked);
    // LOG_INFO("leaf page %d split noroot", left_page->GetPageId());
    Page *root_page{};

    // LOG_INFO("\033[1;42m root_split \033[0m");
    root_page = NewInternalRootPage(&root_page_id_);
    // LOG_INFO("\033[1;42m new internal root %d \033[0m", root_page_id_);

    InternalPage *root_node = CastInternalPage(root_page);

    root_node->SetValueAt(0, left_node->GetPageId());
    root_node->SetKeyAt(1, key);
    root_node->SetValueAt(1, right_node->GetPageId());
    root_node->SetSize(2);

    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    latch_.unlock();  // ！！！
    root_locked = false;

    left_node->SetParentPageId(root_page_id_);
    right_node->SetParentPageId(root_page_id_);

    if (left_node->IsLeafPage()) {
      auto leafpage = CastLeafPage(left_node);
      leafpage->SetNextPageId(right_node->GetPageId());
    }
    return;
  }

  // 有internal parent
  Page *parent_page = transaction->GetPageSet()->back();
  transaction->GetPageSet()->pop_back();
  // Page *parent_page = FetchParentPage(left_node);
  InternalPage *parent_node = CastInternalPage(parent_page);

  assert(parent_node->GetSize() <= parent_node->GetMaxSize());

  if (left_node->IsLeafPage()) {
    auto left_leaf_node = CastLeafPage(left_node);
    auto right_leaf_node = CastLeafPage(right_node);
    right_leaf_node->SetNextPageId(left_leaf_node->GetNextPageId());
    left_leaf_node->SetNextPageId(right_node->GetPageId());
  }

  // parent有空位
  if (IsSafeInsert(parent_node)) {
    int index = parent_node->LowerBound(key, comparator_);
    assert(index > 0);
    parent_node->InsertAt(index, key, right_node->GetPageId());
    if (parent_node->IsRootPage()) {
      assert(root_locked);
      root_locked = false;
      latch_.unlock();
    }
    // LOG_INFO("leaf page %d split parent_page id %d at index %d ", left_page->GetPageId(), parent_page->GetPageId(),
    //  index);
  } else {
    // split Internal parent node

    // 找一个更大的空间存储，二分 然后插入
    auto vec = parent_node->DumpAll();

    int index = std::lower_bound(vec.begin() + 1, vec.end(), key,
                                 [&](const std::pair<KeyType, page_id_t> &l, const KeyType &r) {
                                   return comparator_(l.first, r) < 0;
                                 }) -
                vec.begin();
    vec.insert(vec.begin() + index, {key, right_node->GetPageId()});

    // 分裂
    int parent_right_page_id;
    Page *parent_right_page = NewInternalPage(&parent_right_page_id, parent_node->GetParentPageId());
    InternalPage *parent_right_node = CastInternalPage(parent_right_page);

    int x = parent_node->GetMinSize();
    parent_node->Copy(vec, 0, 0, x);
    parent_right_node->Copy(vec, 0, x, vec.size());
    parent_node->SetSize(x);
    parent_right_node->SetSize(vec.size() - x);
    assert(parent_right_node->GetSize() == internal_max_size_ - x + 1);

    UpdateChild(parent_right_node, 0, parent_right_node->GetSize());

    KeyType key0 = parent_right_node->KeyAt(0);
    parent_right_node->SetKeyAt(0, KeyType{});

    InsertInParent(parent_node, parent_right_node, key0, parent_right_node->GetPageId(), root_locked, transaction);
    UnPinPage(parent_right_node, true);
  }

  transaction->AddIntoPageSet(parent_page);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // LOG_INFO("\033[1;32mRemove %ld\033[0m", key.ToString());

  Page *page{};
  BPlusTreePage *node{};

  {
    latch_.lock();
    if (IsEmpty()) {
      latch_.unlock();
      return;
    }
    page = buffer_pool_manager_->FetchPage(root_page_id_);
    node = CastBPlusPage(page);

    if (node->IsLeafPage()) {
      page->WLatch();
      if (!IsSafeRemove(node)) {
        page->WUnlatch();
        latch_.unlock();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        return RemovePessimistic(key, transaction);
      }
    } else {
      page->RLatch();
    }
    latch_.unlock();
  }

  while (!node->IsLeafPage()) {
    InternalPage *internal_node = CastInternalPage(node);
    int index = internal_node->UpperBound(key, comparator_);

    Page *next_page = FetchChildPage(internal_node, index - 1);
    BPlusTreePage *next_node = CastInternalPage(next_page);

    if (next_node->IsLeafPage()) {
      next_page->WLatch();
    } else {
      next_page->RLatch();
    }

    page->RUnlatch();
    UnPinPage(node, false);

    node = next_node;
    page = next_page;
  }

  LeafPage *leaf_node = CastLeafPage(page);

  if (IsSafeRemove(leaf_node)) {
    bool removed = leaf_node->Remove(key, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), removed);
    // buffer_pool_manager_->CheckPinCount();
    return;
  }

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  // buffer_pool_manager_->CheckPinCount();
  RemovePessimistic(key, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemovePessimistic(const KeyType &key, Transaction *transaction) {
  Page *page;
  BPlusTreePage *node;

  latch_.lock();
  bool root_locked = true;
  page = buffer_pool_manager_->FetchPage(root_page_id_);
  node = CastBPlusPage(page);
  page->WLatch();

  while (!node->IsLeafPage()) {
    InternalPage *internal_node = CastInternalPage(node);
    int index = internal_node->UpperBound(key, comparator_);

    Page *next_page = FetchChildPage(internal_node, index - 1);
    BPlusTreePage *next_node = CastInternalPage(next_page);
    // LOG_INFO("lookup to page %d at index %d", page->GetPageId(), idx - 1);

    next_page->WLatch();
    // TODO(wxx) error: Called C++ object pointer is null ????
    transaction->AddIntoPageSet(page);

    if (next_node->GetParentPageId() != node->GetPageId()) {
      next_node->SetParentPageId(node->GetPageId());
    }

    if (IsSafeRemove(next_node)) {
      FreePageSet(transaction, false);
      if (root_locked) {
        root_locked = false;
        latch_.unlock();
      }
    }

    node = next_node;
    page = next_page;
  }

  // size < min_size
  bool removed = RemoveEntry(page, key, -1, root_locked, transaction);
  assert(!root_locked);

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), removed);
  auto set = transaction->GetDeletedPageSet();
  FreePageSet(transaction, false);
  for (auto page_id : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();
  // buffer_pool_manager_->CheckPinCount();
}

/**
 * @param key   ues to remove kv pair in leaf
 * @param position  use to remove kv pair in internal
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveEntry(Page *page, const KeyType &key, int position, bool &root_locked,
                                 Transaction *transaction) -> bool {
  BPlusTreePage *node = CastBPlusPage(page);
  // delete k from page
  bool removed = false;

  if (node->IsLeafPage()) {
    LeafPage *leaf_node = CastLeafPage(page);

    // 再次判断能否安全移除不会
    if (IsSafeRemove(leaf_node)) {
      if (leaf_node->IsRootPage()) {
        latch_.unlock();
        root_locked = false;
      }
      FreePageSet(transaction, false);
      removed = leaf_node->Remove(key, comparator_);
      return removed;
    }

    // not safe will merge
    removed = leaf_node->Remove(key, comparator_);
    if (!removed) {
      if (root_locked) {
        latch_.unlock();
        root_locked = false;
      }
      FreePageSet(transaction, false);
      return false;
    }

    assert(leaf_node->GetSize() < leaf_node->GetMinSize());

  } else {
    // already in pageset, called recrusively
    InternalPage *internal_node = CastInternalPage(page);
    // 子叶合并已经提前锁住了
    internal_node->ShiftLeft(position);
    removed = true;
  }

  assert(removed);

  if (node->IsRootPage()) {
    if (node->IsLeafPage()) {
      if (node->GetSize() == 0) {
        transaction->AddIntoDeletedPageSet(node->GetPageId());
        root_page_id_ = INVALID_PAGE_ID;
      }
    } else {
      InternalPage *internal_node = CastInternalPage(node);
      if (internal_node->GetSize() == 1) {
        root_page_id_ = internal_node->ValueAt(0);
        UpdateRootPageId();

        Page *new_root_page = FetchChildPage(internal_node, 0);
        BPlusTreePage *new_root_node = CastBPlusPage(new_root_page);
        new_root_node->SetParentPageId(INVALID_PAGE_ID);
        UnPinPage(new_root_node, true);

        left_most_ = right_most_ = root_page_id_;
        transaction->AddIntoDeletedPageSet(internal_node->GetPageId());
      }
    }
    latch_.unlock();
    root_locked = false;
    return true;
  }

  //  N has too few values/pointers
  if (node->GetSize() < node->GetMinSize()) {
    Page *parent_page = transaction->GetPageSet()->back();
    InternalPage *parent_node = CastInternalPage(parent_page);

    int node_position = parent_node->FindValue(node->GetPageId());

    if (node_position == 0) {
      Page *right_page = FetchChildPage(parent_node, 1);
      right_page->WLatch();

      BPlusTreePage *right_node = CastBPlusPage(right_page);
      assert(right_node->GetSize() >= right_node->GetMinSize());

      if (right_node->GetSize() > right_node->GetMinSize()) {
        if (root_locked) {
          latch_.unlock();
          root_locked = false;
        }
        BorrowFromRight(node, right_node, node_position, transaction);
      } else {
        Merge(node, right_node, node_position, root_locked, transaction);
      }

      right_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);

    } else {
      Page *left_page = FetchChildPage(parent_node, node_position - 1);
      left_page->WLatch();

      BPlusTreePage *left_node = CastBPlusPage(left_page);
      assert(left_node->GetSize() >= left_node->GetMinSize());

      if (left_node->GetSize() > left_node->GetMinSize()) {
        if (root_locked) {
          latch_.unlock();
          root_locked = false;
        }
        BorrowFromLeft(node, left_node, node_position, transaction);
      } else {
        Merge(left_node, node, node_position - 1, root_locked, transaction);
      }

      left_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_page->GetPageId(), true);
    }
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromLeft(BPlusTreePage *page, BPlusTreePage *left_page, int left_position,
                                    Transaction *transaction) {
  Page *parent_page = transaction->GetPageSet()->back();
  InternalPage *parent_node = CastInternalPage(parent_page);

  if (page->IsLeafPage()) {
    LeafPage *left_node = CastLeafPage(left_page);
    LeafPage *right_node = CastLeafPage(page);

    right_node->ShiftRight();                                             // shift right to make space
    right_node->SetKeyAt(0, left_node->KeyAt(left_node->GetSize() - 1));  // borrow last fram left
    right_node->SetValueAt(0, left_node->ValueAt(left_node->GetSize() - 1));
    left_node->IncreaseSize(-1);

    parent_node->SetKeyAt(left_position + 1, right_node->KeyAt(0));
  } else {
    InternalPage *left_node = CastInternalPage(left_page);
    InternalPage *right_node = CastInternalPage(page);

    right_node->ShiftRight();
    right_node->SetKeyAt(1, parent_node->KeyAt(left_position + 1));           // bring down the key  from parent
    right_node->SetValueAt(0, left_node->ValueAt(left_node->GetSize() - 1));  // bring the last child from left
    left_node->IncreaseSize(-1);

    // Page *child_page = FetchChildPage(right_node, left_node->GetSize() - 1);
    // child_page->WLatch();
    // BPlusTreePage *child_node = CastBPlusPage(child_page);
    // child_node->SetParentPageId(left_node->GetPageId());
    // child_page->WUnlatch();
    // buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);

    // send up last key of the left to the parent
    parent_node->SetKeyAt(left_position + 1, left_node->KeyAt(left_node->GetSize() - 1));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromRight(BPlusTreePage *page, BPlusTreePage *right_page, int left_position,
                                     Transaction *transaction) {
  Page *parent_page = transaction->GetPageSet()->back();
  InternalPage *parent_node = CastInternalPage(parent_page);
  if (page->IsLeafPage()) {
    LeafPage *left_node = CastLeafPage(page);
    LeafPage *right_node = CastLeafPage(right_page);

    left_node->SetKeyAt(left_node->GetSize(), right_node->KeyAt(0));  // borrow the first pair from right
    left_node->SetValueAt(left_node->GetSize(), right_node->ValueAt(0));
    left_node->IncreaseSize(1);
    right_node->ShiftLeft();

    parent_node->SetKeyAt(left_position + 1, right_node->KeyAt(0));  // shift left by one in right
  } else {
    InternalPage *left_node = CastInternalPage(page);
    InternalPage *right_node = CastInternalPage(right_page);

    // bring down the right key from parent
    left_node->SetKeyAt(left_node->GetSize(), parent_node->KeyAt(left_position + 1));
    left_node->SetValueAt(left_node->GetSize(), right_node->ValueAt(0));  // bring the first child from right
    left_node->IncreaseSize(1);

    // Page *child_page = FetchChildPage(right_node, 0);
    // child_page->WLatch();
    // BPlusTreePage *child_node = CastBPlusPage(child_page);
    // child_node->SetParentPageId(left_node->GetPageId());
    // child_page->WUnlatch();
    // buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);

    parent_node->SetKeyAt(left_position + 1, right_node->KeyAt(1));  // send up first key of the right to the parent
    right_node->ShiftLeft();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(BPlusTreePage *left_node, BPlusTreePage *right_node, int posOfLeftPage, bool &root_locked,
                           Transaction *transaction) {
  // LOG_INFO("Merge page %d %d pos_of_left %d", left->GetPageId(), right->GetPageId(), posOfLeftPage);
  Page *parent_page = transaction->GetPageSet()->back();
  InternalPage *parent_node = CastInternalPage(parent_page);
  parent_page = buffer_pool_manager_->FetchPage(parent_page->GetPageId());

  if (left_node->IsLeafPage()) {
    LeafPage *left_leaf_node = CastLeafPage(left_node);
    LeafPage *right_leaf_node = CastLeafPage(right_node);
    left_leaf_node->Copy(right_leaf_node, left_node->GetSize(), 0, right_leaf_node->GetSize());
    left_leaf_node->SetNextPageId(right_leaf_node->GetNextPageId());
    if (right_leaf_node->GetPageId() == right_most_) {
      right_most_ = left_leaf_node->GetPageId();
    }
  } else {
    InternalPage *left_internal_node = CastInternalPage(left_node);
    InternalPage *right_internal_node = CastInternalPage(right_node);
    // int old_size = left_internal_node->GetSize();

    left_internal_node->SetKeyAt(left_internal_node->GetSize(), parent_node->KeyAt(posOfLeftPage + 1));
    left_internal_node->SetValueAt(left_internal_node->GetSize(), right_internal_node->ValueAt(0));
    left_internal_node->IncreaseSize(1);

    left_internal_node->Copy(right_internal_node, left_internal_node->GetSize(), 1, right_internal_node->GetSize());
    // UpdateChild(left_internal_node, old_size, left_node->GetSize());
  }
  transaction->AddIntoDeletedPageSet(right_node->GetPageId());
  transaction->GetPageSet()->pop_back();

  bool removed = RemoveEntry(parent_page, KeyType{}, posOfLeftPage + 1, root_locked, transaction);
  assert(removed);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), removed);

  transaction->AddIntoPageSet(parent_page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateChild(InternalPage *page, int first, int last) {
  for (int i = first; i < last; i++) {
    Page *child_page = FetchChildPage(page, i);
    // child_page->WLatch();
    BPlusTreePage *child_node = CastBPlusPage(child_page);
    child_node->SetParentPageId(page->GetPageId());
    // child_page->WUnlatch();
    UnPinPage(child_node, true);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Page *page = buffer_pool_manager_->FetchPage(left_most_);
  page->RLatch();
  return Iterator{page, 0, buffer_pool_manager_};
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  page->RLatch();
  BPlusTreePage *node = CastBPlusPage(page);

  if (!node->IsRootPage()) {
    // LOG_INFO("\033[1;43m %d root_change\033[0m", ::gettid());
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    if (node->GetSize() == 0 && page->GetPinCount() == 0) {
      buffer_pool_manager_->DeletePage(page->GetPageId());
    }
    return Begin(key);
  }

  while (!node->IsLeafPage()) {
    InternalPage *internal_node = CastInternalPage(node);
    int index = internal_node->UpperBound(key, comparator_);
    // LOG_INFO("lookup to page %d at index %d", page->GetPageId(), idx - 1);
    Page *next_page = FetchChildPage(internal_node, index - 1);
    next_page->RLatch();

    page->RUnlatch();
    UnPinPage(node, false);

    BPlusTreePage *next_node = CastInternalPage(next_page);
    node = next_node;
    page = next_page;
  }

  LeafPage *leaf_node = CastLeafPage(page);
  int index = leaf_node->LowerBound(key, comparator_);

  // LOG_INFO("lookup int leaf page %d at index %d", page->GetPageId(), index);
  assert(index <= leaf_node->GetSize());

  if (index < leaf_node->GetSize() && comparator_(leaf_node->KeyAt(index), key) == 0) {
    return Iterator{page, index, buffer_pool_manager_};
  }

  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  Page *page = buffer_pool_manager_->FetchPage(right_most_);
  BPlusTreePage *node = CastBPlusPage(page);
  page->RLatch();
  return Iterator{page, node->GetSize(), buffer_pool_manager_};
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  std::cout << "left_most_:" << left_most_ << " right_most_:" << right_most_ << std::endl;
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << " Pa=" << leaf->GetParentPageId()
        << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << " Pa=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewLeafPage(page_id_t *page_id, page_id_t parent_id) -> Page * {
  Page *page = buffer_pool_manager_->NewPage(page_id);
  LeafPage *node = CastLeafPage(page);
  node->Init(*page_id, parent_id, leaf_max_size_);
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewLeafRootPage(page_id_t *page_id) -> Page * {
  Page *page = buffer_pool_manager_->NewPage(page_id);
  LeafPage *node = CastLeafPage(page);
  node->Init(*page_id, INVALID_PAGE_ID, leaf_max_size_);
  left_most_ = *page_id;
  right_most_ = *page_id;
  UpdateRootPageId(0);
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewInternalPage(page_id_t *page_id, page_id_t parent_id) -> Page * {
  Page *page = buffer_pool_manager_->NewPage(page_id);
  InternalPage *node = CastInternalPage(page);
  node->Init(*page_id, parent_id, internal_max_size_);
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewInternalRootPage(page_id_t *page_id) -> Page * {
  Page *page = buffer_pool_manager_->NewPage(page_id);
  InternalPage *node = CastInternalPage(page);
  node->Init(*page_id, INVALID_PAGE_ID, internal_max_size_);
  UpdateRootPageId(0);
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchChildPage(InternalPage *node, int index) const -> Page * {
  page_id_t page_id = node->ValueAt(index);
  return buffer_pool_manager_->FetchPage(page_id);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UnPinPage(BPlusTreePage *node, bool is_dirty) const {
  buffer_pool_manager_->UnpinPage(node->GetPageId(), is_dirty);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeletePage(BPlusTreePage *node) const -> bool {
  return buffer_pool_manager_->DeletePage(node->GetPageId());
}

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::GetLevel() const -> int {
// }

/**
 * @brief leaf < maxsize - 1 or internal < maxsize
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafeInsert(BPlusTreePage *node) -> bool {
  if (node->IsLeafPage()) {
    return node->GetSize() < node->GetMaxSize() - 1;
  }
  return node->GetSize() < node->GetMaxSize();
}

/**
 * @brief it means the page will not coalesce after remove.
 *        if page is root_page return true, otherwise return size > minSize.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafeRemove(BPlusTreePage *node) -> bool {
  if (node->IsRootPage()) {
    return node->GetSize() > 1;
  }
  return node->GetSize() > node->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreePageSet(Transaction *transaction, bool is_dirty) {
  auto vec = transaction->GetPageSet();
  while (!vec->empty()) {
    auto page = vec->front();
    vec->pop_front();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), is_dirty);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
