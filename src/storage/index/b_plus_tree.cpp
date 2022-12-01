#include "storage/index/b_plus_tree.h"
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <cassert>
#include <cstddef>
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
#include "type/value.h"

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
  bool root_locked;
  page = FindLeafPage(key, Operation::FIND, true, root_locked, transaction).first;
  if (page == nullptr) {
    return false;
  }

  LeafPage *leaf_node = CastLeafPage(page);
  ValueType value;
  bool find = leaf_node->GetValue(key, value, comparator_);
  if (find) {
    result->push_back(value);
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  assert(!root_locked);
  return find;
}

/*
 * @brief
 *
 * @param key
 * @param operation   FIND INSERT REMOVE
 * @param optimistic  optimistic or perssmistic
 * @param root_locked
 * @param transaction
 * @return std::pair<Page *, bool>
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Operation operation, bool optimistic, bool &root_locked,
                                  Transaction *transaction) -> std::pair<Page *, bool> {
  Page *page{};
  BPlusTreePage *node;

  if (operation == FIND) {
    latch_.lock();
    root_locked = true;
    if (IsEmpty()) {
      latch_.unlock();
      root_locked = false;
      return {nullptr, false};
    }
    page = buffer_pool_manager_->FetchPage(root_page_id_);
    node = CastBPlusPage(page);
    page->RLatch();
    latch_.unlock();
    root_locked = false;

    while (!node->IsLeafPage()) {
      InternalPage *internal_node = CastInternalPage(node);
      int index = internal_node->UpperBound(key, comparator_);
      // LOG_INFO("lookup to page %d at index %d", page->GetPageId(), idx - 1);
      Page *next_page = FetchChildPage(internal_node, index - 1);
      next_page->RLatch();
      assert(CastBPlusPage(next_page)->GetParentPageId() == internal_node->GetPageId());
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

      BPlusTreePage *next_node = CastInternalPage(next_page);
      node = next_node;
      page = next_page;
    }
    return {page, false};
  }

  // root_page 安全前不能释放锁！！！！获取rootpage时一定要是安全的
  // 乐观insert, root Rlatch ,
  // 悲观insert, root Wlatch ,
  latch_.lock();
  root_locked = true;
  if (IsEmpty()) {
    if (operation == INSERT) {
      page = NewLeafRootPage(&root_page_id_);
    } else {
      latch_.unlock();
      root_locked = false;
      return {nullptr, false};
    }
  } else {
    page = buffer_pool_manager_->FetchPage(root_page_id_);
  }

  node = CastBPlusPage(page);

  if (optimistic) {
    if (node->IsLeafPage()) {
      // 当root 是leaf时，判断root安全插入才能释放锁
      // 否则提前释放了锁，root在这一个insert中分裂， 而别的线程依旧在这个旧的root中插入
      page->WLatch();
      if (!IsSafe(node, operation)) {
        page->WUnlatch();
        latch_.unlock();
        root_locked = false;
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        return {nullptr, true};
      }
    } else {
      page->RLatch();
    }
    latch_.unlock();
    root_locked = false;

    while (!node->IsLeafPage()) {
      InternalPage *internal_node = CastInternalPage(node);
      int index = internal_node->Search(key, comparator_);

      Page *next_page = FetchChildPage(internal_node, index);
      BPlusTreePage *next_node = CastInternalPage(next_page);

      if (next_node->IsLeafPage()) {
        next_page->WLatch();
      } else {
        next_page->RLatch();
      }

      assert(next_node->GetParentPageId() == internal_node->GetPageId());

      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

      node = next_node;
      page = next_page;
    }
    return {page, false};
  }

  assert(transaction != nullptr);
  // perssimistic
  page->WLatch();
  while (!node->IsLeafPage()) {
    InternalPage *internal_node = CastInternalPage(node);
    int index = internal_node->Search(key, comparator_);
    Page *next_page = FetchChildPage(internal_node, index);
    BPlusTreePage *next_node = CastInternalPage(next_page);
    // LOG_INFO("lookup to page %d at index %d", page->GetPageId(), idx - 1);
    next_page->WLatch();
    transaction->AddIntoPageSet(page);
    assert(next_node->GetParentPageId() == internal_node->GetPageId());
    if (IsSafe(next_node, operation)) {
      ClearPageSet(transaction);
      if (root_locked) {
        latch_.unlock();
        root_locked = false;
      }
    }
    node = next_node;
    page = next_page;
  }

  return {page, false};
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
  bool root_locked = false;
  auto [page, again] = FindLeafPage(key, INSERT, true, root_locked, transaction);

  if (again) {
    assert(!root_locked);
    return InsertPessimistic(key, value, transaction);
  }

  LeafPage *leaf_node = CastLeafPage(page);

  if (IsSafe(leaf_node, INSERT)) {
    bool inserted = leaf_node->Insert(key, value, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), inserted);
    return inserted;
  }

  // restart pessimistic
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return InsertPessimistic(key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertPessimistic(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // LOG_INFO("%d InsertPessimistic", ::gettid());
  bool root_locked = false;
  auto page = FindLeafPage(key, INSERT, false, root_locked, transaction).first;

  LeafPage *leaf_node = CastLeafPage(page);

  if (IsSafe(leaf_node, INSERT)) {
    if (leaf_node->IsRootPage()) {  // 如果叶子节点就是root
      latch_.unlock();
      root_locked = false;
    }
    ClearPageSet(transaction);
    bool inserted = leaf_node->Insert(key, value, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), inserted);
    // buffer_pool_manager_->CheckPinCount();
    assert(!root_locked);
    return inserted;
  }

  // 直接插入，空间是足够的
  bool inserted = leaf_node->Insert(key, value, comparator_);
  // 插入失败，重复了
  if (!inserted) {
    ClearPageSet(transaction);
    if (root_locked) {
      latch_.unlock();
      root_locked = false;
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
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
  if (IsSafe(parent_node, INSERT)) {
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
    buffer_pool_manager_->UnpinPage(parent_right_page_id, true);
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

  bool root_locked = false;
  auto [page, again] = FindLeafPage(key, REMOVE, true, root_locked, transaction);

  if (page == nullptr) {
    assert(!root_locked);
    if (again) {
      return RemovePessimistic(key, transaction);  // root can be delete in this case
    }
    return;  // tree empty
  }

  LeafPage *leaf_node = CastLeafPage(page);

  if (IsSafe(leaf_node, REMOVE)) {
    bool removed = leaf_node->Remove(key, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), removed);
    // buffer_pool_manager_->CheckPinCount();
    return;
  }

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  // buffer_pool_manager_->CheckPinCount();
  assert(!root_locked);
  RemovePessimistic(key, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemovePessimistic(const KeyType &key, Transaction *transaction) {
  bool root_locked = false;
  auto [page, again] = FindLeafPage(key, REMOVE, false, root_locked, transaction);
  if (page == nullptr) {  // is empty
    assert(!root_locked);
    return;
  }

  // size < min_size
  bool removed = RemoveEntry(page, key, -1, root_locked, transaction);

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), removed);
  ClearPageSet(transaction);

  auto set = transaction->GetDeletedPageSet();
  for (auto page_id : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();

  // 先删后unlock
  if (root_locked) {
    latch_.unlock();
    root_locked = false;
  }
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

    if (IsSafe(leaf_node, REMOVE)) {
      removed = leaf_node->Remove(key, comparator_);
      return removed;
    }

    // not safe will merge
    removed = leaf_node->Remove(key, comparator_);
    if (!removed) {
      return false;
    }

    assert(leaf_node->GetSize() < leaf_node->GetMinSize());

  } else {
    InternalPage *internal_node = CastInternalPage(page);
    internal_node->ShiftLeft(position);
    removed = true;
  }

  assert(removed);

  if (node->IsRootPage()) {
    assert(root_locked);
    if (node->IsLeafPage()) {
      if (node->GetSize() == 0) {
        transaction->AddIntoDeletedPageSet(node->GetPageId());
        root_page_id_ = INVALID_PAGE_ID;
      }
    } else {
      InternalPage *internal_node = CastInternalPage(node);
      if (internal_node->GetSize() == 1) {
        Page *new_root_page = FetchChildPage(internal_node, 0);
        root_page_id_ = new_root_page->GetPageId();
        UpdateRootPageId();

        BPlusTreePage *new_root_node = CastBPlusPage(new_root_page);
        new_root_node->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(new_root_node->GetPageId(), true);

        if (left_most_ == root_page_id_) {
          left_most_ = right_most_ = root_page_id_;
        }
        transaction->AddIntoDeletedPageSet(internal_node->GetPageId());
      }
    }
    return true;
  }

  assert(!node->IsLeafPage() ? node->GetSize() >= 1 : true);

  //  N has too few values/pointers
  if (node->GetSize() < node->GetMinSize()) {
    Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    InternalPage *parent_node = CastInternalPage(parent_page);

    int node_position = parent_node->FindValue(node->GetPageId());

    if (node_position == 0) {
      Page *right_page = FetchChildPage(parent_node, 1);
      right_page->WLatch();

      BPlusTreePage *right_node = CastBPlusPage(right_page);
      assert(right_node->GetSize() >= right_node->GetMinSize());

      if (right_node->GetSize() > right_node->GetMinSize()) {
        // 会影响fa，但不会影响fa fa
        auto deque = transaction->GetPageSet();
        if (deque->size() > 1) {
          deque->pop_back();
          ClearPageSet(transaction);
          deque->push_back(parent_page);
          if (root_locked) {
            latch_.unlock();
            root_locked = false;
          }
        }
        BorrowFromRight(node, right_node, node_position, transaction);
        right_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);
      } else {
        Merge(node, right_node, node_position, root_locked, transaction);
        right_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(right_page->GetPageId(), false);
        transaction->AddIntoDeletedPageSet(right_page->GetPageId());
      }

    } else {
      Page *left_page = FetchChildPage(parent_node, node_position - 1);
      left_page->WLatch();

      BPlusTreePage *left_node = CastBPlusPage(left_page);
      assert(left_node->GetSize() >= left_node->GetMinSize());

      if (left_node->GetSize() > left_node->GetMinSize()) {
        auto deque = transaction->GetPageSet();
        if (deque->size() > 1) {
          deque->pop_back();
          ClearPageSet(transaction);
          deque->push_back(parent_page);
          if (root_locked) {
            latch_.unlock();
            root_locked = false;
          }
        }
        BorrowFromLeft(node, left_node, node_position, transaction);
      } else {
        Merge(left_node, node, node_position - 1, root_locked, transaction);
        // buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
        transaction->AddIntoDeletedPageSet(node->GetPageId());
      }
      left_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_page->GetPageId(), true);
    }

    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromLeft(BPlusTreePage *node, BPlusTreePage *left_node, int left_position,
                                    Transaction *transaction) {
  Page *parent_page = transaction->GetPageSet()->back();
  InternalPage *parent_node = CastInternalPage(parent_page);

  if (node->IsLeafPage()) {
    LeafPage *left_leaf_node = CastLeafPage(left_node);
    LeafPage *right_leaf_node = CastLeafPage(node);

    right_leaf_node->ShiftRight();                                                       // shift right to make space
    right_leaf_node->SetKeyAt(0, left_leaf_node->KeyAt(left_leaf_node->GetSize() - 1));  // borrow last fram left
    right_leaf_node->SetValueAt(0, left_leaf_node->ValueAt(left_leaf_node->GetSize() - 1));
    left_leaf_node->IncreaseSize(-1);

    parent_node->SetKeyAt(left_position + 1, right_leaf_node->KeyAt(0));
  } else {
    InternalPage *left_internal_node = CastInternalPage(left_node);
    InternalPage *right_internal_node = CastInternalPage(node);

    right_internal_node->ShiftRight();
    right_internal_node->SetKeyAt(1, parent_node->KeyAt(left_position + 1));  // bring down the key  from parent
    // bring the last child from left
    right_internal_node->SetValueAt(0, left_internal_node->ValueAt(left_internal_node->GetSize() - 1));
    left_internal_node->IncreaseSize(-1);
    // send up last key of the left to the parent
    parent_node->SetKeyAt(left_position + 1, left_internal_node->KeyAt(left_internal_node->GetSize() - 1));

    Page *child_page = FetchChildPage(right_internal_node, 0);
    BPlusTreePage *child_node = CastBPlusPage(child_page);
    child_node->SetParentPageId(right_internal_node->GetPageId());
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromRight(BPlusTreePage *node, BPlusTreePage *right_node, int left_position,
                                     Transaction *transaction) {
  Page *parent_page = transaction->GetPageSet()->back();
  InternalPage *parent_node = CastInternalPage(parent_page);
  if (node->IsLeafPage()) {
    LeafPage *left_leaf_node = CastLeafPage(node);
    LeafPage *right_leaf_node = CastLeafPage(right_node);

    left_leaf_node->SetKeyAt(left_leaf_node->GetSize(), right_leaf_node->KeyAt(0));  // borrow the first pair from right
    left_leaf_node->SetValueAt(left_leaf_node->GetSize(), right_leaf_node->ValueAt(0));
    left_leaf_node->IncreaseSize(1);
    right_leaf_node->ShiftLeft();

    parent_node->SetKeyAt(left_position + 1, right_leaf_node->KeyAt(0));  // shift left by one in right
  } else {
    InternalPage *left_internal_node = CastInternalPage(node);
    InternalPage *right_internal_node = CastInternalPage(right_node);

    // bring down the right key from parent
    left_internal_node->SetKeyAt(left_internal_node->GetSize(), parent_node->KeyAt(left_position + 1));
    // bring the first child from right
    left_internal_node->SetValueAt(left_internal_node->GetSize(), right_internal_node->ValueAt(0));
    left_internal_node->IncreaseSize(1);

    // send up first key of the right to the parent
    parent_node->SetKeyAt(left_position + 1, right_internal_node->KeyAt(1));
    right_internal_node->ShiftLeft();

    Page *child_page = FetchChildPage(left_internal_node, left_internal_node->GetSize() - 1);
    BPlusTreePage *child_node = CastBPlusPage(child_page);
    child_node->SetParentPageId(left_internal_node->GetPageId());
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(BPlusTreePage *left_node, BPlusTreePage *right_node, int posOfLeftPage, bool &root_locked,
                           Transaction *transaction) {
  // LOG_INFO("Merge page %d %d pos_of_left %d", left->GetPageId(), right->GetPageId(), posOfLeftPage);
  Page *parent_page = transaction->GetPageSet()->back();
  InternalPage *parent_node = CastInternalPage(parent_page);

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
    int old_size = left_internal_node->GetSize();

    left_internal_node->SetKeyAt(left_internal_node->GetSize(), parent_node->KeyAt(posOfLeftPage + 1));
    left_internal_node->SetValueAt(left_internal_node->GetSize(), right_internal_node->ValueAt(0));
    left_internal_node->IncreaseSize(1);

    left_internal_node->Copy(right_internal_node, left_internal_node->GetSize(), 1, right_internal_node->GetSize());
    UpdateChild(left_internal_node, old_size, left_node->GetSize());
  }
  assert(parent_node->GetSize() >= 2);
  transaction->GetPageSet()->pop_back();
  RemoveEntry(parent_page, KeyType{}, posOfLeftPage + 1, root_locked, transaction);
  transaction->AddIntoPageSet(parent_page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateChild(InternalPage *node, int first, int last) {
  // 不要试图锁住孩子，会发生死锁，
  for (int i = first; i < last; i++) {
    Page *child_page = FetchChildPage(node, i);
    BPlusTreePage *child_node = CastBPlusPage(child_page);
    child_node->SetParentPageId(node->GetPageId());
    buffer_pool_manager_->UnpinPage(child_node->GetPageId(), true);
  }
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
auto BPLUSTREE_TYPE::IsSafe(BPlusTreePage *node, Operation operation) -> bool {
  if (operation == INSERT) {
    return node->IsLeafPage() ? node->GetSize() < node->GetMaxSize() - 1 : node->GetSize() < node->GetMaxSize();
  }
  return node->IsRootPage() ? node->GetSize() > 1 : node->GetSize() > node->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ClearPageSet(Transaction *transaction, bool free_root) {
  auto deque = transaction->GetPageSet();
  while (!deque->empty()) {
    auto page = deque->front();
    deque->pop_front();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
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
  latch_.lock();
  if (IsEmpty()) {
    latch_.unlock();
    return End();
  }
  Page *page = buffer_pool_manager_->FetchPage(left_most_);
  page->RLatch();
  latch_.unlock();
  return Iterator{page, 0, buffer_pool_manager_};
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  bool root_locked = false;
  Page *page = FindLeafPage(key, FIND, true, root_locked, nullptr).first;
  assert(!root_locked);
  if (page == nullptr) {
    return {nullptr, 0, nullptr};
  }
  LeafPage *leaf_node = CastLeafPage(page);
  int index = leaf_node->LowerBound(key, comparator_);
  if (index == leaf_node->GetSize()) {
    return End();
  }
  return {page, index, buffer_pool_manager_};
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  std::lock_guard lock(latch_);
  if (IsEmpty()) {
    return {nullptr, 0, nullptr};
  }
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

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
