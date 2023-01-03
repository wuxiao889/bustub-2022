#include "storage/index/b_plus_tree.h"
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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
#include "storage/page/page.h"
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
  page = FindLeafPage(key, Operation::FIND, true, root_locked, transaction);
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
                                  Transaction *transaction) -> Page * {
  Page *page{};
  BPlusTreePage *node;

  if (operation == FIND) {
    latch_.lock();
    root_locked = true;
    if (IsEmpty()) {
      latch_.unlock();
      root_locked = false;
      return nullptr;
    }
    page = buffer_pool_manager_->FetchPage(root_page_id_);
    node = CastBPlusPage(page);
    page->RLatch();
    latch_.unlock();
    root_locked = false;

    while (!node->IsLeafPage()) {
      InternalPage *internal_node = CastInternalPage(node);
      int index = internal_node->Search(key, comparator_);
      // LOG_INFO("lookup to page %d at index %d", page->GetPageId(), idx - 1);
      Page *next_page = FetchChildPage(internal_node, index);
      assert(next_page);
      next_page->RLatch();

      // assert(CastBPlusPage(next_page)->GetParentPageId() == internal_node->GetPageId());
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

      BPlusTreePage *next_node = CastInternalPage(next_page);
      node = next_node;
      page = next_page;
    }
    return page;
  }

  // root_page 安全前不能释放锁！！！！获取rootpage时一定要是安全的
  // 乐观insert, root Rlatch ,
  // 悲观insert, root Wlatch ,
  latch_.lock();
  // LOG_INFO("\033[1;42m%d latch lock\033[0m\n", ::gettid());
  root_locked = true;
  if (IsEmpty()) {
    if (operation == INSERT) {
      page = NewLeafRootPage(&root_page_id_);
    } else {
      latch_.unlock();
      // LOG_INFO("\033[1;43m%d latch unlock\033[0m\n", ::gettid());
      root_locked = false;
      return nullptr;
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
    } else {
      page->RLatch();
      // LOG_INFO("\033[1;43m%d latch unlock\033[0m", ::gettid());
      latch_.unlock();
      root_locked = false;
    }

    while (!node->IsLeafPage()) {
      InternalPage *internal_node = CastInternalPage(node);
      int index = internal_node->Search(key, comparator_);

      Page *next_page = FetchChildPage(internal_node, index);
      BPlusTreePage *next_node = CastInternalPage(next_page);

      // FIXME(wxx) unsafe
      if (next_node->IsLeafPage()) {
        next_page->WLatch();
      } else {
        next_page->RLatch();
      }
      // assert(next_node->GetParentPageId() == internal_node->GetPageId());
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

      node = next_node;
      page = next_page;
    }
    return page;
  }

  assert(transaction != nullptr);
  // perssimisticf
  page->WLatch();
  if (IsSafe(node, operation)) {
    // LOG_INFO("\033[1;43m%d latch unlock\033[0m\n", ::gettid());
    latch_.unlock();
    root_locked = false;
  }

  while (!node->IsLeafPage()) {
    InternalPage *internal_node = CastInternalPage(node);
    int index = internal_node->Search(key, comparator_);
    Page *next_page = FetchChildPage(internal_node, index);
    BPlusTreePage *next_node = CastInternalPage(next_page);

    next_page->WLatch();
    transaction->AddIntoPageSet(page);
    assert(next_node->GetParentPageId() == internal_node->GetPageId());

    if (IsSafe(next_node, operation)) {
      UnlockRoot(root_locked);
      ClearPageSet(transaction);
    }
    node = next_node;
    page = next_page;
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
  // bool root_locked = false;
  // auto page = FindLeafPage(key, INSERT, true, root_locked, transaction);

  // LeafPage *leaf_node = CastLeafPage(page);

  // if (IsSafe(leaf_node, INSERT)) {
  //   bool inserted = leaf_node->Insert(key, value, comparator_);
  //   page->WUnlatch();
  //   buffer_pool_manager_->UnpinPage(page->GetPageId(), inserted);
  //   if (root_locked) {
  //     latch_.unlock();
  //     root_locked = false;
  //   }
  //   return inserted;
  // }

  // // restart pessimistic
  // page->WUnlatch();
  // buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  // if (root_locked) {
  //   latch_.unlock();
  //   root_locked = false;
  // }
  return InsertPessimistic(key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertPessimistic(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // LOG_INFO("%d InsertPessimistic", ::gettid());
  bool root_locked = false;
  auto page = FindLeafPage(key, INSERT, false, root_locked, transaction);

  LeafPage *leaf_node = CastLeafPage(page);

  if (IsSafe(leaf_node, INSERT)) {
    assert(!root_locked);
    bool inserted = leaf_node->Insert(key, value, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), inserted);
    // buffer_pool_manager_->CheckPinCount();
    return inserted;
  }

  // 直接插入，空间是足够的
  bool inserted = leaf_node->Insert(key, value, comparator_);
  // 插入失败，重复了
  if (!inserted) {
    UnlockRoot(root_locked);
    ClearPageSet(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    assert(!root_locked);
    return false;
  }

  int right_page_id;
  Page *right_page = NewLeafPage(&right_page_id, leaf_node->GetParentPageId());
  LeafPage *right_node = CastLeafPage(right_page);

  leaf_node->Split(right_node);
  right_node->SetNextPageId(leaf_node->GetNextPageId());
  leaf_node->SetNextPageId(right_node->GetPageId());

  if (right_most_ == leaf_node->GetPageId()) {
    right_most_ = right_page_id;
  }

  KeyType key0 = right_node->KeyAt(0);
  InsertInParent(leaf_node, right_node, key0, right_page_id, transaction);

  UnlockRoot(root_locked);
  ClearPageSet(transaction);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);
  // buffer_pool_manager_->CheckPinCount();
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *left_node, BPlusTreePage *right_node, const KeyType &key,
                                    page_id_t value, Transaction *transaction) {
  if (left_node->IsRootPage()) {
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

    left_node->SetParentPageId(root_page_id_);
    right_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }

  assert(transaction->GetPageSet()->empty()
             ? true
             : transaction->GetPageSet()->back()->GetPageId() == left_node->GetParentPageId());

  // 有internal parent
  Page *parent_page = buffer_pool_manager_->FetchPage(left_node->GetParentPageId());
  InternalPage *parent_node = CastInternalPage(parent_page);
  transaction->GetPageSet()->pop_back();

  assert(parent_node->GetSize() <= parent_node->GetMaxSize());

  // parent有空位
  if (IsSafe(parent_node, INSERT)) {
    int index = parent_node->LowerBound(key, comparator_);
    assert(index > 0);
    parent_node->InsertAt(index, key, right_node->GetPageId());
    // LOG_INFO("leaf page %d split parent_page id %d at index %d ", left_page->GetPageId(), parent_page->GetPageId(),
    //  index);
  } else {
    // split Internal parent node
    int parent_right_page_id;
    Page *parent_right_page = NewInternalPage(&parent_right_page_id, parent_node->GetParentPageId());
    InternalPage *parent_right_node = CastInternalPage(parent_right_page);

    parent_node->Split(parent_right_node, key, right_node->GetPageId(), comparator_);
    UpdateChild(parent_right_node, 0, parent_right_node->GetSize());

    KeyType key0 = parent_right_node->KeyAt(0);
    parent_right_node->SetKeyAt(0, KeyType{});

    InsertInParent(parent_node, parent_right_node, key0, parent_right_node->GetPageId(), transaction);

    buffer_pool_manager_->UnpinPage(parent_right_page->GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
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
  // LOG_INFO("\033[1;32mRemove %ld\033[0m\n", key.ToString());
  // mutex_.lock();
  // bool root_locked = false;
  // auto page = FindLeafPage(key, REMOVE, true, root_locked, transaction);

  // if (page == nullptr) {
  //   assert(!root_locked);
  //   return;  // tree empty
  // }

  // LeafPage *leaf_node = CastLeafPage(page);

  // if (IsSafe(leaf_node, REMOVE)) {
  //   bool removed = leaf_node->Remove(key, comparator_);
  //   page->WUnlatch();
  //   buffer_pool_manager_->UnpinPage(page->GetPageId(), removed);
  //   // mutex_.unlock();
  //   if (root_locked) {
  //     // LOG_INFO("\033[1;43m%d latch unlock\033[0m", ::gettid());
  //     latch_.unlock();
  //     root_locked = false;
  //   }
  //   return;
  // }

  // page->WUnlatch();
  // buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  // // assert(!root_locked);
  // // mutex_.unlock();
  // if (root_locked) {
  //   // LOG_INFO("\033[1;43m%d latch unlock\033[0m", ::gettid());
  //   latch_.unlock();
  //   root_locked = false;
  // }
  RemovePessimistic(key, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemovePessimistic(const KeyType &key, Transaction *transaction) {
  // LOG_INFO("\033[1;32mRemovePessimistic %ld\033[0m", key.ToString());
  bool root_locked = false;
  Page *page = FindLeafPage(key, REMOVE, false, root_locked, transaction);
  LeafPage *leaf_node = CastLeafPage(page);

  if (page == nullptr) {  // is empty
    assert(!root_locked);
    return;
  }

  bool removed = false;
  if (IsSafe(leaf_node, REMOVE)) {
    assert(transaction->GetPageSet()->empty());
    assert(!root_locked);
    removed = leaf_node->Remove(key, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), removed);
    return;
  }

  removed = leaf_node->Remove(key, comparator_);

  if (!removed) {
    UnlockRoot(root_locked);
    ClearPageSet(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return;
  }

  bool flush = RemoveInInternal(page, root_locked, transaction);

  UnlockRoot(root_locked);
  ClearPageSet(transaction);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), flush);

  auto set = transaction->GetDeletedPageSet();
  for (auto page_id : *transaction->GetDeletedPageSet()) {
    bool ok = buffer_pool_manager_->DeletePage(page_id);
    (void)ok;
    assert(ok);
  }
  transaction->GetDeletedPageSet()->clear();
  // buffer_pool_manager_->CheckPinCount();
}

/**
 * @brief
 *
 * @param page
 * @param key
 * @param root_locked
 * @param transaction
 * @return true  the page should flush
 * @return false
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveInInternal(Page *page, bool &root_locked, Transaction *transaction) -> bool {
  BPlusTreePage *node = CastBPlusPage(page);
  bool flush_node = true;

  assert(transaction->GetPageSet()->empty()
             ? true
             : transaction->GetPageSet()->back()->GetPageId() == node->GetParentPageId());

  if (node->IsRootPage()) {
    assert(transaction->GetPageSet()->empty());

    if (node->IsLeafPage()) {
      assert(root_locked);
      if (node->GetSize() == 0) {
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId();
        transaction->AddIntoDeletedPageSet(node->GetPageId());
        flush_node = false;
      }
    } else {
      InternalPage *old_root_node = CastInternalPage(node);
      if (old_root_node->GetSize() == 1) {
        assert(root_locked);
        assert(page->GetPinCount() == 2);

        Page *new_root_page = FetchChildPage(old_root_node, 0);
        BPlusTreePage *new_root_node = CastBPlusPage(new_root_page);
        new_root_node->SetParentPageId(INVALID_PAGE_ID);
        root_page_id_ = new_root_page->GetPageId();
        UpdateRootPageId();
        buffer_pool_manager_->UnpinPage(new_root_node->GetPageId(), true);

        transaction->AddIntoDeletedPageSet(old_root_node->GetPageId());

        if (left_most_ == root_page_id_) {
          left_most_ = right_most_ = root_page_id_;
        }

        flush_node = false;
      }
    }
    return flush_node;
  }

  if (node->GetSize() >= node->GetMinSize()) {
    return true;
  }

  //  N has too few values/pointers
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent_node = CastInternalPage(parent_page);
  bool flush_parent_node = true;
  assert(parent_node->GetSize() >= 2);

  int node_position = parent_node->FindIndex(node->GetPageId());
  if (node_position == 0) {
    Page *right_sibling_page = FetchChildPage(parent_node, 1);
    BPlusTreePage *right_sibling_node = CastBPlusPage(right_sibling_page);
    right_sibling_page->WLatch();
    assert(right_sibling_node->GetSize() >= right_sibling_node->GetMinSize());

    if (right_sibling_node->GetSize() > right_sibling_node->GetMinSize()) {
      transaction->GetPageSet()->pop_back();
      UnlockRoot(root_locked);
      ClearPageSet(transaction);
      transaction->AddIntoPageSet(parent_page);
      BorrowFromRight(node, right_sibling_node, node_position, transaction);
      right_sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), true);
    } else {
      assert(flush_node);
      flush_parent_node = Merge(node, right_sibling_node, node_position, root_locked, transaction);
      transaction->AddIntoPageSet(right_sibling_page);
      transaction->AddIntoDeletedPageSet(right_sibling_page->GetPageId());
    }
  } else {
    Page *left_sibling_page = FetchChildPage(parent_node, node_position - 1);
    BPlusTreePage *left_sibling_node = CastBPlusPage(left_sibling_page);
    left_sibling_page->WLatch();
    assert(left_sibling_node->GetSize() >= left_sibling_node->GetMinSize());

    if (left_sibling_node->GetSize() > left_sibling_node->GetMinSize()) {
      transaction->GetPageSet()->pop_back();
      UnlockRoot(root_locked);
      ClearPageSet(transaction);
      transaction->AddIntoPageSet(parent_page);
      BorrowFromLeft(node, left_sibling_node, node_position - 1, transaction);
    } else {
      flush_parent_node = Merge(left_sibling_node, node, node_position - 1, root_locked, transaction);
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    }
    left_sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), true);
  }
  // LOG_INFO("flush %d? %d", parent_page->GetPageId(), flush_parent_node);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), flush_parent_node);
  return flush_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromLeft(BPlusTreePage *node, BPlusTreePage *left_node, int left_position,
                                    Transaction *transaction) {
  Page *parent_page = transaction->GetPageSet()->back();
  InternalPage *parent_node = CastInternalPage(parent_page);

  if (node->IsLeafPage()) {
    LeafPage *left_leaf_node = CastLeafPage(left_node);
    LeafPage *right_leaf_node = CastLeafPage(node);

    int index = left_leaf_node->GetSize() - 1;
    right_leaf_node->InsertAt(0, left_leaf_node->KeyAt(index),
                              left_leaf_node->ValueAt(index));  // borrow last fram left

    left_leaf_node->IncreaseSize(-1);
    parent_node->SetKeyAt(left_position + 1, right_leaf_node->KeyAt(0));
  } else {
    InternalPage *left_internal_node = CastInternalPage(left_node);
    InternalPage *right_internal_node = CastInternalPage(node);

    right_internal_node->ShiftRight();
    right_internal_node->SetKeyAt(1, parent_node->KeyAt(left_position + 1));  // bring down the key  from parent
    // bring the last child from left
    right_internal_node->SetValueAt(0, left_internal_node->ValueAt(left_internal_node->GetSize() - 1));
    // send up last key of the left to the parent
    parent_node->SetKeyAt(left_position + 1, left_internal_node->KeyAt(left_internal_node->GetSize() - 1));
    left_internal_node->IncreaseSize(-1);
    UpdateChild(right_internal_node, 0, 1);
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
    left_leaf_node->InsertAt(left_leaf_node->GetSize(), right_leaf_node->KeyAt(0), right_leaf_node->ValueAt(0));
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

    UpdateChild(left_internal_node, left_internal_node->GetSize() - 1, left_internal_node->GetSize());
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Merge(BPlusTreePage *left_node, BPlusTreePage *right_node, int posOfLeftPage, bool &root_locked,
                           Transaction *transaction) -> bool {
  Page *parent_page = transaction->GetPageSet()->back();
  transaction->GetPageSet()->pop_back();
  InternalPage *parent_node = CastInternalPage(parent_page);
  bool flush_parent_node = true;

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

  parent_node->ShiftLeft(posOfLeftPage + 1);
  flush_parent_node = RemoveInInternal(parent_page, root_locked, transaction);
  transaction->AddIntoPageSet(parent_page);
  return flush_parent_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateChild(InternalPage *node, int first, int last) {
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
  // root = 2 时可能要被删除！！！
  return node->IsRootPage() ? node->GetSize() > 2 : node->GetSize() > node->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ClearPageSet(Transaction *transaction) {
  auto deque = transaction->GetPageSet();
  while (!deque->empty()) {
    auto page = deque->front();
    deque->pop_front();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockRoot(bool &root_locked) {
  if (root_locked) {
    // LOG_INFO("\033[1;43m%d latch unlock\033[0m\n", ::gettid());
    latch_.unlock();
    root_locked = false;
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
    return {};
  }
  if (left_most_ != root_page_id_) {
    latch_.unlock();
    Page *page = buffer_pool_manager_->FetchPage(left_most_);
    auto node = CastLeafPage(page);
    page->RLatch();
    MappingType value = (*node)[0];
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(left_most_, false);
    return Iterator{left_most_, 0, buffer_pool_manager_, value};
  }
  Page *page = buffer_pool_manager_->FetchPage(left_most_);
  auto node = CastLeafPage(page);
  page->RLatch();
  MappingType value = (*node)[0];
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(left_most_, false);
  latch_.unlock();
  return Iterator{left_most_, 0, buffer_pool_manager_, value};
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  bool root_locked = false;
  Page *page = FindLeafPage(key, FIND, true, root_locked, nullptr);
  assert(!root_locked);
  if (page == nullptr) {
    return {};
  }

  LeafPage *leaf_node = CastLeafPage(page);
  int index = leaf_node->LowerBound(key, comparator_);
  if (index == leaf_node->GetSize()) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return {};
  }
  MappingType value = (*leaf_node)[index];
  page_id_t page_id = page->GetPageId();
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return {page_id, index, buffer_pool_manager_, value};
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return {}; }

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
