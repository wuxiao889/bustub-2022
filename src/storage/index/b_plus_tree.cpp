#include <cassert>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>
#include <string>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {
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
  if (IsEmpty()) {
    return false;
  }
  BPlusTreePage *root_page = FetchPage(root_page_id_);
  auto [leaf_page, index] = LoopUp(key, root_page);
  if (leaf_page != nullptr) {
    result->emplace_back(leaf_page->ValueAt(index));
    UnPinPage(leaf_page, false);
    buffer_pool_manager_->CheckPinCount();
    return true;
  }
  buffer_pool_manager_->CheckPinCount();
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LoopUp(const KeyType &key, BPlusTreePage *cur_page) -> std::pair<LeafPage *, int> {
  for (;;) {
    if (cur_page->IsLeafPage()) {
      LeafPage *leaf_page = CastLeafPage(cur_page);
      int index = leaf_page->LowerBound(key, comparator_);
      LOG_INFO("lookup int leaf page %d at index %d", cur_page->GetPageId(), index);
      assert(index <= leaf_page->GetSize());
      if (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
        LOG_INFO("found");
        return std::make_pair(leaf_page, index);
      }
      LOG_INFO("not found");
      break;
    }
    InternalPage *internal_page = CastInternalPage(cur_page);
    int idx = internal_page->UpperBound(key, comparator_);
    cur_page = FetchChild(internal_page, idx - 1);
    // int idx = page->LowerBound(key, comparator_);
    // if (idx < page->GetSize() && comparator_(page->KeyAt(idx), key) == 0) {
    //   cur_page = FetchChild(page, idx);
    // } else {
    //   cur_page = FetchChild(page, idx - 1);
    // }
    UnPinPage(cur_page, false);
    LOG_INFO("lookup to page %d at index %d", cur_page->GetPageId(), idx - 1);
  }
  UnPinPage(cur_page, false);
  return std::make_pair(nullptr, -1);
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
  BPlusTreePage *root_page;
  if (IsEmpty()) {
    root_page = NewLeafRootPage(&root_page_id_);
    UpdateRootPageId(1);
  } else {
    root_page = FetchPage(root_page_id_);
  }
  bool ok = InsertAux(root_page, key, value, transaction);
  buffer_pool_manager_->CheckPinCount();
  return ok;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertAux(BPlusTreePage *page, const KeyType &key, const ValueType &value,
                               Transaction *transaction) -> bool {
  if (page->IsLeafPage()) {
    LeafPage *leaf_page = CastLeafPage(page);
    int index = leaf_page->LowerBound(key, comparator_);
    if (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
      LOG_INFO("key ununique");
      UnPinPage(leaf_page, false);
      return false;
    }
    leaf_page->Insert(index, key, value);
    LOG_INFO("insert to leaf page %d at index %d", leaf_page->GetPageId(), index);
    if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
      SplitLeaf(leaf_page);
    }
    UnPinPage(leaf_page, false);
    return true;
  }
  InternalPage *inter_page = CastInternalPage(page);
  int index = inter_page->LowerBound(key, comparator_);
  BPlusTreePage *next_page = FetchChild(inter_page, index - 1);

  LOG_INFO("index %d insert go to page %d", index, next_page->GetPageId());
  bool ok = InsertAux(next_page, key, value, transaction);
  if (ok && inter_page->GetSize() == inter_page->GetMaxSize() + 1) {
    SplitInternal(inter_page);
  }
  UnPinPage(inter_page, false);
  return ok;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitLeaf(LeafPage *left_page) {
  int x;
  x = leaf_max_size_ / 2;

  int right_page_id;
  LeafPage *right_page = NewLeafPage(&right_page_id, left_page->GetParentPageId());

  for (int i = x, j = 0; i < leaf_max_size_; i++, j++) {
    right_page->SetKeyAt(j, left_page->KeyAt(i));
    right_page->SetValueAt(j, left_page->ValueAt(i));
    left_page->SetKeyAt(i, KeyType{});
    left_page->SetValueAt(i, ValueType{});
  }

  left_page->SetSize(x);
  right_page->SetSize(leaf_max_size_ - x);
  assert(right_page->GetSize() == leaf_max_size_ - x);

  if (right_most_ == left_page->GetPageId()) {
    right_most_ = right_page_id;
  }

  KeyType key = right_page->KeyAt(0);

  if (left_page->IsRootPage()) {
    LOG_INFO("leaf page %d split noroot", left_page->GetPageId());
    InternalPage *root_page = NewInternalRootPage(&root_page_id_);
    UpdateRootPageId(0);

    root_page->Insert(0, KeyType{}, left_page->GetPageId());
    root_page->Insert(1, key, right_page_id);

    assert(root_page->GetSize() == 2);

    left_page->SetParentPageId(root_page_id_);
    right_page->SetParentPageId(root_page_id_);

    left_page->SetNextPageId(right_page_id);

    UnPinPage(root_page, true);

  } else {
    InternalPage *parent_page = FetchParent(left_page);
    assert(parent_page->GetSize() <= parent_page->GetMaxSize());
    int index = parent_page->LowerBound(key, comparator_);
    parent_page->Insert(index, key, right_page_id);
    LOG_INFO("leaf page %d split parent_page id %d at index %d ", left_page->GetPageId(), parent_page->GetPageId(),
             index);
    right_page->SetNextPageId(left_page->GetNextPageId());
    left_page->SetNextPageId(right_page_id);
    UnPinPage(parent_page, true);
  }
  UnPinPage(left_page, true);
  UnPinPage(right_page, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInternal(InternalPage *left_page) {
  LOG_INFO("internal page %d split", left_page->GetPageId());
  int x;
  x = (leaf_max_size_ + 1) / 2;
  int right_page_id;
  InternalPage *right_page = NewInternalPage(&right_page_id, left_page->GetParentPageId());

  for (int i = x, j = 0; i <= internal_max_size_; i++, j++) {
    assert(left_page->ValueAt(i) != INVALID_PAGE_ID);
    right_page->SetKeyAt(j, left_page->KeyAt(i));
    right_page->SetValueAt(j, left_page->ValueAt(i));
    left_page->SetValueAt(i, INVALID_PAGE_ID);
  }
  left_page->SetSize(x);
  right_page->SetSize(internal_max_size_ - x + 1);
  for (int i = 0; i < right_page->GetSize(); i++) {
    BPlusTreePage *child_page = FetchChild(right_page, i);
    child_page->SetParentPageId(right_page_id);
    UnPinPage(child_page, true);
  }

  KeyType key = right_page->KeyAt(0);
  right_page->SetKeyAt(0, KeyType{});

  if (left_page->IsRootPage()) {
    InternalPage *root_page = NewInternalRootPage(&root_page_id_);
    UpdateRootPageId(0);
    left_page->SetParentPageId(root_page_id_);
    right_page->SetParentPageId(root_page_id_);
    root_page->Insert(0, KeyType{}, left_page->GetPageId());
    root_page->Insert(1, key, right_page_id);
    assert(root_page->GetSize() == 2);
    UnPinPage(root_page, true);
  } else {
    InternalPage *parent_page = FetchParent(left_page);
    int index = parent_page->LowerBound(key, comparator_);
    parent_page->Insert(index, key, right_page_id);
    UnPinPage(parent_page, true);
  }
  UnPinPage(left_page, true);
  UnPinPage(right_page, true);
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
  static int step = 0;
  if (IsEmpty()) {
    return;
  }
  auto root_page = FetchPage(root_page_id_);
  RemoveInPage(root_page, key, transaction);
  char buf[50];
  std::sprintf(buf, "step%d_remove%ld.dot", ++step, key.ToString());
  Draw(buffer_pool_manager_, buf);
  buffer_pool_manager_->CheckPinCount();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveInPage(BPlusTreePage *curr_page, const KeyType &key, Transaction *transaction) {
  if (curr_page->IsLeafPage()) {
    LeafPage *leaf_page = CastLeafPage(curr_page);
    int index = leaf_page->LowerBound(key, comparator_);
    if (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
      LOG_INFO("in child page %d index %d found", leaf_page->GetPageId(), index);
      leaf_page->ShiftLeft(index);
      leaf_page->IncreaseSize(-1);
    }
  } else {
    InternalPage *internal_page = CastInternalPage(curr_page);
    int index = internal_page->UpperBound(key, comparator_);
    BPlusTreePage *next_page = FetchChild(internal_page, index - 1);
    LOG_INFO("go to child page %d index %d", next_page->GetPageId(), index - 1);
    RemoveInPage(next_page, key, transaction);
  }

  if (curr_page->GetSize() >= curr_page->GetMinSize()) {
    UnPinPage(curr_page, false);
    return;
  }

  if (curr_page->IsRootPage()) {
    if (curr_page->IsLeafPage()) {
      LOG_INFO("is leaf root page");
      UnPinPage(curr_page, false);
      return;
    }
    if (curr_page->GetSize() == 1) {
      LOG_INFO("is internal root page, and only has one child");
      InternalPage *root_page = CastInternalPage(curr_page);
      root_page_id_ = root_page->ValueAt(0);
      UpdateRootPageId();
      UpdataRoot();
      UnPinPage(root_page, false);
      UnPinPage(curr_page, false);
      int ok = DeletePage(root_page);
      assert(ok == true);
      return;
    }
  }

  int cur_pos = CalcPositionInParent(curr_page);
  LOG_INFO("cur_page %d pos %d in parent", curr_page->GetPageId(), cur_pos);

  if (curr_page->IsLeafPage()) {
    if (cur_pos == 0) {
      // LeafPage* right_page = FetchChild(InternalPage *page, int index)
      LeafPage *right_page = CastLeafPage(FetchSibling(curr_page, cur_pos + 1));
      assert(right_page != nullptr);
      assert(right_page->GetSize() >= right_page->GetMinSize());
      if (right_page->GetSize() > right_page->GetMinSize()) {
        Redistribute(curr_page, right_page, 0, 0);
      } else {
        Merge(curr_page, right_page, cur_pos);
      }
      UnPinPage(right_page, false);

    } else {
      LeafPage *left_page = CastLeafPage(FetchSibling(curr_page, cur_pos - 1));
      LeafPage *right_page = CastLeafPage(FetchSibling(curr_page, cur_pos + 1));
      assert(left_page->GetSize() >= left_page->GetMinSize());
      // assert(right_page->GetSize() >= right_page->GetMinSize());

      if (left_page->GetSize() > left_page->GetMinSize()) {
        Redistribute(left_page, curr_page, cur_pos - 1, 1);
      } else if (right_page != nullptr && right_page->GetSize() > right_page->GetMinSize()) {
        Redistribute(curr_page, right_page, cur_pos, 0);
      } else {
        Merge(left_page, curr_page, cur_pos - 1);
      }
      UnPinPage(left_page, false);
      if (right_page != nullptr) {
        UnPinPage(right_page, false);
      }
      // else if (cur_page->GetSize() + left_page->GetSize() < cur_page->GetMaxSize()) {
      //   Merge();
      // } else if (right_page != nullptr && cur_page->GetSize() + right_page->GetSize() < cur_page->GetMaxSize()) {
      //   Merge();
      // }
    }
  } else {
    if (cur_pos == 0) {
      InternalPage *right_page = CastInternalPage(FetchSibling(curr_page, cur_pos + 1));
      assert(right_page != nullptr);
      assert(right_page->GetSize() >= right_page->GetMinSize());

      if (right_page->GetSize() > right_page->GetMinSize()) {
        Redistribute(curr_page, right_page, 0, 0);
      } else {
        Merge(curr_page, right_page, cur_pos);
      }
      UnPinPage(right_page, false);

    } else {
      InternalPage *left_page = CastInternalPage(FetchSibling(curr_page, cur_pos - 1));
      InternalPage *right_page = CastInternalPage(FetchSibling(curr_page, cur_pos + 1));
      assert(left_page->GetSize() >= left_page->GetMinSize());
      // assert(right_page->GetSize() >= right_page->GetMinSize());
      if (left_page->GetSize() > left_page->GetMinSize()) {
        Redistribute(left_page, curr_page, cur_pos - 1, 0);
      } else if (right_page != nullptr && right_page->GetSize() > right_page->GetMinSize()) {
        Redistribute(curr_page, right_page, cur_pos, 0);
      } else {
        Merge(left_page, curr_page, cur_pos - 1);
      }
      UnPinPage(left_page, false);
      UnPinPage(right_page, false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(BPlusTreePage *left, BPlusTreePage *right, int posOfLeftPage,
                                  int whichOneisCurBlock) {
  LOG_INFO("Redistribute page %d and page %d posofleft %d  %d is cur", left->GetPageId(), right->GetPageId(),
           posOfLeftPage, whichOneisCurBlock);
  InternalPage *parent = FetchParent(left);
  if (left->IsLeafPage()) {
    LeafPage *leftpage = CastLeafPage(left);
    LeafPage *rightpage = CastLeafPage(right);
    // brow from right
    if (whichOneisCurBlock == 0) {
      // borrow the first pair from right
      leftpage->SetKeyAt(leftpage->GetSize(), rightpage->KeyAt(0));
      leftpage->SetValueAt(leftpage->GetSize(), rightpage->ValueAt(0));
      leftpage->IncreaseSize(1);
      // shift left by one in right
      rightpage->ShiftLeft();
      rightpage->IncreaseSize(-1);
      // updade right key in parent
      parent->SetKeyAt(posOfLeftPage + 1, rightpage->KeyAt(0));
    } else {
      // shift right to make space
      rightpage->ShiftRight();
      // borrow last fram left
      rightpage->SetKeyAt(0, leftpage->KeyAt(leftpage->GetSize() - 1));
      rightpage->SetValueAt(0, leftpage->ValueAt(leftpage->GetSize() - 1));
      rightpage->IncreaseSize(1);
      leftpage->IncreaseSize(-1);
      parent->SetKeyAt(posOfLeftPage + 1, rightpage->KeyAt(0));
    }
  } else {
    InternalPage *leftpage = CastInternalPage(left);
    InternalPage *rightpage = CastInternalPage(right);

    if (whichOneisCurBlock == 0) {
      // bring down the right key from parent
      leftpage->SetKeyAt(leftpage->GetSize(), parent->KeyAt(posOfLeftPage + 1));
      // bring the first child from right
      leftpage->SetValueAt(leftpage->GetSize(), rightpage->ValueAt(0));
      leftpage->IncreaseSize(1);
      // send up first key of the right to the parent
      parent->SetKeyAt(posOfLeftPage + 1, rightpage->KeyAt(0));
      // shift left by one in right
      rightpage->ShiftLeft();
      rightpage->IncreaseSize(-1);
    } else {
      rightpage->ShiftRight();
      // bring down the left key from parent
      rightpage->SetKeyAt(0, parent->KeyAt(posOfLeftPage));
      // bring the last child from left
      rightpage->SetValueAt(0, leftpage->ValueAt(leftpage->GetSize() - 1));
      rightpage->IncreaseSize(1);

      // send up last key of the left to the parent
      parent->SetKeyAt(posOfLeftPage, leftpage->KeyAt(leftpage->GetSize() - 1));
      leftpage->IncreaseSize(-1);
    }
  }
  UnPinPage(parent, true);
  UnPinPage(left, true);
  UnPinPage(right, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(BPlusTreePage *left, BPlusTreePage *right, int posOfLeftPage) {
  LOG_INFO("Merge page %d %d pos_of_left %d", left->GetPageId(), right->GetPageId(), posOfLeftPage);
  InternalPage *parent = FetchParent(left);
  if (left->IsLeafPage()) {
    LeafPage *left_page = CastLeafPage(left);
    LeafPage *right_page = CastLeafPage(right);

    for (int i = left_page->GetSize(), j = 0; j < right_page->GetSize(); i++, j++) {
      left_page->SetValueAt(i, right_page->ValueAt(j));
      left_page->SetKeyAt(i, right_page->KeyAt(j));
    }
    left_page->IncreaseSize(right_page->GetSize());

    left_page->SetNextPageId(right_page->GetNextPageId());

    if (right_page->GetPageId() == right_most_) {
      right_most_ = left_page->GetPageId();
    }

    UnPinPage(right_page, false);
    bool ok = DeletePage(right_page);
    assert(ok == true);

  } else {
    InternalPage *left_page = CastInternalPage(left);
    InternalPage *right_page = CastInternalPage(right);
    int old_size = left_page->GetSize();

    left_page->SetKeyAt(left_page->GetSize(), parent->KeyAt(posOfLeftPage + 1));
    left_page->SetValueAt(left_page->GetSize(), right_page->ValueAt(0));
    left_page->IncreaseSize(1);

    for (int i = left_page->GetSize(), j = 1; j < right_page->GetSize(); i++, j++) {
      left_page->SetValueAt(i, right_page->ValueAt(j));
      left_page->SetKeyAt(i, right_page->KeyAt(j));
    }
    left_page->IncreaseSize(right_page->GetSize() - 1);

    for (int i = old_size; i < left_page->GetSize(); i++) {
      BPlusTreePage *child_page = FetchChild(left_page, i);
      child_page->SetParentPageId(left_page->GetPageId());
      UnPinPage(child_page, true);
    }

    UnPinPage(right_page, false);
    bool ok = DeletePage(right_page);
    assert(ok == true);
  }

  parent->ShiftLeft(posOfLeftPage + 1);
  parent->IncreaseSize(-1);
  UnPinPage(left, true);
  UnPinPage(parent, true);
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
  LeafPage *page = CastLeafPage(FetchPage(left_most_));
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
    return Iterator{};
  }
  auto [page, pos] = LoopUp(key, FetchPage(root_page_id_));
  if (page == nullptr) {
    return Iterator{};
  }
  return Iterator{page, pos, buffer_pool_manager_};
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  LeafPage *page = CastLeafPage(FetchPage(right_most_));
  return Iterator{page, page->GetSize(), buffer_pool_manager_};
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
auto BPLUSTREE_TYPE::NewLeafPage(page_id_t *page_id, page_id_t parent_id) -> LeafPage * {
  LeafPage *p = CastLeafPage(PageToB(buffer_pool_manager_->NewPage(page_id)));
  p->Init(*page_id, parent_id, leaf_max_size_);
  return p;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewLeafRootPage(page_id_t *page_id) -> LeafPage * {
  LeafPage *p = CastLeafPage(PageToB(buffer_pool_manager_->NewPage(page_id)));
  p->Init(*page_id, INVALID_PAGE_ID, leaf_max_size_);
  left_most_ = *page_id;
  right_most_ = *page_id;
  return p;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewInternalPage(page_id_t *page_id, page_id_t parent_id) -> InternalPage * {
  InternalPage *p = CastInternalPage(PageToB(buffer_pool_manager_->NewPage(page_id)));
  p->Init(*page_id, parent_id, internal_max_size_);
  return p;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewInternalRootPage(page_id_t *page_id) -> InternalPage * {
  InternalPage *p = CastInternalPage(PageToB(buffer_pool_manager_->NewPage(page_id)));
  p->Init(*page_id, INVALID_PAGE_ID, internal_max_size_);
  return p;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchPage(page_id_t page_id) -> BPlusTreePage * {
  return PageToB(buffer_pool_manager_->FetchPage(page_id));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchParent(BPlusTreePage *page) -> InternalPage * {
  return CastInternalPage(PageToB(buffer_pool_manager_->FetchPage(page->GetParentPageId())));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchSibling(BPlusTreePage *page, int index) -> BPlusTreePage * {
  InternalPage *parent_page = FetchParent(page);
  assert(index >= 0 && index <= parent_page->GetSize());
  if (index == parent_page->GetSize()) {
    UnPinPage(parent_page, false);
    return nullptr;
  }
  page_id_t page_id = parent_page->ValueAt(index);
  UnPinPage(parent_page, false);
  return FetchPage(page_id);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchChild(InternalPage *page, int index) -> BPlusTreePage * {
  page_id_t page_id = page->ValueAt(index);
  BPlusTreePage *res = FetchPage(page_id);
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UnPinPage(BPlusTreePage *page, bool is_dirty) const {
  buffer_pool_manager_->UnpinPage(page->GetPageId(), is_dirty);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeletePage(BPlusTreePage *page) const -> bool {
  return buffer_pool_manager_->DeletePage(page->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UpdataRoot() {
  BPlusTreePage *page = FetchPage(root_page_id_);
  page->SetParentPageId(INVALID_PAGE_ID);
  UnPinPage(page, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CalcPositionInParent(BPlusTreePage *page) -> int {
  InternalPage *parent_page = FetchParent(page);
  int position = -1;
  if (page->IsLeafPage()) {
    position = parent_page->UpperBound(CastLeafPage(page)->KeyAt(0), comparator_);
  } else {
    position = parent_page->UpperBound(CastInternalPage(page)->KeyAt(0), comparator_);
  }
  assert(position <= parent_page->GetSize());
  UnPinPage(parent_page, false);
  return position - 1;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
