/**
 * index_iterator.cpp
 */
#include <cassert>
#include <mutex>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int position, BufferPoolManager *buffer_pool_manager,
                                  MappingType value)
    : page_id_(page_id), position_(position), buffer_pool_manager_(buffer_pool_manager), value_(value) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(const IndexIterator &x)
    : page_id_(x.page_id_), position_(x.position_), buffer_pool_manager_(x.buffer_pool_manager_), value_(x.value_) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  assert(cur_page_);
  auto node = reinterpret_cast<LeafPage *>(cur_page_->GetData());
  return node->GetNextPageId() == INVALID_PAGE_ID && position_ == node->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  assert(page_id_ != INVALID_PAGE_ID);
  return value_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  cur_page_ = buffer_pool_manager_->FetchPage(page_id_);
  assert(cur_page_ != nullptr);

  cur_page_->RLatch();
  auto node = reinterpret_cast<LeafPage *>(cur_page_->GetData());

  if (++position_ != node->GetSize()) {
    value_ = (*node)[position_];

    cur_page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id_, false);
    cur_page_ = nullptr;
    return *this;
  }

  if (IsEnd()) {
    cur_page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page_->GetPageId(), false);
    *this = {};
    return *this;
  }

  Page *next_page = buffer_pool_manager_->FetchPage(node->GetNextPageId());
  cur_page_->RUnlatch();
  buffer_pool_manager_->UnpinPage(cur_page_->GetPageId(), false);

  cur_page_ = next_page;

  cur_page_->RLatch();
  page_id_ = cur_page_->GetPageId();
  position_ = 0;
  node = reinterpret_cast<LeafPage *>(cur_page_->GetData());
  value_ = (*node)[position_];
  cur_page_->RUnlatch();

  cur_page_ = nullptr;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
