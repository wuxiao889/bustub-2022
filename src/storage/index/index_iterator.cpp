/**
 * index_iterator.cpp
 */
#include <cassert>
#include <mutex>  // NOLINT

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/index/b_plus_tree_index.h"
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
    : page_id_(page_id), position_(position), buffer_pool_manager_(buffer_pool_manager), value_(std::move(value)) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(const IndexIterator &x)
    : page_id_(x.page_id_), position_(x.position_), buffer_pool_manager_(x.buffer_pool_manager_), value_(x.value_) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return *this == IndexIterator{}; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  assert(page_id_ != INVALID_PAGE_ID);
  return value_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto page = buffer_pool_manager_->FetchPage(page_id_);

  page->RLatch();
  auto node = reinterpret_cast<LeafPage *>(page->GetData());

  if (++position_ != node->GetSize()) {
    value_ = node->GetKeyValue(position_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id_, false);
    return *this;
  }

  if (node->GetNextPageId() == INVALID_PAGE_ID && position_ == node->GetSize()) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    *this = {};
    return *this;
  }

  Page *next_page = buffer_pool_manager_->FetchPage(node->GetNextPageId());
  assert(next_page);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

  page = next_page;
  page->RLatch();
  page_id_ = page->GetPageId();
  position_ = 0;
  node = reinterpret_cast<LeafPage *>(page->GetData());
  value_ = node->GetKeyValue(position_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_id_, false);

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
