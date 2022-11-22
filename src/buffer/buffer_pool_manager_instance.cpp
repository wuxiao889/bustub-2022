//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>
#include <mutex>  // NOLINT

#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/page/page.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard lock(latch_);
  int frame_id = -1;
  Page *page = nullptr;
  // You should pick the replacement frame from either the free list or the replacer (always find from the free list
  // first)
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = pages_ + frame_id;
  } else if (replacer_->Evict(&frame_id)) {
    page = pages_ + frame_id;
    page_table_->Remove(page->GetPageId());
    // If the replacement frame has a dirty page, you should write it back to the disk first.
    // You also need to reset the memory and metadata for the new page.
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->data_);
      ResetPage(page);
    }
    // LOG_DEBUG("evit frame_id %d",frame_id);
  }
  if (page != nullptr) {
    // Create a new page in the buffer pool
    page->page_id_ = AllocatePage();
    // Set page_id to the new page's id
    *page_id = page->page_id_;
    page_table_->Insert(page->page_id_, frame_id);
    // remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
    replacer_->RecordAccess(frame_id);
    // Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
    // so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
    replacer_->SetEvictable(frame_id, false);
    page->pin_count_++;
    // LOG_DEBUG("new frame_id %d page_id %d",frame_id, *page_id);
  }
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard lock(latch_);
  Page *page = nullptr;
  frame_id_t frame_id = -1;
  // First search for page_id in the buffer pool.
  if (page_table_->Find(page_id, frame_id)) {
    page = pages_ + frame_id;
    // disable eviction and record the access history of the frame
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page->pin_count_++;
    return page;
  }
  // If not found, pick a replacement frame from either the free list or the replacer (always find from the free list
  // first
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = pages_ + frame_id;
  } else if (replacer_->Evict(&frame_id)) {
    page = pages_ + frame_id;
    // replace the old page in the frame.
    page_table_->Remove(page->GetPageId());
    if (page->IsDirty()) {
      // if the old page is dirty, you need to write it back to disk and update the metadata of the new page In addition
      disk_manager_->WritePage(page->GetPageId(), page->data_);
      ResetPage(page);
    }
  }
  if (page != nullptr) {
    // read the page from disk by calling disk_manager_->ReadPage()
    disk_manager_->ReadPage(page_id, page->data_);

    page->pin_count_++;
    page->page_id_ = page_id;

    page_table_->Insert(page_id, frame_id);
    // disable eviction and record the access history of the frame
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  }
  // Return nullptr if page_id needs to be fetched from the disk but all frames are currently in use and not evictable
  // (in another word, pinned).
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard lock(latch_);
  frame_id_t frame_id = -1;
  // If page_id is not in the buffer pool
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page *page = pages_ + frame_id;
  // its pin count is already 0, return false.
  if (page->pin_count_ <= 0) {
    return false;
  }
  // is_dirty true if the page should be marked as dirty, false otherwise
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  --page->pin_count_;
  // Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard lock(latch_);
  frame_id_t frame_id = -1;
  // false if the page could not be found in the page table
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page *page = pages_ + frame_id;
  // Use the DiskManager::WritePage() method to flush a page to disk.
  disk_manager_->WritePage(page_id, page->data_);
  // Unset the dirty flag of the page after flushing.
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard lock(latch_);
  // flush all the pages in the buffer pool to disk.
  for (size_t id = 0; id < pool_size_; ++id) {
    Page *page = pages_ + id;
    auto page_id = page->page_id_;
    if (page_id != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page_id, page->data_);
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id = -1;
  // If page_id is not in the buffer pool, do nothing and return true.
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  Page *page = pages_ + frame_id;
  // If the page is pinned and cannot be deleted, return false immediately.
  if (page->pin_count_ > 0) {
    return false;
  }
  // After deleting the page from the page table, stop tracking the frame 
  // in the replacer and add the frame back to the free list.
  std::lock_guard lock(latch_);
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);

  // reset the page's memory and metadata. Finally, you should call DeallocatePage() to imitate freeing the page on the disk.
  ResetPage(page);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
