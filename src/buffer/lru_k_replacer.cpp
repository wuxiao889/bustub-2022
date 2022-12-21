//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

// list通过反向迭代器删除节点时需要 (++iterator).base()
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> guard_lock(latch_);
  for (auto it = history_list_.begin(); it != history_list_.end(); it++) {
    if (key_evictable_[*it]) {
      *frame_id = *it;
      key_evictable_.erase(*it);
      history_map_.erase(*it);
      key_acscnt_.erase(*it);
      history_list_.erase(it);
      --evict_cnt_;
      return true;
    }
  }
  for (auto it = cache_list_.rbegin(); it != cache_list_.rend(); it++) {
    if (key_evictable_[*it]) {
      *frame_id = *it;
      key_evictable_.erase(*it);
      cache_map_.erase(*it);
      key_acscnt_.erase(*it);
      cache_list_.erase((++it).base());
      --evict_cnt_;
      return true;
    }
  }
  return false;
}

// Create a new entry for access history if frame id has not been seen before.
// If frame id is invalid (ie. larger than replacer_size_), throw an exception
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> guard_lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  if (key_evictable_.find(frame_id) == key_evictable_.end()) {
    history_list_.push_back(frame_id);
    history_map_.insert(std::make_pair(frame_id, (++history_list_.rbegin()).base()));
    key_acscnt_.insert(std::make_pair(frame_id, 1));
    key_evictable_.insert(std::make_pair(frame_id, false));
  } else {
    if (history_map_.find(frame_id) != history_map_.end()) {
      auto iter = history_map_[frame_id];
      if (++key_acscnt_[frame_id] == k_) {
        cache_list_.splice(cache_list_.begin(), history_list_, iter);
        history_map_.erase(frame_id);
        cache_map_.insert(std::make_pair(frame_id, iter));
      }
    } else {
      cache_list_.splice(cache_list_.begin(), cache_list_, cache_map_[frame_id]);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> guard_lock(latch_);
  if (key_evictable_.find(frame_id) != key_evictable_.end()) {
    if (key_evictable_[frame_id] != set_evictable) {
      key_evictable_[frame_id] = set_evictable;
      evict_cnt_ += ((set_evictable) ? 1 : -1);
    }
  } else if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  } else {
    return;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> guard_lock(latch_);
  if (key_evictable_.find(frame_id) == key_evictable_.end()) {
    return;
  }
  if (!key_evictable_[frame_id]) {
    throw std::exception();
  }
  if (history_map_.find(frame_id) != history_map_.end()) {
    history_list_.erase(history_map_[frame_id]);
    history_map_.erase(frame_id);
  } else if (cache_map_.find(frame_id) != cache_map_.end()) {
    cache_list_.erase(cache_map_[frame_id]);
    cache_map_.erase(frame_id);
  }
  key_acscnt_.erase(frame_id);
  --evict_cnt_;
  key_evictable_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> guard_lock(latch_);
  return evict_cnt_;
}

}  // namespace bustub
