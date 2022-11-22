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
#define LOG_LEVEL 1000
#include "buffer/lru_k_replacer.h"
#include <bits/types/time_t.h>
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard lock(latch_);
  return EvitFromSet(inf_set_, frame_id) ? true : EvitFromSet(kth_set_, frame_id);
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard lock(latch_);
  current_timestamp_ = GetTimeStamp();
  if (frame_cnt_map_.count(frame_id) == 0 && cur_size_ == replacer_size_) {
    // BUSTUB_ASSERT("RecordAccess invalid %d", frame_id);
    return;
  }

  size_t use_cnt = ++frame_cnt_map_[frame_id];
  if (use_cnt == 1) {
    evitable_[frame_id] = true;
    cur_size_++;
    InsertToSet(inf_set_, frame_id);
  }
  if (use_cnt == k_) {
    RemoveFromSet(inf_set_, frame_id);
    InsertToSet(kth_set_, frame_id);
    LOG_DEBUG("record %d into kth_set_", frame_id);
  } else if (use_cnt > k_) {
    RemoveFromSet(kth_set_, frame_id);
    InsertToSet(kth_set_, frame_id);
  }
  LOG_DEBUG("record %d access_cnt %lu curr_size %zu", frame_id, frame_cnt_map_[frame_id], cur_size_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard lock(latch_);
  // BUSTUB_ASSERT(frame_time_map_.count(frame_id) != 0 , "frame_it invalid");
  if (frame_time_map_.count(frame_id) == 0 || evitable_[frame_id] == set_evictable) {
    return;
  }
  if (!evitable_[frame_id] && set_evictable) {
    replacer_size_++;
    cur_size_++;
    LOG_DEBUG("SetEvictable %d  true curr_size %lu", frame_id, cur_size_);
  } else if (evitable_[frame_id] && !set_evictable) {
    replacer_size_--;
    cur_size_--;
    LOG_DEBUG("SetEvictable %d  false curr_size %lu", frame_id, cur_size_);
  }
  evitable_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard lock(latch_);
  if (frame_time_map_.count(frame_id) == 0) {
    return;
  }
  if (!evitable_[frame_id]) {
    LOG_DEBUG("Remove %d  unevitable", frame_id);
    return;
  }
  auto cnt = frame_cnt_map_[frame_id];
  if (cnt >= k_) {
    RemoveFromSet(kth_set_, frame_id);
    LOG_DEBUG("Remove %d from kth_set_ curr_size %lu", frame_id, cur_size_ - 1);
  } else {
    RemoveFromSet(inf_set_, frame_id);
    LOG_DEBUG("Remove %d from inf_set_ curr_size %lu", frame_id, cur_size_ - 1);
  }
  RemoveAll(frame_id);
  cur_size_--;
}

// returns the number of evictable frames that are currently in the LRUKReplacer.
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard lock(latch_);
  return this->cur_size_;
}

auto LRUKReplacer::RemoveFromSet(std::set<Node> &set_, frame_id_t frame_id) -> void {
  auto old_time = frame_time_map_[frame_id];
  set_.erase(Node{frame_id, old_time});
}

auto LRUKReplacer::InsertToSet(std::set<Node> &set_, frame_id_t frame_id) -> void {
  Node node{frame_id, static_cast<time_t>(current_timestamp_)};
  frame_time_map_[frame_id] = node.time_;
  set_.insert(node);
}

auto LRUKReplacer::EvitFromSet(std::set<Node> &set_, frame_id_t *frame_id) -> bool {
  for (auto it = set_.begin(); it != set_.end(); ++it) {
    auto id = it->id_;
    if (evitable_[id]) {
      set_.erase(it);
      RemoveAll(id);
      *frame_id = id;
      cur_size_--;
      LOG_DEBUG("evit %d curr_size %zu", id, cur_size_);
      return true;
    }
  }
  return false;
}

auto LRUKReplacer::RemoveAll(frame_id_t frame_id) -> void {
  evitable_.erase(frame_id);
  frame_time_map_.erase(frame_id);
  frame_cnt_map_.erase(frame_id);
}

auto LRUKReplacer::GetTimeStamp() -> int64_t { return std::chrono::system_clock::now().time_since_epoch().count(); }

}  // namespace bustub
