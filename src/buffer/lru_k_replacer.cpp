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
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard lock(latch_);
  return EvitFromSet(inf_set_, frame_id) ? true : EvitFromSet(kth_set_, frame_id);
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard lock(latch_);
  if (frame_cnt_map_.count(frame_id) == 0 && curr_size_ == replacer_size_) {
    BUSTUB_ASSERT("RecordAccess invalid %d", frame_id);
  } else {
    size_t use_cnt = frame_cnt_map_[frame_id];
    if (use_cnt == 0) {
      evit_map_[frame_id] = true;
      curr_size_++;
    }
    if (use_cnt + 1 < k_) {
      InsertToSet(inf_set_, frame_id);
    } else if (use_cnt + 1 == k_) {
      RemoveFromSet(inf_set_, frame_id);
      InsertToSet(kth_set_, frame_id);
      LOG_INFO("record %d into kth_set_", frame_id);
    } else {
      InsertToSet(kth_set_, frame_id);
    }
    LOG_INFO("record %d access_cnt %lu curr_size %zu", frame_id, frame_cnt_map_[frame_id], curr_size_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard lock(latch_);
  if (frame_time_map_.count(frame_id) == 0) {
    LOG_DEBUG("SetEvictable %d  not foune", frame_id);
    BUSTUB_ASSERT(false, 1);
  }
  if (!evit_map_[frame_id] && set_evictable) {
    replacer_size_++;
    curr_size_++;
    LOG_INFO("SetEvictable %d  true curr_size %lu", frame_id, curr_size_);
  } else if (evit_map_[frame_id] && !set_evictable) {
    replacer_size_--;
    curr_size_--;
    LOG_INFO("SetEvictable %d  false curr_size %lu", frame_id, curr_size_);
  }
  evit_map_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard lock(latch_);
  if (frame_time_map_.count(frame_id) == 0) {
    return;
  }
  if (!evit_map_[frame_id]) {
    LOG_DEBUG("Remove %d  unevitable", frame_id);
    BUSTUB_ASSERT(false, "unevitable");
  }
  auto cnt = frame_cnt_map_[frame_id];
  if (cnt >= k_) {
    RemoveFromSet(kth_set_, frame_id);
    LOG_INFO("Remove %d from kth_set_ curr_size %lu", frame_id, curr_size_ - 1);
  } else {
    RemoveFromSet(inf_set_, frame_id);
    LOG_INFO("Remove %d from inf_set_ curr_size %lu", frame_id, curr_size_ - 1);
  }
  RemoveAll(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard lock(latch_);
  return this->curr_size_;
}

auto LRUKReplacer::RemoveFromSet(std::set<Node> &set_, frame_id_t frame_id) -> void {
  auto old_time = frame_time_map_[frame_id];
  set_.erase(Node{frame_id, old_time});
}

auto LRUKReplacer::InsertToSet(std::set<Node> &set_, frame_id_t frame_id) -> void {
  auto old_time = frame_time_map_[frame_id];
  set_.erase(Node{frame_id, old_time});
  Node node{frame_id, GetTimeStamp()};
  frame_cnt_map_[frame_id]++;
  frame_time_map_[frame_id] = node.time_;
  set_.insert(node);
}

auto LRUKReplacer::EvitFromSet(std::set<Node> &set_, frame_id_t *frame_id) -> bool {
  for (auto it = set_.begin(); it != set_.end(); ++it) {
    auto id = it->id_;
    if (evit_map_[id]) {
      RemoveFromSet(set_, id);
      RemoveAll(id);
      *frame_id = id;
      curr_size_--;
      LOG_INFO("evit %d curr_size %zu", id, curr_size_);
      return true;
    }
  }
  return false;
}

auto LRUKReplacer::RemoveAll(frame_id_t frame_id) -> void {
  evit_map_.erase(frame_id);
  frame_time_map_.erase(frame_id);
  frame_cnt_map_.erase(frame_id);
}

auto LRUKReplacer::GetTimeStamp() -> int64_t { return std::chrono::system_clock::now().time_since_epoch().count(); }

}  // namespace bustub
