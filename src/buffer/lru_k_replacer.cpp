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
#include <bits/types/struct_timespec.h>
#include <cstddef>
#include <ctime>
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard lock(latch_);
  bool evicted = EvitFromSet(inf_set_, frame_id) ? true : EvitFromSet(kth_set_, frame_id);
  if (evicted) {
    RemoveRecord(*frame_id);
    // LOG_DEBUG("evite %d cur_size %ld replacer_size %ld", *frame_id, cur_size_, replacer_size_);
  }
  return evicted;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard lock(latch_);
  current_timestamp_ = GetTimeStamp();
  // BUSTUB_ASSERT(frame_cnt_map_.count(frame_id) == 0, "RecordAccess invalid %d");
  if (frame_cnt_.count(frame_id) == 0 && replacer_size_ == 0) {
    // BUSTUB_ASSERT("RecordAccess invalid %d", frame_id);
    return;
  }
  size_t &frame_cnt = frame_cnt_[frame_id];
  if (frame_cnt == 0) {
    evitable_[frame_id] = false;
    frame_time_[frame_id] = current_timestamp_;
    replacer_size_--;
  }
  if (!evitable_[frame_id]) {
    // frame_time_[frame_id] = current_timestamp_;
    if (++frame_cnt >= k_) {
      frame_time_[frame_id] = current_timestamp_;
    }
    // LOG_DEBUG("record unevitable %d cnt %ld cur_size %ld replacer_size %ld", frame_id, frame_cnt_[frame_id],
    // cur_size_, replacer_size_);
    return;
  }

  // evitable
  if (frame_cnt < k_ - 1) {
    frame_cnt++;
  } else {
    RemoveFromSet(frame_id);
    frame_time_[frame_id] = current_timestamp_;
    frame_cnt++;
    InsertToSet(frame_id);
  }
  // LOG_DEBUG("record evitable %d cnt %ld cur_size %ld replacer_size %ld", frame_id, frame_cnt_[frame_id],
  // cur_size_,replacer_size_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard lock(latch_);
  if (frame_time_.count(frame_id) == 0 || evitable_[frame_id] == set_evictable) {
    // LOG_DEBUG("set xevitable %d cur_size %ld replacer_size %ld", frame_id, cur_size_, replacer_size_);
    return;
  }
  evitable_[frame_id] = set_evictable;
  if (set_evictable) {
    InsertToSet(frame_id);
    cur_size_++;
    // LOG_DEBUG("set evitable %d cur_size %ld replacer_size %ld", frame_id, cur_size_, replacer_size_);
  } else {
    RemoveFromSet(frame_id);
    cur_size_--;
    // LOG_DEBUG("set unevitable %d cur_size %ld replacer_size %ld", frame_id, cur_size_, replacer_size_);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard lock(latch_);
  if (frame_time_.count(frame_id) == 0) {
    return;
  }
  BUSTUB_ASSERT(evitable_[frame_id], "unremovalbe!");
  RemoveFromSet(frame_id);
  RemoveRecord(frame_id);
}

// returns the number of evictable frames that are currently in the LRUKReplacer.
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard lock(latch_);
  return this->cur_size_;
}

auto LRUKReplacer::RemoveFromSet(frame_id_t frame_id) -> void {
  assert(frame_time_.count(frame_id) != 0);
  Node node{frame_id, frame_time_[frame_id]};
  if (frame_cnt_[frame_id] >= k_) {
    kth_set_.erase(node);
  } else {
    inf_set_.erase(node);
  }
}

auto LRUKReplacer::InsertToSet(frame_id_t frame_id) -> void {
  assert(frame_time_.count(frame_id) != 0);
  Node node{frame_id, frame_time_[frame_id]};
  if (frame_cnt_[frame_id] >= k_) {
    kth_set_.insert(node);
  } else {
    inf_set_.insert(node);
  }
}

auto LRUKReplacer::EvitFromSet(std::set<Node> &set_, frame_id_t *frame_id) -> bool {
  for (auto it = set_.begin(); it != set_.end(); ++it) {
    auto id = it->id_;
    set_.erase(it);
    *frame_id = id;
    return true;
  }
  return false;
}

auto LRUKReplacer::RemoveRecord(frame_id_t frame_id) -> void {
  evitable_.erase(frame_id);
  frame_time_.erase(frame_id);
  frame_cnt_.erase(frame_id);
  cur_size_--;
  replacer_size_++;
}

auto LRUKReplacer::GetTimeStamp() -> int64_t {
  struct timespec abs_time;
  clock_gettime(CLOCK_REALTIME, &abs_time);
  const int k = 1000000000;
  return abs_time.tv_sec * k + abs_time.tv_nsec;
}

}  // namespace bustub
