/**
 * lru_k_replacer_test.cpp
 */

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/lru_k_replacer.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(LRUKReplacerTest, SampleTest) {
  LRUKReplacer lru_replacer(7, 2);

  // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(6);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);
  lru_replacer.SetEvictable(6, false);
  ASSERT_EQ(5, lru_replacer.Size());

  // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
  // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
  lru_replacer.RecordAccess(1);

  // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
  // first based on LRU.
  int value;
  lru_replacer.Evict(&value);
  ASSERT_EQ(2, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(4, value);
  ASSERT_EQ(2, lru_replacer.Size());

  // Scenario: Now replacer has frames [5,1].
  // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(4);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  ASSERT_EQ(4, lru_replacer.Size());

  // Scenario: continue looking for victims. We expect 3 to be evicted next.
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
  lru_replacer.SetEvictable(6, true);
  ASSERT_EQ(4, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(6, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Now we have [1,5,4]. Continue looking for victims.
  lru_replacer.SetEvictable(1, false);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(5, value);
  ASSERT_EQ(1, lru_replacer.Size());

  // Update access history for 1. Now we have [4,1]. Next victim is 4.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(value, 4);

  ASSERT_EQ(1, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(value, 1);
  ASSERT_EQ(0, lru_replacer.Size());

  // These operations should not modify size
  ASSERT_EQ(false, lru_replacer.Evict(&value));
  ASSERT_EQ(0, lru_replacer.Size());
  lru_replacer.Remove(1);
  ASSERT_EQ(0, lru_replacer.Size());
}

TEST(LRUKReplacerTest, Evict) {
  {
    // Empty and try removing
    LRUKReplacer lru_replacer(10, 2);
    int result;
    auto success = lru_replacer.Evict(&result);
    ASSERT_EQ(success, false) << "Check your return value behavior for LRUKReplacer::Evict";
  }

  {
    // Can only evict element if evictable=true
    int result;
    LRUKReplacer lru_replacer(10, 2);
    lru_replacer.RecordAccess(2);
    lru_replacer.SetEvictable(2, false);
    ASSERT_EQ(false, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    lru_replacer.SetEvictable(2, true);
    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(2, result) << "Check your return value behavior for LRUKReplacer::Evict";
  }

  {
    // Elements with less than k history should have max backward k-dist and get evicted first based on LRU
    LRUKReplacer lru_replacer(10, 3);
    int result;
    // 1 has three access histories, where as 2 has two access histories
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(1);
    lru_replacer.SetEvictable(2, true);
    lru_replacer.SetEvictable(1, true);

    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(2, result) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(1, result) << "Check your return value behavior for LRUKReplacer::Evict";
  }

  {
    // Select element with largest backward k-dist to evict
    // Evicted page should not maintain previous history
    LRUKReplacer lru_replacer(10, 3);
    int result;
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(3);
    lru_replacer.RecordAccess(3);
    lru_replacer.RecordAccess(3);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(3);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(1);
    lru_replacer.SetEvictable(2, true);
    lru_replacer.SetEvictable(1, true);
    lru_replacer.SetEvictable(3, true);

    // Should evict in this order
    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(3, result) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(2, result) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(1, result) << "Check your return value behavior for LRUKReplacer::Evict";
  }

  {
    // Evicted page should not maintain previous history
    LRUKReplacer lru_replacer(10, 3);
    int result;
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(1);
    lru_replacer.SetEvictable(2, true);
    lru_replacer.SetEvictable(1, true);

    // At this point, page 1 should be evicted since it has higher backward k distance
    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(1, result) << "Check your return value behavior for LRUKReplacer::Evict";

    lru_replacer.RecordAccess(1);
    lru_replacer.SetEvictable(1, true);

    // 1 should still be evicted since it has max backward k distance
    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(1, result) << "Check your return value behavior for LRUKReplacer::Evict";
  }

  {
    LRUKReplacer lru_replacer(10, 3);
    int result;
    lru_replacer.RecordAccess(1);  // ts=0
    lru_replacer.RecordAccess(2);  // ts=1
    lru_replacer.RecordAccess(3);  // ts=2
    lru_replacer.RecordAccess(4);  // ts=3
    lru_replacer.RecordAccess(1);  // ts=4
    lru_replacer.RecordAccess(2);  // ts=5
    lru_replacer.RecordAccess(3);  // ts=6
    lru_replacer.RecordAccess(1);  // ts=7
    lru_replacer.RecordAccess(2);  // ts=8
    lru_replacer.SetEvictable(1, true);
    lru_replacer.SetEvictable(2, true);
    lru_replacer.SetEvictable(3, true);
    lru_replacer.SetEvictable(4, true);

    // Max backward k distance follow lru
    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(3, result) << "Check your return value behavior for LRUKReplacer::Evict";
    lru_replacer.RecordAccess(4);  // ts=9
    lru_replacer.RecordAccess(4);  // ts=10

    // Now 1 has largest backward k distance, followed by 2 and 4
    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(1, result) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(2, result) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(4, result) << "Check your return value behavior for LRUKReplacer::Evict";
  }

  {
    // New unused page with max backward k-dist should be evicted first
    LRUKReplacer lru_replacer(10, 2);
    int result;
    lru_replacer.RecordAccess(1);  // ts=0
    lru_replacer.RecordAccess(2);  // ts=1
    lru_replacer.RecordAccess(3);  // ts=2
    lru_replacer.RecordAccess(4);  // ts=3
    lru_replacer.RecordAccess(1);  // ts=4
    lru_replacer.RecordAccess(2);  // ts=5
    lru_replacer.RecordAccess(3);  // ts=6
    lru_replacer.RecordAccess(4);  // ts=7

    lru_replacer.SetEvictable(2, true);
    lru_replacer.SetEvictable(1, true);

    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(1, result) << "Check your return value behavior for LRUKReplacer::Evict";

    lru_replacer.RecordAccess(5);  // ts=9
    lru_replacer.SetEvictable(5, true);
    ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
    ASSERT_EQ(5, result) << "Check your return value behavior for LRUKReplacer::Evict";
  }

  {
    // 1/4 page has one access history, 1/4 has two accesses, 1/4 has three, and 1/4 has four
    LRUKReplacer lru_replacer(1000, 3);
    int result;
    for (int j = 0; j < 4; ++j) {
      for (int i = j * 250; i < 1000; ++i) {
        lru_replacer.RecordAccess(i);
        lru_replacer.SetEvictable(i, true);
      }
    }
    ASSERT_EQ(1000, lru_replacer.Size());

    // Set second 1/4 to be non-evictable
    for (int i = 250; i < 500; ++i) {
      lru_replacer.SetEvictable(i, false);
    }
    ASSERT_EQ(750, lru_replacer.Size());

    // Remove first 100 elements
    for (int i = 0; i < 100; ++i) {
      lru_replacer.Remove(i);
    }
    ASSERT_EQ(650, lru_replacer.Size());

    // Try to evict some elements
    for (int i = 100; i < 600; ++i) {
      if (i < 250 || i >= 500) {
        ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
        ASSERT_EQ(i, result) << "Check your return value behavior for LRUKReplacer::Evict";
      }
    }
    ASSERT_EQ(400, lru_replacer.Size());

    // Add second 1/4 elements back and modify access history for the last 150 elements of third 1/4 elements.
    for (int i = 250; i < 500; ++i) {
      lru_replacer.SetEvictable(i, true);
    }
    ASSERT_EQ(650, lru_replacer.Size());
    for (int i = 600; i < 750; ++i) {
      lru_replacer.RecordAccess(i);
      lru_replacer.RecordAccess(i);
    }
    ASSERT_EQ(650, lru_replacer.Size());

    // We expect the following eviction pattern
    for (int i = 250; i < 500; ++i) {
      ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
      ASSERT_EQ(i, result) << "Check your return value behavior for LRUKReplacer::Evict";
    }
    ASSERT_EQ(400, lru_replacer.Size());
    for (int i = 750; i < 1000; ++i) {
      ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
      ASSERT_EQ(i, result) << "Check your return value behavior for LRUKReplacer::Evict";
    }
    ASSERT_EQ(150, lru_replacer.Size());
    for (int i = 600; i < 750; ++i) {
      ASSERT_EQ(true, lru_replacer.Evict(&result)) << "Check your return value behavior for LRUKReplacer::Evict";
      ASSERT_EQ(i, result) << "Check your return value behavior for LRUKReplacer::Evict";
    }
    ASSERT_EQ(0, lru_replacer.Size());
  }
}

TEST(LRUKReplacerTest, Size) {
  {
    // Size is increased/decreased if SetEvictable's argument is different from node state
    LRUKReplacer lru_replacer(10, 2);
    lru_replacer.RecordAccess(1);
    lru_replacer.SetEvictable(1, true);
    ASSERT_EQ(1, lru_replacer.Size()) << "Check your return value for LRUKReplacer::Size";
    lru_replacer.SetEvictable(1, true);
    ASSERT_EQ(1, lru_replacer.Size()) << "Check your return value for LRUKReplacer::Size";
    lru_replacer.SetEvictable(1, false);
    ASSERT_EQ(0, lru_replacer.Size()) << "Check your return value for LRUKReplacer::Size";
    lru_replacer.SetEvictable(1, false);
    ASSERT_EQ(0, lru_replacer.Size()) << "Check your return value for LRUKReplacer::Size";
  }

  {
    // Insert new history. Calling SetEvictable = false should not modify Size.
    // Calling SetEvictable = true should increase Size.
    // Size should only be called when SetEvictable is called for every inserted node.
    LRUKReplacer lru_replacer(10, 2);
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(3);
    lru_replacer.SetEvictable(1, false);
    lru_replacer.SetEvictable(2, false);
    lru_replacer.SetEvictable(3, false);
    ASSERT_EQ(0, lru_replacer.Size()) << "Check your return value for LRUKReplacer::Size";

    LRUKReplacer lru_replacer2(10, 2);
    lru_replacer2.RecordAccess(1);
    lru_replacer2.RecordAccess(2);
    lru_replacer2.RecordAccess(3);
    lru_replacer2.SetEvictable(1, true);
    lru_replacer2.SetEvictable(2, true);
    lru_replacer2.SetEvictable(3, true);
    ASSERT_EQ(3, lru_replacer2.Size()) << "Check your return value for LRUKReplacer::Size";
  }

  // Size depends on how many nodes have evictable=true
  {
    LRUKReplacer lru_replacer(10, 2);
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(3);
    lru_replacer.RecordAccess(4);
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(3);
    lru_replacer.RecordAccess(4);
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(3);
    lru_replacer.RecordAccess(4);
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(3);
    lru_replacer.RecordAccess(4);
    lru_replacer.SetEvictable(1, false);
    lru_replacer.SetEvictable(2, false);
    lru_replacer.SetEvictable(3, false);
    lru_replacer.SetEvictable(4, false);
    ASSERT_EQ(0, lru_replacer.Size()) << "Check your return value for LRUKReplacer::Size";
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(3);
    lru_replacer.RecordAccess(4);
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(3);
    lru_replacer.RecordAccess(4);
    lru_replacer.SetEvictable(1, true);
    lru_replacer.SetEvictable(2, true);
    lru_replacer.SetEvictable(1, true);
    lru_replacer.SetEvictable(2, true);
    ASSERT_EQ(2, lru_replacer.Size()) << "Check your return value for LRUKReplacer::Size";
    // Evicting a page should decrement Size
    lru_replacer.RecordAccess(4);
  }

  {
    // Remove a page to decrement its size
    LRUKReplacer lru_replacer(10, 2);
    lru_replacer.RecordAccess(1);
    lru_replacer.SetEvictable(1, true);
    lru_replacer.RecordAccess(2);
    lru_replacer.SetEvictable(2, true);
    lru_replacer.RecordAccess(3);
    lru_replacer.SetEvictable(3, true);
    ASSERT_EQ(3, lru_replacer.Size()) << "Check your return value for LRUKReplacer::Size";
    lru_replacer.Remove(1);
    ASSERT_EQ(2, lru_replacer.Size()) << "Check your return value for LRUKReplacer::Size";
    lru_replacer.Remove(2);
    ASSERT_EQ(1, lru_replacer.Size()) << "Check your return value for LRUKReplacer::Size";
  }

  {
    // Victiming a page should decrement its size
    LRUKReplacer lru_replacer(10, 3);
    int result;
    // 1 has three access histories, where as 2 only has two access histories
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(1);
    lru_replacer.RecordAccess(2);
    lru_replacer.RecordAccess(1);
    lru_replacer.SetEvictable(2, true);
    lru_replacer.SetEvictable(1, true);
    ASSERT_EQ(2, lru_replacer.Size()) << "Check your return value for LRUKReplacer::Size";

    ASSERT_EQ(true, lru_replacer.Evict(&result));
    ASSERT_EQ(2, result);
    ASSERT_EQ(1, lru_replacer.Size()) << "Check your return value for LRUKReplacer::Size";
    ASSERT_EQ(true, lru_replacer.Evict(&result));
    ASSERT_EQ(1, result);
    ASSERT_EQ(0, lru_replacer.Size()) << "Check your return value for LRUKReplacer::Size";
  }

  {
    LRUKReplacer lru_replacer(10, 2);
    lru_replacer.RecordAccess(1);
    lru_replacer.SetEvictable(1, true);
    lru_replacer.RecordAccess(2);
    lru_replacer.SetEvictable(2, true);
    lru_replacer.RecordAccess(3);
    lru_replacer.SetEvictable(3, true);
    ASSERT_EQ(3, lru_replacer.Size());
    lru_replacer.Remove(1);
    ASSERT_EQ(2, lru_replacer.Size());

    lru_replacer.SetEvictable(1, true);
    lru_replacer.SetEvictable(2, true);
    lru_replacer.SetEvictable(3, true);
    lru_replacer.Remove(2);
    ASSERT_EQ(1, lru_replacer.Size());

    // Delete non existent page should do nothing
    lru_replacer.Remove(1);
    lru_replacer.Remove(4);
    ASSERT_EQ(1, lru_replacer.Size());
  }
}

TEST(LRUKReplacerTest, ConcurrencyTest) {  // NOLINT
  // 1/4 page has one access history, 1/4 has two accesses, 1/4 has three, and 1/4 has four
  LRUKReplacer lru_replacer(1000, 3);
  std::vector<std::thread> threads;

  auto record_access_task = [&](int i, bool if_set_evict, bool evictable) {
    lru_replacer.RecordAccess(i);
    if (if_set_evict) {
      lru_replacer.SetEvictable(i, evictable);
    }
  };
  auto remove_task = [&](int i) { lru_replacer.Remove(i); };
  auto set_evictable_task = [&](int i, bool evictable) { lru_replacer.SetEvictable(i, evictable); };

  auto record_access_thread = [&](int from_i, int to_i, bool if_set_evict, bool evictable) {
    for (auto i = from_i; i < to_i; i++) {
      record_access_task(i, if_set_evict, evictable);
    }
  };

  auto remove_task_thread = [&](int from_i, int to_i) {
    for (auto i = from_i; i < to_i; i++) {
      remove_task(i);
    }
  };

  auto set_evictable_thread = [&](int from_i, int to_i, bool evictable) {
    for (auto i = from_i; i < to_i; i++) {
      set_evictable_task(i, evictable);
    }
  };

  // Record first 1000 accesses. Set all frames to be evictable.
  for (int i = 0; i < 1000; i += 100) {
    threads.emplace_back(std::thread{record_access_thread, i, i + 100, true, true});
  }
  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();
  ASSERT_EQ(1000, lru_replacer.Size());

  // Remove frame id 250-500, set some keys to be non-evictable, and insert accesses concurrently
  threads.emplace_back(std::thread{record_access_thread, 250, 1000, true, true});
  threads.emplace_back(std::thread{record_access_thread, 500, 1000, true, true});
  threads.emplace_back(std::thread{record_access_thread, 750, 1000, true, true});
  threads.emplace_back(std::thread{remove_task_thread, 250, 400});
  threads.emplace_back(std::thread{remove_task_thread, 400, 500});
  threads.emplace_back(std::thread{set_evictable_thread, 0, 150, false});
  threads.emplace_back(std::thread{set_evictable_thread, 150, 250, false});

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Call remove again to ensure all items between 250, 500 have been removed.
  threads.emplace_back(std::thread{remove_task_thread, 250, 400});
  threads.emplace_back(std::thread{remove_task_thread, 400, 500});

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  ASSERT_EQ(500, lru_replacer.Size());

  std::mutex mutex;
  std::vector<int> evicted_elements;

  auto evict_task = [&](bool success) -> int {
    int evicted_value;
    BUSTUB_ASSERT(success == lru_replacer.Evict(&evicted_value), "evict not successful!");
    return evicted_value;
  };

  auto evict_task_thread = [&](int from_i, int to_i, bool success) {
    std::vector<int> local_evicted_elements;

    for (auto i = from_i; i < to_i; i++) {
      local_evicted_elements.push_back(evict_task(success));
    }
    {
      std::scoped_lock lock(mutex);
      for (const auto &evicted_value : local_evicted_elements) {
        evicted_elements.push_back(evicted_value);
      }
    }
  };

  // Remove elements, append history, and evict concurrently
  // Some of these frames are non-evictable and should not be removed.
  threads.emplace_back(std::thread{remove_task_thread, 250, 400});
  threads.emplace_back(std::thread{remove_task_thread, 400, 500});
  threads.emplace_back(std::thread{record_access_thread, 500, 700, false, true});
  threads.emplace_back(std::thread{record_access_thread, 700, 1000, false, true});
  threads.emplace_back(std::thread{evict_task_thread, 500, 600, true});
  threads.emplace_back(std::thread{evict_task_thread, 600, 800, true});
  threads.emplace_back(std::thread{evict_task_thread, 800, 1000, true});

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();
  ASSERT_EQ(0, lru_replacer.Size());
  ASSERT_EQ(evicted_elements.size(), 500);

  std::sort(evicted_elements.begin(), evicted_elements.end());
  for (int i = 500; i < 1000; ++i) {
    ASSERT_EQ(i, evicted_elements[i - 500]);
  }
}
}  // namespace bustub
