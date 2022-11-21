/**
 * extendible_hash_test.cpp
 */

#include <memory>
#include <thread>  // NOLINT

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(ExtendibleHashTableTest, SampleTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");
  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  std::string result;
  table->Find(9, result);
  EXPECT_EQ("i", result);
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));
}

// 16- 10000
// 4- 00100
// 6- 00110
// 22- 10110
// 24- 11000
// 10- 01010
// 31- 11111
// 7- 00111
// 9- 01001
// 20- 10100
// 26- 11010
TEST(ExtendibleHashTableTest, SampleTest1) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(3);

  table->Insert(16, "a");
  table->Insert(4, "b");
  table->Insert(6, "c");
  table->Insert(22, "d");
  table->Insert(24, "e");
  table->Insert(10, "f");
  table->Insert(31, "g");
  table->Insert(7, "h");
  table->Insert(9, "i");
  table->Insert(20, "i");
  table->Insert(26, "i");
  EXPECT_EQ(3, table->GetLocalDepth(0));
  EXPECT_EQ(1, table->GetLocalDepth(1));
  EXPECT_EQ(3, table->GetLocalDepth(4));
  EXPECT_EQ(3, table->GetGlobalDepth());
}

TEST(ExtendibleHashTableTest, ConcurrentInsertTest) {
  const int num_runs = 1;
  const int num_threads = 5;
  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([&table]() {
        for (int i = 0; i < 10000; i++) {
          table->Insert(i, i);
        }
      });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

}  // namespace bustub
