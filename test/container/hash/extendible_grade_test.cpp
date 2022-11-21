/**
 * extendible_hash_test.cpp
 */

#include <random>
#include <thread>  // NOLINT

#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(ExtendibleHashTableTest, InsertSplit) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  ASSERT_EQ(1, table->GetNumBuckets());
  ASSERT_EQ(0, table->GetLocalDepth(0));
  ASSERT_EQ(0, table->GetGlobalDepth());

  table->Insert(1, "a");
  table->Insert(2, "b");
  ASSERT_EQ(1, table->GetNumBuckets());
  ASSERT_EQ(0, table->GetLocalDepth(0));
  ASSERT_EQ(0, table->GetGlobalDepth());

  table->Insert(3, "c");  // first split
  ASSERT_EQ(2, table->GetNumBuckets());
  ASSERT_EQ(1, table->GetLocalDepth(0));
  ASSERT_EQ(1, table->GetLocalDepth(1));
  ASSERT_EQ(1, table->GetGlobalDepth());
  table->Insert(4, "d");

  table->Insert(5, "e");  // second split
  ASSERT_EQ(3, table->GetNumBuckets());
  ASSERT_EQ(1, table->GetLocalDepth(0));
  ASSERT_EQ(2, table->GetLocalDepth(1));
  ASSERT_EQ(1, table->GetLocalDepth(2));
  ASSERT_EQ(2, table->GetLocalDepth(3));
  ASSERT_EQ(2, table->GetGlobalDepth());

  table->Insert(6, "f");  // third split (global depth doesn't increase)
  ASSERT_EQ(4, table->GetNumBuckets());
  ASSERT_EQ(2, table->GetLocalDepth(0));
  ASSERT_EQ(2, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(2, table->GetLocalDepth(3));
  ASSERT_EQ(2, table->GetGlobalDepth());

  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");
  ASSERT_EQ(5, table->GetNumBuckets());
  ASSERT_EQ(2, table->GetLocalDepth(0));
  ASSERT_EQ(3, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(2, table->GetLocalDepth(3));
  ASSERT_EQ(2, table->GetLocalDepth(0));
  ASSERT_EQ(3, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(2, table->GetLocalDepth(3));
  ASSERT_EQ(3, table->GetGlobalDepth());

  // find table
  std::string result;
  table->Find(9, result);
  ASSERT_EQ("i", result);
  table->Find(8, result);
  ASSERT_EQ("h", result);
  table->Find(2, result);
  ASSERT_EQ("b", result);
  ASSERT_EQ(false, table->Find(10, result));

  // delete table
  ASSERT_EQ(true, table->Remove(8));
  ASSERT_EQ(true, table->Remove(4));
  ASSERT_EQ(true, table->Remove(1));
  ASSERT_EQ(false, table->Remove(20));
}

TEST(ExtendibleHashTableTest, InsertMultipleSplit) {
  {
    auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

    table->Insert(0, "0");
    table->Insert(1024, "1024");
    table->Insert(4, "4");  // this causes 3 splits

    ASSERT_EQ(4, table->GetNumBuckets());
    ASSERT_EQ(3, table->GetGlobalDepth());
    ASSERT_EQ(3, table->GetLocalDepth(0));
    ASSERT_EQ(1, table->GetLocalDepth(1));
    ASSERT_EQ(2, table->GetLocalDepth(2));
    ASSERT_EQ(1, table->GetLocalDepth(3));
    ASSERT_EQ(3, table->GetLocalDepth(4));
    ASSERT_EQ(1, table->GetLocalDepth(5));
    ASSERT_EQ(2, table->GetLocalDepth(6));
    ASSERT_EQ(1, table->GetLocalDepth(7));
  }
  {
    auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

    table->Insert(0, "0");
    table->Insert(1024, "1024");
    table->Insert(16, "16");  // this causes 5 splits

    ASSERT_EQ(6, table->GetNumBuckets());
    ASSERT_EQ(5, table->GetGlobalDepth());
  }
}

TEST(ExtendibleHashTableTest, ConcurrentInsertFind) {
  const int num_runs = 50;
  const int num_threads = 5;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() {
        // for random number generation
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, num_threads * 10);

        for (int i = 0; i < 10; i++) {
          table->Insert(tid * 10 + i, std::to_string(tid * 10 + i));

          // Run Find on random keys to let Thread Sanitizer check for race conditions
          std::string val;
          table->Find(dis(gen), val);
        }
      });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (int i = 0; i < num_threads * 10; i++) {
      std::string val;
      ASSERT_TRUE(table->Find(i, val));
      ASSERT_EQ(std::to_string(i), val);
    }
  }
}

TEST(ExtendibleHashTableTest, ConcurrentRemoveInsert) {
  const int num_threads = 5;
  const int num_runs = 50;

  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);
    std::vector<std::thread> threads;
    std::vector<std::string> values;
    values.reserve(100);
    for (int i = 0; i < 100; i++) {
      values.push_back(std::to_string(i));
    }
    for (unsigned int i = 0; i < values.size(); i++) {
      table->Insert(i, values[i]);
    }

    threads.reserve(num_threads);
    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() {
        for (int i = tid * 20; i < tid * 20 + 20; i++) {
          table->Remove(i);
          table->Insert(i + 400, std::to_string(i + 400));
        }
      });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    std::string val;
    for (int i = 0; i < 100; i++) {
      ASSERT_FALSE(table->Find(i, val));
    }
    for (int i = 400; i < 500; i++) {
      ASSERT_TRUE(table->Find(i, val));
      ASSERT_EQ(std::to_string(i), val);
    }
  }
}

TEST(ExtendibleHashTableTest, InitiallyEmpty) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  ASSERT_EQ(0, table->GetGlobalDepth());
  ASSERT_EQ(0, table->GetLocalDepth(0));

  std::string result;
  ASSERT_FALSE(table->Find(1, result));
  ASSERT_FALSE(table->Find(0, result));
  ASSERT_FALSE(table->Find(-1, result));
}

TEST(ExtendibleHashTableTest, InsertAndFind) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);

  std::vector<std::string> val;
  for (int i = 0; i <= 100; i++) {
    val.push_back(std::to_string(i));
  }
  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  table->Insert(5, val[5]);
  table->Insert(10, val[10]);
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  table->Insert(20, val[20]);
  table->Insert(7, val[7]);
  table->Insert(21, val[21]);
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);

  std::string result;
  ASSERT_TRUE(table->Find(4, result));
  ASSERT_EQ(val[4], result);
  ASSERT_TRUE(table->Find(12, result));
  ASSERT_EQ(val[12], result);
  ASSERT_TRUE(table->Find(16, result));
  ASSERT_EQ(val[16], result);
  ASSERT_TRUE(table->Find(64, result));
  ASSERT_EQ(val[64], result);
  ASSERT_TRUE(table->Find(5, result));
  ASSERT_EQ(val[5], result);
  ASSERT_TRUE(table->Find(10, result));
  ASSERT_EQ(val[10], result);
  ASSERT_TRUE(table->Find(51, result));
  ASSERT_EQ(val[51], result);
  ASSERT_TRUE(table->Find(15, result));
  ASSERT_EQ(val[15], result);
  ASSERT_TRUE(table->Find(18, result));
  ASSERT_EQ(val[18], result);
  ASSERT_TRUE(table->Find(20, result));
  ASSERT_EQ(val[20], result);
  ASSERT_TRUE(table->Find(7, result));
  ASSERT_EQ(val[7], result);
  ASSERT_TRUE(table->Find(21, result));
  ASSERT_EQ(val[21], result);
  ASSERT_TRUE(table->Find(11, result));
  ASSERT_EQ(val[11], result);
  ASSERT_TRUE(table->Find(19, result));
  ASSERT_EQ(val[19], result);

  ASSERT_FALSE(table->Find(0, result));
  ASSERT_FALSE(table->Find(1, result));
  ASSERT_FALSE(table->Find(-1, result));
  ASSERT_FALSE(table->Find(2, result));
  ASSERT_FALSE(table->Find(3, result));
  for (int i = 65; i < 1000; i++) {
    ASSERT_FALSE(table->Find(i, result));
  }
}

TEST(ExtendibleHashTableTest, GlobalDepth) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);

  std::vector<std::string> val;
  for (int i = 0; i <= 100; i++) {
    val.push_back(std::to_string(i));
  }

  // Inserting 4 keys belong to the same bucket
  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  ASSERT_EQ(0, table->GetGlobalDepth());

  // Inserting into another bucket
  table->Insert(5, val[5]);
  ASSERT_EQ(1, table->GetGlobalDepth());

  // Inserting into filled bucket 0
  table->Insert(10, val[10]);
  ASSERT_EQ(2, table->GetGlobalDepth());

  // Inserting 3 keys into buckets with space
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  ASSERT_EQ(2, table->GetGlobalDepth());

  // Inserting into filled buckets with local depth = global depth
  table->Insert(20, val[20]);
  ASSERT_EQ(3, table->GetGlobalDepth());

  // Inserting 2 keys into filled buckets with local depth < global depth
  table->Insert(7, val[7]);
  table->Insert(21, val[21]);
  ASSERT_EQ(3, table->GetGlobalDepth());

  // More Insertions(2 keys)
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);
  ASSERT_EQ(3, table->GetGlobalDepth());
}

TEST(ExtendibleHashTableTest, LocalDepth) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);

  std::vector<std::string> val;
  for (int i = 0; i <= 100; i++) {
    val.push_back(std::to_string(i));
  }

  // Inserting 4 keys belong to the same bucket
  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  ASSERT_EQ(0, table->GetLocalDepth(0));

  // Inserting into another bucket
  table->Insert(5, val[5]);
  ASSERT_EQ(1, table->GetLocalDepth(0));
  ASSERT_EQ(1, table->GetLocalDepth(1));

  // Inserting into filled bucket 0
  table->Insert(10, val[10]);
  ASSERT_EQ(2, table->GetLocalDepth(0));
  ASSERT_EQ(1, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(1, table->GetLocalDepth(3));

  // Inserting 3 keys into buckets with space
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  ASSERT_EQ(2, table->GetLocalDepth(0));
  ASSERT_EQ(1, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(1, table->GetLocalDepth(3));

  // Inserting into filled buckets with local depth = global depth
  table->Insert(20, val[20]);
  ASSERT_EQ(3, table->GetLocalDepth(0));
  ASSERT_EQ(1, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(1, table->GetLocalDepth(3));
  ASSERT_EQ(3, table->GetLocalDepth(4));
  ASSERT_EQ(1, table->GetLocalDepth(5));
  ASSERT_EQ(2, table->GetLocalDepth(6));
  ASSERT_EQ(1, table->GetLocalDepth(7));

  // Inserting 2 keys into filled buckets with local depth < global depth
  table->Insert(7, val[7]);
  table->Insert(21, val[21]);
  ASSERT_EQ(3, table->GetLocalDepth(0));
  ASSERT_EQ(2, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(2, table->GetLocalDepth(3));
  ASSERT_EQ(3, table->GetLocalDepth(4));
  ASSERT_EQ(2, table->GetLocalDepth(5));
  ASSERT_EQ(2, table->GetLocalDepth(6));
  ASSERT_EQ(2, table->GetLocalDepth(7));

  // More Insertions(2 keys)
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);
  ASSERT_EQ(3, table->GetLocalDepth(0));
  ASSERT_EQ(2, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(3, table->GetLocalDepth(3));
  ASSERT_EQ(3, table->GetLocalDepth(4));
  ASSERT_EQ(2, table->GetLocalDepth(5));
  ASSERT_EQ(2, table->GetLocalDepth(6));
  ASSERT_EQ(3, table->GetLocalDepth(7));
}

TEST(ExtendibleHashTableTest, InsertAndReplace) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);

  std::vector<std::string> val;
  std::vector<std::string> newval;
  for (int i = 0; i <= 100; i++) {
    val.push_back(std::to_string(i));
    newval.push_back(std::to_string(i + 1));
  }

  std::string result;

  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  table->Insert(5, val[5]);
  table->Insert(10, val[10]);
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  table->Insert(20, val[20]);
  table->Insert(7, val[7]);
  table->Insert(21, val[21]);
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);
  table->Insert(4, newval[4]);
  table->Insert(12, newval[12]);
  table->Insert(16, newval[16]);
  table->Insert(64, newval[64]);
  table->Insert(5, newval[5]);
  table->Insert(10, newval[10]);
  table->Insert(51, newval[51]);
  table->Insert(15, newval[15]);
  table->Insert(18, newval[18]);
  table->Insert(20, newval[20]);
  table->Insert(7, newval[7]);
  table->Insert(21, newval[21]);
  table->Insert(11, newval[11]);
  table->Insert(19, newval[19]);

  ASSERT_TRUE(table->Find(4, result));
  ASSERT_EQ(newval[4], result);
  ASSERT_TRUE(table->Find(12, result));
  ASSERT_EQ(newval[12], result);
  ASSERT_TRUE(table->Find(16, result));
  ASSERT_EQ(newval[16], result);
  ASSERT_TRUE(table->Find(64, result));
  ASSERT_EQ(newval[64], result);
  ASSERT_TRUE(table->Find(5, result));
  ASSERT_EQ(newval[5], result);
  ASSERT_TRUE(table->Find(10, result));
  ASSERT_EQ(newval[10], result);
  ASSERT_TRUE(table->Find(51, result));
  ASSERT_EQ(newval[51], result);
  ASSERT_TRUE(table->Find(15, result));
  ASSERT_EQ(newval[15], result);
  ASSERT_TRUE(table->Find(18, result));
  ASSERT_EQ(newval[18], result);
  ASSERT_TRUE(table->Find(20, result));
  ASSERT_EQ(newval[20], result);
  ASSERT_TRUE(table->Find(7, result));
  ASSERT_EQ(newval[7], result);
  ASSERT_TRUE(table->Find(21, result));
  ASSERT_EQ(newval[21], result);
  ASSERT_TRUE(table->Find(11, result));
  ASSERT_EQ(newval[11], result);
  ASSERT_TRUE(table->Find(19, result));
  ASSERT_EQ(newval[19], result);
}

TEST(ExtendibleHashTableTest, Remove) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(4);

  std::vector<std::string> val;
  for (int i = 0; i <= 100; i++) {
    val.push_back(std::to_string(i));
  }

  std::string result;

  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  table->Insert(5, val[5]);
  table->Insert(10, val[10]);
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  table->Insert(20, val[20]);
  table->Insert(7, val[7]);
  table->Insert(21, val[21]);
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);

  ASSERT_TRUE(table->Remove(4));
  ASSERT_TRUE(table->Remove(12));
  ASSERT_TRUE(table->Remove(16));
  ASSERT_TRUE(table->Remove(64));
  ASSERT_TRUE(table->Remove(5));
  ASSERT_TRUE(table->Remove(10));

  ASSERT_FALSE(table->Find(4, result));
  ASSERT_FALSE(table->Find(12, result));
  ASSERT_FALSE(table->Find(16, result));
  ASSERT_FALSE(table->Find(64, result));
  ASSERT_FALSE(table->Find(5, result));
  ASSERT_FALSE(table->Find(10, result));
  ASSERT_TRUE(table->Find(51, result));
  ASSERT_EQ(val[51], result);
  ASSERT_TRUE(table->Find(15, result));
  ASSERT_EQ(val[15], result);
  ASSERT_TRUE(table->Find(18, result));
  ASSERT_EQ(val[18], result);
  ASSERT_TRUE(table->Find(20, result));
  ASSERT_EQ(val[20], result);
  ASSERT_TRUE(table->Find(7, result));
  ASSERT_EQ(val[7], result);
  ASSERT_TRUE(table->Find(21, result));
  ASSERT_EQ(val[21], result);
  ASSERT_TRUE(table->Find(11, result));
  ASSERT_EQ(val[11], result);
  ASSERT_TRUE(table->Find(19, result));
  ASSERT_EQ(val[19], result);

  ASSERT_TRUE(table->Remove(51));
  ASSERT_TRUE(table->Remove(15));
  ASSERT_TRUE(table->Remove(18));

  ASSERT_FALSE(table->Remove(5));
  ASSERT_FALSE(table->Remove(10));
  ASSERT_FALSE(table->Remove(51));
  ASSERT_FALSE(table->Remove(15));
  ASSERT_FALSE(table->Remove(18));

  ASSERT_TRUE(table->Remove(20));
  ASSERT_TRUE(table->Remove(7));
  ASSERT_TRUE(table->Remove(21));
  ASSERT_TRUE(table->Remove(11));
  ASSERT_TRUE(table->Remove(19));

  for (int i = 0; i < 1000; i++) {
    ASSERT_FALSE(table->Find(i, result));
  }

  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  table->Insert(5, val[5]);
  table->Insert(10, val[10]);
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  table->Insert(20, val[20]);
  table->Insert(7, val[7]);
  table->Insert(21, val[21]);
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);

  ASSERT_TRUE(table->Find(4, result));
  ASSERT_EQ(val[4], result);
  ASSERT_TRUE(table->Find(12, result));
  ASSERT_EQ(val[12], result);
  ASSERT_TRUE(table->Find(16, result));
  ASSERT_EQ(val[16], result);
  ASSERT_TRUE(table->Find(64, result));
  ASSERT_EQ(val[64], result);
  ASSERT_TRUE(table->Find(5, result));
  ASSERT_EQ(val[5], result);
  ASSERT_TRUE(table->Find(10, result));
  ASSERT_EQ(val[10], result);
  ASSERT_TRUE(table->Find(51, result));
  ASSERT_EQ(val[51], result);
  ASSERT_TRUE(table->Find(15, result));
  ASSERT_EQ(val[15], result);
  ASSERT_TRUE(table->Find(18, result));
  ASSERT_EQ(val[18], result);
  ASSERT_TRUE(table->Find(20, result));
  ASSERT_EQ(val[20], result);
  ASSERT_TRUE(table->Find(7, result));
  ASSERT_EQ(val[7], result);
  ASSERT_TRUE(table->Find(21, result));
  ASSERT_EQ(val[21], result);
  ASSERT_TRUE(table->Find(11, result));
  ASSERT_EQ(val[11], result);
  ASSERT_TRUE(table->Find(19, result));
  ASSERT_EQ(val[19], result);

  ASSERT_EQ(3, table->GetLocalDepth(0));
  ASSERT_EQ(2, table->GetLocalDepth(1));
  ASSERT_EQ(2, table->GetLocalDepth(2));
  ASSERT_EQ(3, table->GetLocalDepth(3));
  ASSERT_EQ(3, table->GetLocalDepth(4));
  ASSERT_EQ(2, table->GetLocalDepth(5));
  ASSERT_EQ(2, table->GetLocalDepth(6));
  ASSERT_EQ(3, table->GetLocalDepth(7));
  ASSERT_EQ(3, table->GetGlobalDepth());
}

TEST(ExtendibleHashTableTest, GetNumBuckets) {
  auto table = std::make_unique<ExtendibleHashTable<int, int>>(4);

  std::vector<int> val;
  for (int i = 0; i <= 100; i++) {
    val.push_back(i);
  }

  // Inserting 4 keys belong to the same bucket
  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  ASSERT_EQ(1, table->GetNumBuckets());
  // Inserting into another bucket

  table->Insert(31, val[31]);
  ASSERT_EQ(2, table->GetNumBuckets());

  // Inserting into filled bucket 0
  table->Insert(10, val[10]);
  ASSERT_EQ(3, table->GetNumBuckets());

  // Inserting 3 keys into buckets with space
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  ASSERT_EQ(3, table->GetNumBuckets());

  // Inserting into filled buckets with local depth = global depth
  table->Insert(20, val[20]);
  ASSERT_EQ(4, table->GetNumBuckets());

  // Inserting 2 keys into filled buckets with local depth < global depth
  // Adding a new bucket and inserting will still be full so
  // will test if they add another bucket again.
  table->Insert(7, val[7]);
  table->Insert(23, val[21]);
  ASSERT_EQ(6, table->GetNumBuckets());

  // More Insertions(2 keys)
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);
  ASSERT_EQ(6, table->GetNumBuckets());
}

TEST(ExtendibleHashTableTest, IntegratedTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(7);

  std::vector<std::string> val;

  for (int i = 0; i <= 2000; i++) {
    val.push_back(std::to_string(i));
  }

  for (int i = 1; i <= 1000; i++) {
    table->Insert(i, val[i]);
  }

  int global_depth = table->GetGlobalDepth();
  ASSERT_EQ(8, global_depth);

  for (int i = 1; i <= 1000; i++) {
    std::string result;
    ASSERT_TRUE(table->Find(i, result));
    ASSERT_EQ(val[i], result);
  }

  for (int i = 1; i <= 500; i++) {
    ASSERT_TRUE(table->Remove(i));
  }

  for (int i = 1; i <= 500; i++) {
    std::string result;
    ASSERT_FALSE(table->Find(i, result));
    ASSERT_FALSE(table->Remove(i));
  }

  for (int i = 501; i <= 1000; i++) {
    std::string result;
    ASSERT_TRUE(table->Find(i, result));
    ASSERT_EQ(val[i], result);
  }

  for (int i = 1; i <= 2000; i++) {
    table->Insert(i, val[i]);
  }

  global_depth = table->GetGlobalDepth();
  ASSERT_EQ(9, global_depth);

  for (int i = 1; i <= 2000; i++) {
    std::string result;
    ASSERT_TRUE(table->Find(i, result));
    ASSERT_EQ(val[i], result);
  }

  for (int i = 1; i <= 2000; i++) {
    ASSERT_TRUE(table->Remove(i));
  }

  for (int i = 1; i <= 2000; i++) {
    std::string result;
    ASSERT_FALSE(table->Find(i, result));
    ASSERT_FALSE(table->Remove(i));
  }
}
}  // namespace bustub
