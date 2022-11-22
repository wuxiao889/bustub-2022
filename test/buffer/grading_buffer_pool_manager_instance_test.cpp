//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_instance_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "mock_buffer_pool_manager.h"  // NOLINT

namespace bustub {

#define BufferPoolManager MockBufferPoolManager

// NOLINTNEXTLINE
TEST(BufferPoolManagerInstanceTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  ASSERT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), sizeof(page0->GetData()), "Hello");
  ASSERT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    ASSERT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    ASSERT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one cache frame left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(true, bpm->UnpinPage(i, true));
  }
  for (int i = 0; i < 4; ++i) {
    ASSERT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  ASSERT_EQ(0, strcmp(page0->GetData(), "Hello"));
  ASSERT_EQ(true, bpm->UnpinPage(0, true));
  // NewPage again, and now all buffers are pinned. Page 0 would be failed to fetch.
  ASSERT_NE(nullptr, bpm->NewPage(&page_id_temp));
  ASSERT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, BinaryDataTest) {  // NOLINT
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 5;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManagerInstance(buffer_pool_size, disk_manager, k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  ASSERT_EQ(0, page_id_temp);

  char random_binary_data[PAGE_SIZE];
  unsigned int seed = 15645;
  for (char &i : random_binary_data) {
    i = static_cast<char>(rand_r(&seed) % 256);
  }

  random_binary_data[PAGE_SIZE / 2] = '\0';
  random_binary_data[PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::strncpy(page0->GetData(), random_binary_data, PAGE_SIZE);
  ASSERT_EQ(0, std::strcmp(page0->GetData(), random_binary_data));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    ASSERT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    ASSERT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one cache frame left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    ASSERT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  ASSERT_EQ(0, strcmp(page0->GetData(), random_binary_data));
  ASSERT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, NewPage) {  // NOLINT
  page_id_t temp_page_id;
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(10, disk_manager, 5);

  std::vector<page_id_t> page_ids;

  for (int i = 0; i < 10; ++i) {
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    strcpy(new_page->GetData(), std::to_string(i).c_str());  // NOLINT
    page_ids.push_back(temp_page_id);
  }

  // all the pages are pinned, the buffer pool is full
  for (int i = 0; i < 100; ++i) {
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_EQ(nullptr, new_page);
  }

  // upin the first five pages, add them to LRU list, set as dirty
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(true, bpm->UnpinPage(page_ids[i], true));
  }

  // we have 5 empty slots in LRU list, evict page zero out of buffer pool
  for (int i = 0; i < 5; ++i) {
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    page_ids[i] = temp_page_id;
  }

  // all the pages are pinned, the buffer pool is full
  for (int i = 0; i < 100; ++i) {
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_EQ(nullptr, new_page);
  }

  // upin the first five pages, add them to LRU list
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(true, bpm->UnpinPage(page_ids[i], false));
  }

  // we have 5 empty slots in LRU list, evict page zero out of buffer pool
  for (int i = 0; i < 5; ++i) {
    ASSERT_NE(nullptr, bpm->NewPage(&temp_page_id));
  }

  // all the pages are pinned, the buffer pool is full
  for (int i = 0; i < 100; ++i) {
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_EQ(nullptr, new_page);
  }

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, UnpinPage) {  // NOLINT
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(2, disk_manager, 5);

  page_id_t pageid0;
  auto *page0 = bpm->NewPage(&pageid0);
  ASSERT_NE(nullptr, page0);
  strcpy(page0->GetData(), "page0");  // NOLINT

  page_id_t pageid1;
  auto *page1 = bpm->NewPage(&pageid1);
  ASSERT_NE(nullptr, page1);
  strcpy(page1->GetData(), "page1");  // NOLINT

  ASSERT_EQ(1, bpm->UnpinPage(pageid0, true));
  ASSERT_EQ(1, bpm->UnpinPage(pageid1, true));

  for (int i = 0; i < 2; i++) {
    page_id_t temp_page_id;
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    bpm->UnpinPage(temp_page_id, true);
  }

  auto *page = bpm->FetchPage(pageid0);
  ASSERT_EQ(0, strcmp(page->GetData(), "page0"));
  strcpy(page->GetData(), "page0updated");  // NOLINT

  page = bpm->FetchPage(pageid1);
  ASSERT_EQ(0, strcmp(page->GetData(), "page1"));
  strcpy(page->GetData(), "page1updated");  // NOLINT

  ASSERT_EQ(1, bpm->UnpinPage(pageid0, false));
  ASSERT_EQ(1, bpm->UnpinPage(pageid1, true));

  for (int i = 0; i < 2; i++) {
    page_id_t temp_page_id;
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    bpm->UnpinPage(temp_page_id, true);
  }

  page = bpm->FetchPage(pageid0);
  ASSERT_EQ(0, strcmp(page->GetData(), "page0"));
  strcpy(page->GetData(), "page0updated");  // NOLINT

  page = bpm->FetchPage(pageid1);
  ASSERT_EQ(0, strcmp(page->GetData(), "page1updated"));
  strcpy(page->GetData(), "page1againupdated");  // NOLINT

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, FetchPage) {  // NOLINT
  page_id_t temp_page_id;
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(10, disk_manager, 5);

  std::vector<Page *> pages;
  std::vector<page_id_t> page_ids;
  std::vector<std::string> content;

  for (int i = 0; i < 10; ++i) {
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    strcpy(new_page->GetData(), std::to_string(i).c_str());  // NOLINT
    pages.push_back(new_page);
    page_ids.push_back(temp_page_id);
    content.push_back(std::to_string(i));
  }

  for (int i = 0; i < 10; ++i) {
    auto *page = bpm->FetchPage(page_ids[i]);
    ASSERT_NE(nullptr, page);
    ASSERT_EQ(pages[i], page);
    ASSERT_EQ(0, std::strcmp(std::to_string(i).c_str(), (page->GetData())));
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    bpm->FlushPage(page_ids[i]);
  }

  for (int i = 0; i < 10; ++i) {
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    bpm->UnpinPage(temp_page_id, true);
  }

  for (int i = 0; i < 10; ++i) {
    auto *page = bpm->FetchPage(page_ids[i]);
    ASSERT_NE(nullptr, page);
  }

  ASSERT_EQ(1, bpm->UnpinPage(page_ids[4], true));
  auto *new_page = bpm->NewPage(&temp_page_id);
  ASSERT_NE(nullptr, new_page);
  ASSERT_EQ(nullptr, bpm->FetchPage(page_ids[4]));

  // Check Clock
  auto *page5 = bpm->FetchPage(page_ids[5]);
  auto *page6 = bpm->FetchPage(page_ids[6]);
  auto *page7 = bpm->FetchPage(page_ids[7]);
  ASSERT_NE(nullptr, page5);
  ASSERT_NE(nullptr, page6);
  ASSERT_NE(nullptr, page7);
  strcpy(page5->GetData(), "updatedpage5");  // NOLINT
  strcpy(page6->GetData(), "updatedpage6");  // NOLINT
  strcpy(page7->GetData(), "updatedpage7");  // NOLINT
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[5], false));
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[6], false));
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[7], false));

  ASSERT_EQ(1, bpm->UnpinPage(page_ids[5], false));
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[6], false));
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[7], false));

  // page5 would be evicted.
  new_page = bpm->NewPage(&temp_page_id);
  ASSERT_NE(nullptr, new_page);
  // page6 would be evicted.
  page5 = bpm->FetchPage(page_ids[5]);
  ASSERT_NE(nullptr, page5);
  ASSERT_EQ(0, std::strcmp("5", (page5->GetData())));
  page7 = bpm->FetchPage(page_ids[7]);
  ASSERT_NE(nullptr, page7);
  ASSERT_EQ(0, std::strcmp("updatedpage7", (page7->GetData())));
  // All pages pinned
  ASSERT_EQ(nullptr, bpm->FetchPage(page_ids[6]));
  bpm->UnpinPage(temp_page_id, false);
  page6 = bpm->FetchPage(page_ids[6]);
  ASSERT_NE(nullptr, page6);
  ASSERT_EQ(0, std::strcmp("6", page6->GetData()));

  strcpy(page6->GetData(), "updatedpage6");  // NOLINT

  // Remove from LRU and update pin_count on fetch
  new_page = bpm->NewPage(&temp_page_id);
  ASSERT_EQ(nullptr, new_page);

  ASSERT_EQ(1, bpm->UnpinPage(page_ids[7], false));
  ASSERT_EQ(1, bpm->UnpinPage(page_ids[6], false));

  new_page = bpm->NewPage(&temp_page_id);
  ASSERT_NE(nullptr, new_page);
  page6 = bpm->FetchPage(page_ids[6]);
  ASSERT_NE(nullptr, page6);
  ASSERT_EQ(0, std::strcmp("updatedpage6", page6->GetData()));
  page7 = bpm->FetchPage(page_ids[7]);
  ASSERT_EQ(nullptr, page7);
  bpm->UnpinPage(temp_page_id, false);
  page7 = bpm->FetchPage(page_ids[7]);
  ASSERT_NE(nullptr, page7);
  ASSERT_EQ(0, std::strcmp("7", (page7->GetData())));

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, DeletePage) {  // NOLINT
  page_id_t temp_page_id;
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(10, disk_manager, 5);

  std::vector<Page *> pages;
  std::vector<page_id_t> page_ids;
  std::vector<std::string> content;

  for (int i = 0; i < 10; ++i) {
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    strcpy(new_page->GetData(), std::to_string(i).c_str());  // NOLINT
    pages.push_back(new_page);
    page_ids.push_back(temp_page_id);
    content.push_back(std::to_string(i));
  }

  for (int i = 0; i < 10; ++i) {
    auto *page = bpm->FetchPage(page_ids[i]);
    ASSERT_NE(nullptr, page);
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
  }

  for (int i = 0; i < 10; ++i) {
    auto *new_page = bpm->NewPage(&temp_page_id);
    ASSERT_NE(nullptr, new_page);
    bpm->UnpinPage(temp_page_id, true);
  }

  for (int i = 0; i < 10; ++i) {
    auto *page = bpm->FetchPage(page_ids[i]);
    ASSERT_NE(nullptr, page);
  }

  auto *new_page = bpm->NewPage(&temp_page_id);
  ASSERT_EQ(nullptr, new_page);

  ASSERT_EQ(0, bpm->DeletePage(page_ids[4]));
  bpm->UnpinPage(4, false);
  ASSERT_EQ(1, bpm->DeletePage(page_ids[4]));

  new_page = bpm->NewPage(&temp_page_id);
  ASSERT_NE(nullptr, new_page);
  ASSERT_NE(nullptr, new_page);

  auto *page5 = bpm->FetchPage(page_ids[5]);
  ASSERT_NE(nullptr, page5);
  auto *page6 = bpm->FetchPage(page_ids[6]);
  ASSERT_NE(nullptr, page6);
  auto *page7 = bpm->FetchPage(page_ids[7]);
  ASSERT_NE(nullptr, page7);
  strcpy(page5->GetData(), "updatedpage5");  // NOLINT
  strcpy(page6->GetData(), "updatedpage6");  // NOLINT
  strcpy(page7->GetData(), "updatedpage7");  // NOLINT
  bpm->UnpinPage(5, false);
  bpm->UnpinPage(6, false);
  bpm->UnpinPage(7, false);

  bpm->UnpinPage(5, false);
  bpm->UnpinPage(6, false);
  bpm->UnpinPage(7, false);
  ASSERT_EQ(1, bpm->DeletePage(page_ids[7]));

  bpm->NewPage(&temp_page_id);
  page5 = bpm->FetchPage(page_ids[5]);
  page6 = bpm->FetchPage(page_ids[6]);
  ASSERT_NE(nullptr, page5);
  ASSERT_NE(nullptr, page6);
  ASSERT_EQ(0, std::strcmp(page5->GetData(), "updatedpage5"));
  ASSERT_EQ(0, std::strcmp(page6->GetData(), "updatedpage6"));

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, IsDirty) {  // NOLINT
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(1, disk_manager, 5);

  // Make new page and write to it
  page_id_t pageid0;
  auto *page0 = bpm->NewPage(&pageid0);
  ASSERT_NE(nullptr, page0);
  ASSERT_EQ(0, page0->IsDirty());
  strcpy(page0->GetData(), "page0");  // NOLINT
  ASSERT_EQ(1, bpm->UnpinPage(pageid0, true));

  // Fetch again but don't write. Assert it is still marked as dirty
  page0 = bpm->FetchPage(pageid0);
  ASSERT_NE(nullptr, page0);
  ASSERT_EQ(1, page0->IsDirty());
  ASSERT_EQ(1, bpm->UnpinPage(pageid0, false));

  // Fetch and assert it is still dirty
  page0 = bpm->FetchPage(pageid0);
  ASSERT_NE(nullptr, page0);
  ASSERT_EQ(1, page0->IsDirty());
  ASSERT_EQ(1, bpm->UnpinPage(pageid0, false));

  // Create a new page, assert it's not dirty
  page_id_t pageid1;
  auto *page1 = bpm->NewPage(&pageid1);
  ASSERT_NE(nullptr, page1);
  ASSERT_EQ(0, page1->IsDirty());

  // Write to the page, and then delete it
  strcpy(page1->GetData(), "page1");  // NOLINT
  ASSERT_EQ(1, bpm->UnpinPage(pageid1, true));
  ASSERT_EQ(1, page1->IsDirty());
  ASSERT_EQ(1, bpm->DeletePage(pageid1));

  // Fetch page 0 again, and confirm its not dirty
  page0 = bpm->FetchPage(pageid0);
  ASSERT_NE(nullptr, page0);
  ASSERT_EQ(0, page0->IsDirty());

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, ConcurrencyTest) {  // NOLINT
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    auto *disk_manager = new DiskManager("test.db");
    std::shared_ptr<BufferPoolManagerInstance> bpm{new BufferPoolManagerInstance(50, disk_manager)};
    std::vector<std::thread> threads;

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([&bpm]() {  // NOLINT
        page_id_t temp_page_id;
        std::vector<page_id_t> page_ids;
        for (int i = 0; i < 10; i++) {
          auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
          ASSERT_NE(nullptr, new_page);
          strcpy(new_page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
          page_ids.push_back(temp_page_id);
        }
        for (int i = 0; i < 10; i++) {
          ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
        }
        for (int j = 0; j < 10; j++) {
          auto *page = bpm->FetchPage(page_ids[j], nullptr);
          ASSERT_NE(nullptr, page);
          ASSERT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
          ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], true, nullptr));
        }
        for (int j = 0; j < 10; j++) {
          ASSERT_EQ(1, bpm->DeletePage(page_ids[j], nullptr));
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    remove("test.db");
    remove("test.log");
    delete disk_manager;
  }
}

TEST(BufferPoolManagerInstanceTest, IntegratedTest) {  // NOLINT
  page_id_t temp_page_id;
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(10, disk_manager, 5);

  std::vector<page_id_t> page_ids;
  for (int j = 0; j < 1000; j++) {
    for (int i = 0; i < 10; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id);
      ASSERT_NE(nullptr, new_page);
      strcpy(new_page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
      page_ids.push_back(temp_page_id);
    }
    for (unsigned int i = page_ids.size() - 10; i < page_ids.size(); i++) {
      ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    }
  }

  for (int j = 0; j < 10000; j++) {
    auto *page = bpm->FetchPage(page_ids[j]);
    ASSERT_NE(nullptr, page);
    ASSERT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], true));
    page_ids.push_back(temp_page_id);
  }
  for (int j = 0; j < 10000; j++) {
    ASSERT_EQ(1, bpm->DeletePage(page_ids[j]));
  }
  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, HardTest_1) {  // NOLINT
  page_id_t temp_page_id;
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(10, disk_manager, 5);

  std::vector<page_id_t> page_ids;
  for (int j = 0; j < 1000; j++) {
    for (int i = 0; i < 10; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id);
      ASSERT_NE(nullptr, new_page);
      strcpy(new_page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
      page_ids.push_back(temp_page_id);
    }
    for (unsigned int i = page_ids.size() - 10; i < page_ids.size() - 5; i++) {
      ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false));
    }
    for (unsigned int i = page_ids.size() - 5; i < page_ids.size(); i++) {
      ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true));
    }
  }

  for (int j = 0; j < 10000; j++) {
    auto *page = bpm->FetchPage(page_ids[j]);
    ASSERT_NE(nullptr, page);
    if (j % 10 < 5) {
      ASSERT_NE(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    } else {
      ASSERT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    }
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], true));
  }

  auto rng = std::default_random_engine{};
  std::shuffle(page_ids.begin(), page_ids.end(), rng);

  for (int j = 0; j < 5000; j++) {
    auto *page = bpm->FetchPage(page_ids[j]);
    ASSERT_NE(nullptr, page);
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false));
    ASSERT_EQ(1, bpm->DeletePage(page_ids[j]));
  }

  for (int j = 5000; j < 10000; j++) {
    auto *page = bpm->FetchPage(page_ids[j]);
    ASSERT_NE(nullptr, page);
    if (page_ids[j] % 10 < 5) {
      ASSERT_NE(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    } else {
      ASSERT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
    }
    ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false));
    ASSERT_EQ(1, bpm->DeletePage(page_ids[j]));
  }

  remove("test.db");
  remove("test.log");
  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerInstanceTest, HardTest_2) {  // NOLINT
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    auto *disk_manager = new DiskManager("test.db");
    std::shared_ptr<BufferPoolManagerInstance> bpm{new BufferPoolManagerInstance(50, disk_manager)};
    std::vector<std::thread> threads;

    page_id_t temp_page_id;
    std::vector<page_id_t> page_ids;
    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      ASSERT_NE(nullptr, new_page);
      strcpy(new_page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
      page_ids.push_back(temp_page_id);
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
      } else {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false, nullptr));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      ASSERT_NE(nullptr, new_page);
      ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
    }

    for (int j = 0; j < 50; j++) {
      auto *page = bpm->FetchPage(page_ids[j], nullptr);
      ASSERT_NE(nullptr, page);
      strcpy(page->GetData(), (std::string("Hard") + std::to_string(page_ids[j])).c_str());  // NOLINT
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false, nullptr));
      } else {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      ASSERT_NE(nullptr, new_page);
      ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
    }

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([&bpm, tid, page_ids]() {  // NOLINT
        int j = (tid * 10);
        while (j < 50) {
          auto *page = bpm->FetchPage(page_ids[j], nullptr);
          while (page == nullptr) {
            page = bpm->FetchPage(page_ids[j], nullptr);
          }
          ASSERT_NE(nullptr, page);
          if (j % 2 == 0) {
            ASSERT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false, nullptr));
          } else {
            ASSERT_EQ(0, std::strcmp((std::string("Hard") + std::to_string(page_ids[j])).c_str(), (page->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false, nullptr));
          }
          j = (j + 1);
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (int j = 0; j < 50; j++) {
      ASSERT_EQ(1, bpm->DeletePage(page_ids[j], nullptr));
    }

    remove("test.db");
    remove("test.log");
    delete disk_manager;
  }
}

TEST(BufferPoolManagerInstanceTest, HardTest_3) {  // NOLINT
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    auto *disk_manager = new DiskManager("test.db");
    std::shared_ptr<BufferPoolManagerInstance> bpm{new BufferPoolManagerInstance(50, disk_manager)};
    std::vector<std::thread> threads;

    page_id_t temp_page_id;
    std::vector<page_id_t> page_ids;
    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      ASSERT_NE(nullptr, new_page);
      strcpy(new_page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
      page_ids.push_back(temp_page_id);
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
      } else {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false, nullptr));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      ASSERT_NE(nullptr, new_page);
      ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
    }

    for (int j = 0; j < 50; j++) {
      auto *page = bpm->FetchPage(page_ids[j], nullptr);
      ASSERT_NE(nullptr, page);
      strcpy(page->GetData(), (std::string("Hard") + std::to_string(page_ids[j])).c_str());  // NOLINT
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false, nullptr));
      } else {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      ASSERT_NE(nullptr, new_page);
      ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
    }

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([&bpm, tid, page_ids]() {  // NOLINT
        page_id_t temp_page_id;
        int j = (tid * 10);
        while (j < 50) {
          if (j != tid * 10) {
            auto *page_local = bpm->FetchPage(temp_page_id, nullptr);
            while (page_local == nullptr) {
              page_local = bpm->FetchPage(temp_page_id, nullptr);
            }
            ASSERT_NE(nullptr, page_local);
            ASSERT_EQ(0, std::strcmp(std::to_string(temp_page_id).c_str(), (page_local->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, false, nullptr));
            // If the page is still in buffer pool then put it in free list,
            // else also we are happy
            ASSERT_EQ(1, bpm->DeletePage(temp_page_id, nullptr));
          }

          auto *page = bpm->FetchPage(page_ids[j], nullptr);
          while (page == nullptr) {
            page = bpm->FetchPage(page_ids[j], nullptr);
          }
          ASSERT_NE(nullptr, page);
          if (j % 2 == 0) {
            ASSERT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false, nullptr));
          } else {
            ASSERT_EQ(0, std::strcmp((std::string("Hard") + std::to_string(page_ids[j])).c_str(), (page->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false, nullptr));
          }
          j = (j + 1);

          page = bpm->NewPage(&temp_page_id, nullptr);
          while (page == nullptr) {
            page = bpm->NewPage(&temp_page_id, nullptr);
          }
          ASSERT_NE(nullptr, page);
          strcpy(page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
          ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (int j = 0; j < 50; j++) {
      ASSERT_EQ(1, bpm->DeletePage(page_ids[j], nullptr));
    }

    remove("test.db");
    remove("test.log");
    delete disk_manager;
  }
}

TEST(BufferPoolManagerInstanceTest, HardTest_4) {  // NOLINT
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    auto *disk_manager = new DiskManager("test.db");
    std::shared_ptr<BufferPoolManagerInstance> bpm{new BufferPoolManagerInstance(50, disk_manager)};
    std::vector<std::thread> threads;

    page_id_t temp_page_id;
    std::vector<page_id_t> page_ids;
    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      ASSERT_NE(nullptr, new_page);
      ASSERT_NE(nullptr, new_page);
      strcpy(new_page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
      page_ids.push_back(temp_page_id);
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
      } else {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false, nullptr));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      ASSERT_NE(nullptr, new_page);
      ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
    }

    for (int j = 0; j < 50; j++) {
      auto *page = bpm->FetchPage(page_ids[j], nullptr);
      ASSERT_NE(nullptr, page);
      strcpy(page->GetData(), (std::string("Hard") + std::to_string(page_ids[j])).c_str());  // NOLINT
    }

    for (int i = 0; i < 50; i++) {
      if (i % 2 == 0) {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], false, nullptr));
      } else {
        ASSERT_EQ(1, bpm->UnpinPage(page_ids[i], true, nullptr));
      }
    }

    for (int i = 0; i < 50; i++) {
      auto *new_page = bpm->NewPage(&temp_page_id, nullptr);
      ASSERT_NE(nullptr, new_page);
      ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, true, nullptr));
    }

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([&bpm, tid, page_ids]() {  // NOLINT
        page_id_t temp_page_id;
        int j = (tid * 10);
        while (j < 50) {
          if (j != tid * 10) {
            auto *page_local = bpm->FetchPage(temp_page_id, nullptr);
            while (page_local == nullptr) {
              page_local = bpm->FetchPage(temp_page_id, nullptr);
            }
            ASSERT_NE(nullptr, page_local);
            ASSERT_EQ(0, std::strcmp(std::to_string(temp_page_id).c_str(), (page_local->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, false, nullptr));
            // If the page is still in buffer pool then put it in free list,
            // else also we are happy
            ASSERT_EQ(1, bpm->DeletePage(temp_page_id, nullptr));
          }

          auto *page = bpm->FetchPage(page_ids[j], nullptr);
          while (page == nullptr) {
            page = bpm->FetchPage(page_ids[j], nullptr);
          }
          ASSERT_NE(nullptr, page);
          if (j % 2 == 0) {
            ASSERT_EQ(0, std::strcmp(std::to_string(page_ids[j]).c_str(), (page->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false, nullptr));
          } else {
            ASSERT_EQ(0, std::strcmp((std::string("Hard") + std::to_string(page_ids[j])).c_str(), (page->GetData())));
            ASSERT_EQ(1, bpm->UnpinPage(page_ids[j], false, nullptr));
          }
          j = (j + 1);

          page = bpm->NewPage(&temp_page_id, nullptr);
          while (page == nullptr) {
            page = bpm->NewPage(&temp_page_id, nullptr);
          }
          ASSERT_NE(nullptr, page);
          strcpy(page->GetData(), std::to_string(temp_page_id).c_str());  // NOLINT
          // FLush page instead of unpining with true
          ASSERT_EQ(1, bpm->FlushPage(temp_page_id, nullptr));
          ASSERT_EQ(1, bpm->UnpinPage(temp_page_id, false, nullptr));

          // Flood with new pages
          for (int k = 0; k < 10; k++) {
            page_id_t flood_page_id;
            auto *flood_page = bpm->NewPage(&flood_page_id, nullptr);
            while (flood_page == nullptr) {
              flood_page = bpm->NewPage(&flood_page_id, nullptr);
            }
            ASSERT_NE(nullptr, flood_page);
            ASSERT_EQ(1, bpm->UnpinPage(flood_page_id, false, nullptr));
            // If the page is still in buffer pool then put it in free list,
            // else also we are happy
            ASSERT_EQ(1, bpm->DeletePage(flood_page_id, nullptr));
          }
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (int j = 0; j < 50; j++) {
      ASSERT_EQ(1, bpm->DeletePage(page_ids[j], nullptr));
    }

    remove("test.db");
    remove("test.log");
    delete disk_manager;
  }
}

}  // namespace bustub
