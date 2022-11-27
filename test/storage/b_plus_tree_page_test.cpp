#include <algorithm>
#include <cstdio>

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

TEST(BPlusTreePageTests, InternalPageTest) {
  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);

  int page_id;
  Page *new_page = bpm->NewPage(&page_id);
  auto page = reinterpret_cast<BPlusTreeInternalPage<int, int, std::less<>> *>(new_page);
  page->Init(1, 2);
  EXPECT_EQ(0, page->GetSize());

  // x 1 2 3 4 5 6 7 8 9//
  for (int i = 0; i < 10; ++i) {
    page->InsertAndIncrease(i, i, i);
    EXPECT_EQ(i, page->ValueAt(i));
  }
  EXPECT_EQ(10, page->GetSize());

  int index = -1;
  index = page->LowerBound(1, std::less<>());
  EXPECT_EQ(1, index);

  index = page->LowerBound(15, std::less<>());
  EXPECT_EQ(10, index);

  // x 0 1 2 3 4 5 6 7 8 9//
  page->InsertAndIncrease(0, -1, -1);
  EXPECT_EQ(-1, page->ValueAt(0));

  for (int i = 1; i < 11; ++i) {
    EXPECT_EQ(i - 1, page->ValueAt(i));
  }

  bpm->UnpinPage(page_id, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
}

TEST(BPlusTreePageTests, LeafPageTest) {
  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);

  int page_id;
  Page *new_page = bpm->NewPage(&page_id);
  auto page = reinterpret_cast<BPlusTreeLeafPage<int, int, std::less<>> *>(new_page);
  page->Init(1, 2, 10);
  EXPECT_EQ(0, page->GetSize());

  // 0 //
  page->InsertAndIncrease(page->GetSize(), 0, 0);
  EXPECT_EQ(0, page->KeyAt(0));
  EXPECT_EQ(1, page->GetSize());

  // 0 1//
  page->InsertAndIncrease(page->GetSize(), 1, 1);
  EXPECT_EQ(1, page->KeyAt(1));
  EXPECT_EQ(2, page->GetSize());

  // -1 0 1 //
  page->InsertAndIncrease(0, -1, -1);
  EXPECT_EQ(-1, page->KeyAt(0));
  EXPECT_EQ(0, page->KeyAt(1));
  EXPECT_EQ(1, page->KeyAt(2));
  EXPECT_EQ(3, page->GetSize());

  int index = -1;
  index = page->LowerBound(0, std::less<>());
  EXPECT_EQ(1, index);
  index = page->LowerBound(4, std::less<>());
  EXPECT_EQ(3, index);

  // -1 0 1 4 //
  page->InsertAndIncrease(index, 4, 4);
  EXPECT_EQ(4, page->KeyAt(index));
  EXPECT_EQ(4, page->GetSize());

  bpm->UnpinPage(page_id, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  // page.Insert(page.GetSize(), 1, 1);
}

}  // namespace bustub
