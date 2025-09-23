//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_insert_test.cpp
//
// Identification: test/storage/b_plus_tree_insert_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT
#include <unordered_set>  // 新增：用于std::unordered_set
#include <random>         // 新增：用于随机数生成

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, BasicInsertTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  int64_t key = 42;
  int64_t value = key & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(key), value);
  index_key.SetFromInteger(key);
  tree.Insert(index_key, rid);

  auto root_page_id = tree.GetRootPageId();
  auto root_page_guard = bpm->ReadPage(root_page_id);
  auto root_page = root_page_guard.As<BPlusTreePage>();
  ASSERT_NE(root_page, nullptr);
  ASSERT_TRUE(root_page->IsLeafPage());

  auto root_as_leaf = root_page_guard.As<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>>();
  ASSERT_EQ(root_as_leaf->GetSize(), 1);
  ASSERT_EQ(comparator(root_as_leaf->KeyAt(0), index_key), 0);

  delete bpm;
}

TEST(BPlusTreeTests, InsertTest1NoIterator) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  std::vector<int64_t> keys = {};
  for(int i = 30 ; i >0 ;i--){
    keys.emplace_back(i);
  }/*
  for(int i = 1 ; i < 30 ;i++){
    keys.emplace_back(i);
  }*/

  for (auto key : keys) {
    printf("the key is %ld \n",key);
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }

  bool is_present;
  std::vector<RID> rids;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  delete bpm;
}

TEST(BPlusTreeTests, InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
    auto pair = *iter;
    auto location = pair.second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  start_key = 3;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.IsEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }
  delete bpm;
}


// 测试边界值插入
TEST (BPlusTreeTests, InsertBoundaryValuesTest) {
auto key_schema = ParseCreateStatement ("a bigint");
GenericComparator<8> comparator (key_schema.get ());

auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
auto *bpm = new BufferPoolManager(50, disk_manager.get());
page_id_t page_id = bpm->NewPage();
BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("boundary_test", page_id, bpm, comparator, 2, 2);

GenericKey<8> index_key;
RID rid;

// 插入边界值
std::vector<int64_t> keys = {0, 1, INT64_MAX, INT64_MIN, -1};
for (auto key : keys) {
index_key.SetFromInteger (key);
rid.Set (static_cast<int32_t>(key % 100), static_cast<int32_t>(key % 1000));
EXPECT_TRUE (tree.Insert (index_key, rid));
}

// 验证所有边界值都能正确查询
for (auto key : keys) {
index_key.SetFromInteger (key);
std::vector<RID> rids;
tree.GetValue(index_key, &rids);
EXPECT_EQ(rids.size(), 1);
}

delete bpm;
}

// 测试大量随机插入（修复迭代器访问方式）
TEST (BPlusTreeTests, InsertRandomLargeTest) {
auto key_schema = ParseCreateStatement ("a bigint");
GenericComparator<8> comparator (key_schema.get ());

auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
auto *bpm = new BufferPoolManager(50, disk_manager.get());
page_id_t page_id = bpm->NewPage();
BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("large_random_test", page_id, bpm, comparator, 4, 5);

GenericKey<8> index_key;
RID rid;
const int TEST_SIZE = 1000;
std::unordered_set<int64_t> inserted_keys;

// 随机插入 1000 个不同的键
std::random_device rd;
std::mt19937_64 gen (rd ());
std::uniform_int_distribution<int64_t> dist (INT64_MIN, INT64_MAX);

for (int i = 0; i < TEST_SIZE; ++i) {
int64_t key;
// 确保键唯一
do {
key = dist (gen);
} while (inserted_keys.count (key) > 0);

inserted_keys.insert(key);
index_key.SetFromInteger(key);
rid.Set(static_cast<int32_t>(i / 100), static_cast<int32_t>(i % 100));
EXPECT_TRUE(tree.Insert(index_key, rid));
}

// 验证所有插入的键都能查询到
for (int64_t key : inserted_keys) {
index_key.SetFromInteger (key);
std::vector<RID> rids;
tree.GetValue(index_key, &rids);
EXPECT_EQ(rids.size(), 1);
}

// 验证迭代器顺序（修复：用 * iter 解引用迭代器，而非 iter->）
auto iter = tree.Begin ();
ASSERT_FALSE (iter.IsEnd ());

// 迭代器是对象，用 * 获取当前元素（键值对），再访问.first
GenericKey<8> prev_key = (*iter).first;
int count = 1;
++iter;

while (iter != tree.End ()) {
// 同样用 * 解引用，获取当前键值对
GenericKey<8> current_key = (*iter).first;
// 使用比较器验证键的有序性（当前键 > 前一个键，比较器返回 < 0）
EXPECT_LT (comparator (prev_key, current_key), 0);
prev_key = current_key;
++iter;
++count;
}

EXPECT_EQ(count, TEST_SIZE);

delete bpm;
}

// 测试分裂场景
TEST (BPlusTreeTests, InsertSplitTest) {
auto key_schema = ParseCreateStatement ("a bigint");
GenericComparator<8> comparator (key_schema.get ());

auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
auto *bpm = new BufferPoolManager (100, disk_manager.get ());
page_id_t page_id = bpm->NewPage ();
// 使用较小的节点大小，更容易触发分裂（internal_max_size=2，leaf_max_size=3）
BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree ("split_test", page_id, bpm, comparator, 2, 2);

GenericKey<8> index_key;
RID rid;
const int TEST_SIZE = 20; // 足够触发多次分裂（叶子 / 内部节点均会分裂）

// 插入有序数据
for (int i = 1; i <= TEST_SIZE; ++i) {
index_key.SetFromInteger (i);
rid.Set (0, i); // PageId 设为 0，SlotNum 设为 i（便于验证）
EXPECT_TRUE (tree.Insert (index_key, rid)) << "插入失败 at i=" << i;
}

// 验证所有数据能正确查询
for (int i = 1; i <= TEST_SIZE; ++i) {
index_key.SetFromInteger (i);
std::vector<RID> rids;
tree.GetValue (index_key, &rids);
EXPECT_EQ (rids.size (), 1) << "查询失败 at i=" << i;
EXPECT_EQ (rids [0].GetSlotNum (), i) << "SlotNum 不匹配 at i=" << i;
}

// 验证范围查询（从 key=5 开始遍历）
index_key.SetFromInteger (1);
auto iter = tree.Begin (index_key);
int current = 1;

while (!iter.IsEnd ()) {
auto pair = *iter; // 迭代器解引用获取键值对
EXPECT_EQ (pair.second.GetSlotNum (), current) << "范围查询顺序错误 at current=" << current;
++iter;
++current;
}

EXPECT_EQ (current, TEST_SIZE + 1) << "范围查询未遍历所有元素";

delete bpm;
}

// 测试空树插入第一个元素
TEST (BPlusTreeTests,InsertFirstElementTest) {
auto key_schema = ParseCreateStatement ("a bigint");
GenericComparator<8> comparator (key_schema.get ());

auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
auto *bpm = new BufferPoolManager(10, disk_manager.get());
page_id_t page_id = bpm->NewPage();
BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("first_element_test", page_id, bpm, comparator, 2, 3);

GenericKey<8> index_key;
RID rid(1, 100); // PageId=1，SlotNum=100
int64_t key = 1000;

// 验证初始时树为空
EXPECT_TRUE (tree.IsEmpty ());

// 插入第一个元素
index_key.SetFromInteger (key);
EXPECT_TRUE (tree.Insert (index_key, rid));

// 验证树不再为空
EXPECT_FALSE (tree.IsEmpty ());

// 验证能查询到第一个元素
std::vector<RID> rids;
tree.GetValue (index_key, &rids);
EXPECT_EQ (rids.size (), 1);
EXPECT_EQ (rids [0].GetPageId (), 1) << "PageId 不匹配";
EXPECT_EQ (rids [0].GetSlotNum (), 100) << "SlotNum 不匹配";

delete bpm;
}

// 测试交替插入不同范围的值
TEST (BPlusTreeTests, InsertAlternatingRangesTest) {
auto key_schema = ParseCreateStatement ("a bigint");
GenericComparator<8> comparator (key_schema.get ());

auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
auto *bpm = new BufferPoolManager(50, disk_manager.get());
page_id_t page_id = bpm->NewPage();
BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("alternating_test", page_id, bpm, comparator, 3, 4);

GenericKey<8> index_key;
RID rid;

// 交替插入高范围和低范围的键（模拟无序插入）
std::vector<int64_t> keys = {100, 1, 200, 2, 300, 3, 400, 4, 500, 5, 600, 6};
for (size_t i = 0; i < keys.size (); ++i) {
index_key.SetFromInteger (keys [i]);
rid.Set (static_cast<int32_t>(i / 10), static_cast<int32_t>(i % 10)); // 按插入顺序设置 PageId/SlotNum
EXPECT_TRUE (tree.Insert (index_key, rid));
}

// 验证所有键能正确查询
for (size_t i = 0; i < keys.size (); ++i) {
index_key.SetFromInteger (keys [i]);
std::vector<RID> rids;
tree.GetValue (index_key, &rids);
EXPECT_EQ (rids.size (), 1) << "查询失败 at key=" << keys [i];
}

// 验证顺序遍历是有序的（修复迭代器访问）
std::vector<int64_t> expected_order = {1, 2, 3, 4, 5, 6, 100, 200, 300, 400, 500, 600};
auto iter = tree.Begin ();
size_t idx = 0;

while (iter != tree.End () && idx < expected_order.size ()) {
auto pair = *iter; // 迭代器解引用获取键值对
GenericKey<8> expected_key;
expected_key.SetFromInteger (expected_order [idx]);
// 使用比较器验证当前键与预期键相等（返回 0）
EXPECT_EQ (comparator (pair.first, expected_key), 0) << "遍历顺序错误 at idx=" << idx;
++iter;
++idx;
}

EXPECT_EQ (idx, expected_order.size ()) << "遍历未覆盖所有预期键";

delete bpm;
}
}  // namespace bustub
