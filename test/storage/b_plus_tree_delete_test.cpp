//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_delete_test.cpp
//
// Identification: test/storage/b_plus_tree_delete_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <vector>
#include <iostream>  // 需包含 iostream 头文件

// 关键：添加 vector<int64_t> 的 operator<< 重载
std::ostream& operator<<(std::ostream& os, const std::vector<int64_t>& vec) {
  os << "[";
  for (size_t i = 0; i < vec.size(); ++i) {
    os << vec[i];
    if (i != vec.size() - 1) {
      os << ", ";
    }
  }
  os << "]";
  return os;
}

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/b_plus_tree_utils.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, DeleteTestNoIterator) {
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

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
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

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key);
  }

  int64_t size = 0;
  bool is_present;
  index_key.SetFromInteger(2);
  rids.clear();
  is_present = tree.GetValue(index_key, &rids);
  if(is_present){
    printf("the key 2 is present\n");
  }


  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      ++size;
    }
  }
  EXPECT_EQ(size, 1);

  // Remove the remaining key
  index_key.SetFromInteger(2);
  tree.Remove(index_key);
  auto root_page_id = tree.GetRootPageId();
  ASSERT_EQ(root_page_id, INVALID_PAGE_ID);

  delete bpm;
}

TEST(BPlusTreeTests, SequentialEdgeMixTest) {  // NOLINT
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(200, disk_manager.get());

  for (int leaf_max_size = 2; leaf_max_size <= 5; leaf_max_size++) {
    // create and fetch header_page
    page_id_t page_id = bpm->NewPage();

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, leaf_max_size, 2);
    GenericKey<8> index_key;
    RID rid;

    std::vector<int64_t> keys = {1, 5, 15, 20, 25, 2, -1, -2, 6, 14, 4,  10, 11, 12, 13};
    std::vector<int64_t> inserted = {};
    std::vector<int64_t> deleted = {};
    for (auto key : keys) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree.Insert(index_key, rid);
      inserted.push_back(key);
      auto res = TreeValuesMatch<GenericKey<8>, RID, GenericComparator<8>>(tree, inserted, deleted);
      ASSERT_TRUE(res);
    }

    index_key.SetFromInteger(1);
    tree.Remove(index_key);
    deleted.push_back(1);
    inserted.erase(std::find(inserted.begin(), inserted.end(), 1));
    auto res = TreeValuesMatch<GenericKey<8>, RID, GenericComparator<8>>(tree, inserted, deleted);
    ASSERT_TRUE(res);

    index_key.SetFromInteger(3);
    rid.Set(3, 3);
    tree.Insert(index_key, rid);
    inserted.push_back(3);
    res = TreeValuesMatch<GenericKey<8>, RID, GenericComparator<8>>(tree, inserted, deleted);
    ASSERT_TRUE(res);

    keys = {4, 14, 6, 2, 15, -2, -1, 3, 5, 25, 20, 11, 10, 12, 13};
    for (auto key : keys) {
      index_key.SetFromInteger(key);
      tree.Remove(index_key);
      deleted.push_back(key);
      inserted.erase(std::find(inserted.begin(), inserted.end(), key));
      res = TreeValuesMatch<GenericKey<8>, RID, GenericComparator<8>>(tree, inserted, deleted);
      ASSERT_TRUE(res);
    }
  }

  delete bpm;
}




// 辅助函数：从 GenericKey<8> 解析 int64_t（根据项目实际编码逻辑调整，如字节序）
template <size_t KeySize>
int64_t GenericKeyToInteger(const GenericKey<KeySize> &key) {
  int64_t result;
  // 假设 GenericKey 的 data_ 直接存储 int64_t（若为大端/小端，需补充字节序转换）
  std::memcpy(&result, key.data_, sizeof(int64_t));
  return result;
}



// 测试4：删除不存在的键（容错性，不破坏树结构）
TEST(BPlusTreeDeleteTests, DeleteNonExistentKeyTest) {  // NOLINT
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  const int leaf_max_size = 2;
  page_id_t header_page_id = bpm->NewPage();
  ASSERT_NE(header_page_id, INVALID_PAGE_ID) << "创建头页失败";

  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", header_page_id, bpm, comparator, leaf_max_size, 3);

  GenericKey<8> index_key;
  RID rid;
  std::vector<int64_t> valid_keys = {10, 20};  // 仅插入2个有效键

  // 1. 插入有效键
  for (auto key : valid_keys) {
    rid.Set(0, static_cast<uint32_t>(key));
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }

  // 2. 尝试删除不存在的键（30、40、50）
  std::vector<int64_t> invalid_keys = {30, 40, 50};
  for (auto key : invalid_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key);  // 无崩溃、无异常即通过
  }

  // 3. 验证原有有效键仍存在（树结构未破坏）
  for (auto key : valid_keys) {
    std::vector<RID> rids;
    index_key.SetFromInteger(key);
    ASSERT_TRUE(tree.GetValue(index_key, &rids)) 
        << "删除不存在的键后，有效键" << key << "丢失";
    ASSERT_EQ(rids[0].GetSlotNum(), key) << "有效键" << key << "的RID错误";
  }

  delete bpm;
}

// 测试5：批量删除所有键（树为空，根节点设为INVALID）
TEST(BPlusTreeDeleteTests, BatchDeleteAllKeysTest) {  // NOLINT
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  const int leaf_max_size = 3;
  page_id_t header_page_id = bpm->NewPage();
  ASSERT_NE(header_page_id, INVALID_PAGE_ID) << "创建头页失败";

  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", header_page_id, bpm, comparator, leaf_max_size, 3);

  GenericKey<8> index_key;
  RID rid;
  std::vector<int64_t> all_keys = {5, 15, 25, 35};  // 所有要插入的键

  // 1. 插入所有键
  for (auto key : all_keys) {
    rid.Set(0, static_cast<uint32_t>(key));
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }

  // 2. 批量删除所有键
  for (auto key : all_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key);
  }

  // 3. 验证树为空（根节点ID为INVALID）
  ASSERT_EQ(tree.GetRootPageId(), INVALID_PAGE_ID) << "删除所有键后，树未清空（根节点非INVALID）";

  // 4. 验证所有键均不存在
  for (auto key : all_keys) {
    std::vector<RID> rids;
    index_key.SetFromInteger(key);
    ASSERT_FALSE(tree.GetValue(index_key, &rids)) << "键" << key << "未被删除";
  }

  delete bpm;
}

// 测试6：删除叶子中间键（不触发合并，仅内部调整）
TEST(BPlusTreeDeleteTests, DeleteMiddleKeyNoMergeTest) {  // NOLINT
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  const int leaf_max_size = 4;  // 最小size=2（ceil(4/2)）
  page_id_t header_page_id = bpm->NewPage();
  ASSERT_NE(header_page_id, INVALID_PAGE_ID) << "创建头页失败";

  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", header_page_id, bpm, comparator, leaf_max_size, 3);

  GenericKey<8> index_key;
  RID rid;
  std::vector<int64_t> insert_keys = {1, 2, 3, 4, 5};  // 分2个叶子：[1,2,3,4]、[5]
  std::vector<int64_t> inserted = insert_keys;

  // 1. 插入所有键
  for (auto key : insert_keys) {
    rid.Set(0, static_cast<uint32_t>(key));
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }

  // 2. 删除叶子1的中间键（3）→ 叶子1变为[1,2,4]，size=3≥2，不触发合并
  int64_t delete_key = 3;
  index_key.SetFromInteger(delete_key);
  tree.Remove(index_key);
  inserted.erase(std::find(inserted.begin(), inserted.end(), delete_key));

  // 3. 验证叶子1的键有序性（[1,2,4]）
  page_id_t root_id = tree.GetRootPageId();
  ReadPageGuard root_guard = bpm->ReadPage(root_id);
  auto *root_page = reinterpret_cast<const BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(
      root_guard.GetData());

  // 获取第一个叶子节点的ID（根节点的第一个子节点）
  page_id_t leaf1_id = root_page->ValueAt(1);  // ValueAt返回子节点页ID

  // 读取叶子1的数据
  ReadPageGuard leaf1_guard = bpm->ReadPage(leaf1_id);
  auto *leaf1_page = reinterpret_cast<const BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(
      leaf1_guard.GetData());

  // 收集叶子1的键并验证
  std::vector<int64_t> actual_leaf_keys;
  for (int i = 0; i < leaf1_page->GetSize(); ++i) {
    GenericKey<8> key = leaf1_page->KeyAt(i);
    actual_leaf_keys.push_back(GenericKeyToInteger(key));
  }
  std::vector<int64_t> expected_leaf_keys = {4,5};
  ASSERT_EQ(actual_leaf_keys, expected_leaf_keys) 
      << "删除中间键后，叶子节点键无序（预期" << expected_leaf_keys << "，实际" << actual_leaf_keys << "）";

  delete bpm;
}

// 测试7：重复删除同一键（幂等性，多次删除与单次效果一致）
TEST(BPlusTreeDeleteTests, RepeatDeleteSameKeyTest) {  // NOLINT
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  const int leaf_max_size = 2;
  page_id_t header_page_id = bpm->NewPage();
  ASSERT_NE(header_page_id, INVALID_PAGE_ID) << "创建头页失败";

  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", header_page_id, bpm, comparator, leaf_max_size, 3);

  GenericKey<8> index_key;
  RID rid;
  int64_t target_key = 100;  // 待删除的目标键

  // 1. 插入目标键
  rid.Set(0, static_cast<uint32_t>(target_key));
  index_key.SetFromInteger(target_key);
  tree.Insert(index_key, rid);

  // 2. 第一次删除：键应被移除
  tree.Remove(index_key);
  std::vector<RID> rids;
  ASSERT_FALSE(tree.GetValue(index_key, &rids)) << "第一次删除后，键" << target_key << "仍存在";

  // 3. 第二次删除：无崩溃，键仍不存在
  tree.Remove(index_key);
  ASSERT_FALSE(tree.GetValue(index_key, &rids)) << "第二次删除后，键" << target_key << "异常存在";

  // 4. 第三次删除：同样无崩溃，键仍不存在
  tree.Remove(index_key);
  ASSERT_FALSE(tree.GetValue(index_key, &rids)) << "第三次删除后，键" << target_key << "异常存在";

  delete bpm;
}
}  // namespace bustub
