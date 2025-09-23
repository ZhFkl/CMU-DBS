//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
 using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

  // you may define your own constructor based on your member variables
  explicit IndexIterator(BPlusTreeLeafPage<KeyType,ValueType,KeyComparator> *leaf_page,int index,BufferPoolManager *bpm);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &,const  ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return leaf_page_ == itr.leaf_page_ && index_ == itr.index_; 
    UNIMPLEMENTED("TODO(P2): Add implementation."); }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return !(*this == itr); 
    UNIMPLEMENTED("TODO(P2): Add implementation."); }

 private:
  // add your own private member variables here
  BufferPoolManager *bpm_;
  BPlusTreeLeafPage<KeyType,ValueType,KeyComparator> *leaf_page_;
  int index_;
};

}  // namespace bustub
