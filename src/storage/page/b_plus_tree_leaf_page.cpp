//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(bustub::IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
  prev_page_id_ = INVALID_PAGE_ID;
  return;
  UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_; 
  UNIMPLEMENTED("TODO(P2): Add implementation."); }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  return ;
   UNIMPLEMENTED("TODO(P2): Add implementation.");
}

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index)  const-> const KeyType & {
  //first of all we need to judge if the index is out of range  
 // std::cout << "In the leaf the key at " << index << " is " << key_array_[index] << std::endl;
  return key_array_[index];
  UNIMPLEMENTED("TODO(P2): Add implementation."); 
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const-> const ValueType &{
  // first we need to judge if the index is out of range
  return rid_array_[index];
  UNIMPLEMENTED("TODO(P2): Add implementation.");
}
// add the pair can only by the bplus tree 


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE:: Addpair(const KeyType &key,const ValueType &value,int insert_pos) ->bool{
  //add the pair to the key_array and the rid_array and sort key  increasingly 
  int size = GetSize();
  for(int i = size; i> insert_pos;i--){
    key_array_[i] = key_array_[i-1];
    rid_array_[i] = rid_array_[i-1];
  }
  key_array_[insert_pos] = key;
  rid_array_[insert_pos] = value;
  SetSize(size+1);
  std::cout <<"LeafPage::add a new key at pos::  "<<insert_pos <<"the key is::   " << key << std::endl;
  printf("The size is %d\n",size+1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE:: Deletepair(int delete_pos) ->bool{
  //add the pair to the key_array and the rid_array and sort key  increasingly 
  int size = GetSize();
  std::cout <<"LeafPage::delete a  key at pos::  "<<delete_pos <<"the key is::   " << key_array_[delete_pos] << std::endl;
  for(int i = delete_pos; i<size -1;i++){
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i+1];
  }
  SetSize(size-1);
  printf("Then size is %d\n",size-1);
  return true;
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
