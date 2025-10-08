//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new internal page.
 *
 * Writes the necessary header information to a newly created page,
 * including set page type, set current size, set page id, set parent id and set max page size,
 * must be called after the creation of a new page to make a valid BPlusTreeInternalPage.
 *
 * @param max_size Maximal size of the page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) { 
  SetPageType(bustub::IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  return;
  UNIMPLEMENTED("TODO(P2): Add implementation.");
 }

/**
 * @brief Helper method to get/set the key associated with input "index"(a.k.a
 * array offset).
 *
 * @param index The index of the key to get. Index must be non-zero.
 * @return Key at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  std::cout << "In the Internal the key at " << index << " is " << key_array_[index] << std::endl;
  return key_array_[index];
  UNIMPLEMENTED("TODO(P2): Add implementation.");
}

/**
 * @brief Set key at the specified index.
 *
 * @param index The index of the key to set. Index must be non-zero.
 * @param key The new value for key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  key_array_[index] = key;
  return ;
  UNIMPLEMENTED("TODO(P2): Add implementation.");
}

/**
 * @brief Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 *
 * @param index The index of the value to get.
 * @return Value at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return page_id_array_[index];
  UNIMPLEMENTED("TODO(P2): Add implementation.");
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE:: Addpair(const KeyType &key,const ValueType &left,const ValueType &right,int insert_pos) ->bool{
  //add the pair to the key_array and the rid_array and sort key  increasingly 
  int size = GetSize();
  for(int i = size + 1; i> insert_pos;i--){
    key_array_[i] = key_array_[i-1];
    page_id_array_[i] = page_id_array_[i-1];
  }
  key_array_[insert_pos] = key;
  page_id_array_[insert_pos-1] = left;
  page_id_array_[insert_pos] = right;
  SetSize(size+1);
  std::cout <<"Internal::add a new key at pos::  "<<insert_pos <<"the key is::   " << key << std::endl;
  printf("The size is %d\n",size+1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE:: Deletepair(int delete_pos) ->bool{
  //add the pair to the key_array and the rid_array and sort key  increasingly 
  int size = GetSize();
  std::cout <<"Internal::delete a  key at pos::  "<<delete_pos <<"the key is::   " << key_array_[delete_pos] << std::endl;

  for(int i = std::max(delete_pos,1); i< size;i++){
    key_array_[i] = key_array_[i+1];
  }
  for(int i = delete_pos; i< size;i++){
    page_id_array_[i] = page_id_array_[i+1];
  }
  SetSize(size - 1);
  printf("The size is %d\n",size-1);
  return true;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
