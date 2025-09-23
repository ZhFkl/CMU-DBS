//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE:: IndexIterator(BPlusTreeLeafPage<KeyType,ValueType,KeyComparator> *leaf_page,int index,BufferPoolManager *bpm)
{
  bpm_ = bpm;
  leaf_page_ = leaf_page;
  index_ = index;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
   return (index_ == leaf_page_->GetSize())&& (leaf_page_->GetNextPageId() == INVALID_PAGE_ID);

  UNIMPLEMENTED("TODO(P2): Add implementation."); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &,const  ValueType &> {
  std::cout << " the next page_id of this  leaf is:: " << leaf_page_->GetNextPageId()<<std::endl;
  return {leaf_page_->KeyAt(index_),leaf_page_->ValueAt(index_)};
  
  UNIMPLEMENTED("TODO(P2): Add implementation.");
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // first the index is not to the size
  if(index_ < leaf_page_->GetSize() -1){
   index_++;
   std::cout<<"the index is "<<index_<<std::endl;
  }else{
    page_id_t next_page_id = leaf_page_->GetNextPageId();
    if(next_page_id != INVALID_PAGE_ID){
      WritePageGuard next_page_guard = bpm_->WritePage(next_page_id);
      auto  next_page = next_page_guard.AsMut<LeafPage> ();
      std::cout<<"go into the next page:: "<<leaf_page_->GetNextPageId()<<std::endl;
      leaf_page_ = next_page;
      index_ = 0;
      std::cout<<"the index is ::"<< index_<<std::endl;
    }else{
      index_++;
    }
  }
  return *this;

  UNIMPLEMENTED("TODO(P2): Add implementation."); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
