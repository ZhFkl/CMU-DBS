//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
  printf("the max leaf size is %d\n",leaf_max_size_);
  printf("the max internal size is %d\n",internal_max_size_);
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
   //how to judge a tree is empty ?
   // if this tree has no keys and values,then the tree have no page 
   ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
   auto root_page = guard.As<BPlusTreeHeaderPage>();
   if(root_page->root_page_id_ == INVALID_PAGE_ID){
    return true;
   }
   return false;
  UNIMPLEMENTED("TODO(P2): Add implementation.");
 }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  if(IsEmpty()){ // this tree is a empty tree so we return false directly 
    return false;
  }
  page_id_t root_page_id = GetRootPageId();
  return SearchInternal(root_page_id,key,result);

  // we get the BplusTreePage and 

  // which means this a internalpage and we need to search deeper or we need to recursive way to
  // to search the tree

  // find the root leaf page about the key
  UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  return false;
}
//
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchInternal(const page_id_t page_id,const KeyType &key,std::vector<ValueType> *result) -> bool{
  //Get the internal page and judge if it is a leaf node ,
  ReadPageGuard guard = bpm_->ReadPage(page_id);
  auto *page = guard.As<BPlusTreePage>();
  // if this page is a leaf page
  if(page->IsLeafPage()){
    //std::cout<<"the key we need is " << key.ToString() <<std::endl;
    auto *leaf = guard.As<LeafPage>();
    for(int i = 0; i < leaf->GetSize(); i++){
      if(comparator_(key,leaf->KeyAt(i)) == 0){
        result->push_back(leaf->ValueAt(i));
        return true;
      }
      // we can't find the node 
      if(comparator_(key,leaf->KeyAt(i)) < 0){
        return false;
      }
    }
    return false;
  }
  // this page is a internal page
  else{
    auto *internal = guard.As<InternalPage>();
    for(int i = 1; i < internal->GetSize()+1;i++){
      if(comparator_(key,internal->KeyAt(i)) < 0){
        return SearchInternal(internal->ValueAt(i-1),key,result);
      }
    }
    return SearchInternal(internal->ValueAt(internal->GetSize()),key,result);
    // we cann't find the key and 

  }
  // if this is a leaf node 
  // then we can search the node to check if we can find the corresponding key
  
  //if this node is a Internal node  then we need to find the key is bigger than the key we want and serche Internal again
    return true;
  // we need to search in the internal node until we find the
  
  
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // if a tree is empty then we need to updata the root page id
  if(IsEmpty()){
    //printf("the root is empty\n");
    WritePageGuard guard = bpm_->WritePage(header_page_id_);
    auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
    auto [root,page_id] = Leaf_generator();
    //printf("the root page_id is %d\n",page_id);
    root->Addpair(key,value,0);
    //printf("add successs\n");
    root_page->root_page_id_ = page_id;
    root_page_id_ = page_id;
    return true;
  }
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  //printf("the insert function\n");
  auto Header = guard.AsMut<BPlusTreeHeaderPage>();
  page_id_t root_page_id = Header->root_page_id_;
  bpm_->TrackWriteGuard(std::move(guard));
  auto judge = InsertHelper(root_page_id,key,value);
  if(!judge.has_value()){
    bpm_->ReleaseThreadGuards();
    return true;
  }else{
    printf("the root is changed we need to add a new root\n");
    // we need to drop the old_root_page_guard in the table and then we can delete it
    auto tuple = judge.value();
    KeyType rootKey;
    page_id_t left,right;
    std::tie(rootKey, left,right) = tuple;
    auto [new_root,new_root_page_id] = Internal_generator();
    new_root->Addpair(rootKey,left,right,1);
    Header->root_page_id_ = new_root_page_id;
    root_page_id_ = new_root_page_id;
    bpm_->ReleaseThreadGuards();
    bpm_->DeletePage(root_page_id);
    return true;
  }
  
  // if a insertion violate the root page then we need to updata the page guard
  UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  return true;
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Internal_generator() -> std::pair<InternalPage*,page_id_t>{
  printf("generator a Internal page\n");
  page_id_t page_id = bpm_->NewPage();
  WritePageGuard page_guard = bpm_->WritePage(page_id);
  InternalPage *internal = page_guard.AsMut<InternalPage>();
  internal->Init(internal_max_size_);
  internal->page_id_ = page_id;
  return {internal,page_id};
}



INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Leaf_generator() ->std::pair <LeafPage*,page_id_t>{
  printf("generator a leaf page\n");
  page_id_t page_id = bpm_->NewPage();
  WritePageGuard page_guard = bpm_->WritePage(page_id);
  LeafPage *leaf = page_guard.AsMut<LeafPage>();
  leaf->Init(leaf_max_size_);
  leaf->page_id_ = page_id;
  printf(" generator the leaf page id is %d\n",page_id);
  return {leaf,page_id};
}
//which is to check if we need to the key value or 
//we need to add 



INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf( LeafPage *parent,const KeyType &key, const ValueType &value,int pos) ->std::tuple<KeyType,page_id_t,page_id_t>{
  //  split a leafpage into 2 leaf  
  auto [left,left_page_id] = Leaf_generator();
  auto [right,right_page_id] = Leaf_generator();
    parent->Addpair(key,value,pos);
      int middle = (leaf_max_size_ + 1)/2 ;
  for(int i = 0; i < middle; i++){
    left->Addpair(parent->KeyAt(i),parent->ValueAt(i),i);
  }
  for(int j = middle; j< (leaf_max_size_ + 1);j++){
    right->Addpair(parent->KeyAt(j),parent->ValueAt(j),j -middle);
  }
  right->SetNextPageId(parent->GetNextPageId());
  right->SetPrevPageId(left_page_id);
  left->SetNextPageId(right_page_id);
  left->SetPrevPageId(parent->GetPrevPageId());
  if(parent->GetNextPageId() != INVALID_PAGE_ID){
    WritePageGuard guard = bpm_->WritePage(parent->GetNextPageId());
    auto next_leaf = guard.AsMut<LeafPage>();
    next_leaf->SetPrevPageId(right_page_id);
  }
  if(parent->GetPrevPageId() != INVALID_PAGE_ID){
    WritePageGuard guard = bpm_->WritePage(parent->GetPrevPageId());
    auto prev_leaf = guard.AsMut<LeafPage>();
    prev_leaf->SetNextPageId(left_page_id);
  }
return {right->KeyAt(0),left_page_id,right_page_id};
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *parent,const KeyType &key, const page_id_t &left,const page_id_t &right,int pos)  ->std::tuple<KeyType,page_id_t,page_id_t> {
  //may be we need to use a guard to protect the internal add and delete function
  auto [left_child,left_page_id] = Internal_generator();
  auto [right_child,right_page_id] =  Internal_generator();
  int middle = (internal_max_size_ + 1)/2 + 1;
  parent->Addpair(key,left,right,pos);
  for(int i = 1; i < middle; i++){
    left_child->Addpair(parent->KeyAt(i),parent->ValueAt(i-1),parent->ValueAt(i),i);
  }
  for(int j = middle +1 ; j<= (internal_max_size_ + 1);j++){
    right_child->Addpair(parent->KeyAt(j),parent->ValueAt(j-1),parent->ValueAt(j),j -middle);
  }
  std::cout<<"the keyat middle ::"<<parent->KeyAt(middle) << "the middle is ::" << middle<<std::endl;
  KeyType middle_key = parent->KeyAt(middle);
  return {middle_key,left_page_id,right_page_id};
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertHelper(const page_id_t page_id,const KeyType &key, const ValueType &value)->std::optional<std::tuple<KeyType, page_id_t, page_id_t>>{
  //InsertHelper function the insert the key
  WritePageGuard guard = bpm_->WritePage(page_id);
  auto *page = guard.AsMut<BPlusTreePage>();
  page->page_id_ = page_id;
  //this page is a leaf page and we can insert it to the leaf
  // then judge if we need to split this leaf 
  if(page->IsLeafPage()){
    
    //printf("we reach to the leaf page\n");
    LeafPage *leaf = guard.AsMut<LeafPage>();
    if(leaf->GetSize()< leaf_max_size_){
      bpm_->ReleaseThreadGuards();
    }
    // we need to add this pair into the array


    //first we should judge if this key is already in the array 
    int size = leaf->GetSize();
    //printf("the size is %d\n",size);
    for(int pos = 0; pos < size;pos++){
      // this page is already in page
      if(comparator_(key,leaf->KeyAt(pos)) == 0){
        return std::nullopt;
      }
    }


  //second  we need to judge where to insert
   if(size < leaf_max_size_){
    for(int pos = 0; pos <size;pos++){
      if(comparator_(key,leaf->KeyAt(pos)) < 0){
        leaf->Addpair(key,value,pos);
        return std::nullopt;
      }
      
    }
    leaf->Addpair(key,value,size);
   }else if(size == leaf_max_size_){
    for(int pos =0 ;pos < size;pos++){
      if(comparator_(key,leaf->KeyAt(pos))<0){
       return SplitLeaf(leaf,key,value,pos);
      }
    }   
    return  SplitLeaf(leaf,key,value,size);
   } 
  }
  
  
  else{
    printf("in insert function we reach to the internal page\n");
    //this page is a internal node we need to insert deeper 
    InternalPage *internal = guard.AsMut<InternalPage>();
    if(internal->GetSize() < internal_max_size_){
      bpm_->ReleaseThreadGuards();
    }
    for(int i =1 ; i < internal->GetSize() + 1;i++){
      if(comparator_(key,internal->KeyAt(i)) < 0){
        // this is key in the
        bpm_->TrackWriteGuard(std::move(guard));
        auto parent = InsertHelper(internal->ValueAt(i-1),key,value);
        if(!parent.has_value()){
          return std::nullopt;
        }
        // which means we need to add this pair into our internal node 
        else{
          auto tuple = parent.value();
          KeyType mergeKey;
          page_id_t left,right;
          bpm_->DeletePage(internal->ValueAt(i-1));
          std::tie(mergeKey, left,right) = tuple;
          // first we need to judge if we 
          int size = internal->GetSize();
          if(size < internal_max_size_){
            internal->Addpair(mergeKey,left,right,i);
            return std::nullopt;
            
          }else if(size == internal_max_size_){
             return SplitInternal(internal,mergeKey,left,right,i);
          }
        }
      }
    }
    int pos = internal->GetSize();
    bpm_->TrackWriteGuard(std::move(guard));
    auto parent2 = InsertHelper(internal->ValueAt(pos),key,value);
    if(!parent2.has_value()){
      return std::nullopt;
    }
    // which means we need to add this pair into our internal node 
    else{
      auto tuple = parent2.value();
      KeyType mergeKey;
      page_id_t left,right;
      bpm_->DeletePage(internal->ValueAt(pos));
      std::tie(mergeKey, left,right) = tuple;
      // first we need to judge if we 
      int size = internal->GetSize();
      if(size < internal_max_size_){
        internal->Addpair(mergeKey,left,right,pos+1);
        return std::nullopt;
        
      }else if(size == internal_max_size_){
         return SplitInternal(internal,mergeKey,left,right,pos+1);
      }
  }
}
  return std::nullopt;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  if(IsEmpty()){
    return;
  }
  // first we should find the key the key is not exist
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  //printf("in the remove function\n");
  auto Header = guard.AsMut<BPlusTreeHeaderPage>();
  page_id_t root_page_id = Header->root_page_id_;
  auto parent_result = RemoveHelper(root_page_id,key);
  //after remove this key we need to judge if the root is changed
  if(!parent_result.has_value()){
    return;
  }else{
    //this root is changed and we need to updata the root page id
    printf("test deadlock in the romove function\n");
    auto [new_root_page_id,mode] = parent_result.value();
    // which means this root is changed 
    // either the value of the root is changed or the root is deleted 
    // in most of the case which means the root's children is changed 
    if(mode == 1){
      Header->root_page_id_ = new_root_page_id;
      root_page_id_ = new_root_page_id;
      return;
    }else{
      return;
    }
  }

  // Declaration of context instance.
  Context ctx;
  UNIMPLEMENTED("TODO(P2): Add implementation.");
}



INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveHelper(page_id_t page_id,const KeyType &key) ->std::optional<std::pair<page_id_t,int>>{
    WritePageGuard guard = bpm_->WritePage(page_id);
    auto *page = guard.AsMut<BPlusTreePage>();
    page->page_id_ = page_id;
    // if this is a leaf node 
    if(page->IsLeafPage()){
      auto *leaf = guard.AsMut<LeafPage>();
      leaf->page_id_ = page_id;
      return DeleteleafKey(leaf,key);
    }

    // if this is a internal node 
    // use for loop to find the pos
    auto *internal = guard.AsMut<InternalPage>();
    int size = internal->GetSize();
    int child_idx = 0;
    for(child_idx =0; child_idx < size + 1;child_idx++){
      if(child_idx == size ||comparator_(key,internal->KeyAt(child_idx + 1)) < 0){
        break;
      }
    }
    auto child_result = RemoveHelper(internal->ValueAt(child_idx),key);
    if(!child_result.has_value()){
      return std::nullopt;
    }
    // which means the child is changed
    auto [child_page_id,mode] = child_result.value();
    if(mode == 0){
      // this child is changed but not underflow
      //if the internal is not underflow then we need to updata the key
        if(child_page_id != INVALID_PAGE_ID){
          WritePageGuard child_guard = bpm_->WritePage(child_page_id);
          auto *child_page = child_guard.AsMut<BPlusTreePage>();
          if(child_page->IsLeafPage()){
            auto *child_leaf = child_guard.AsMut<LeafPage>();
            internal->SetKeyAt(child_idx,child_leaf->KeyAt(0));
        }
        }
        return std::make_pair(page_id,0);
  }else{
      return HandleUnderflow(internal,child_idx);
    }

}



INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteleafKey(LeafPage* leaf,const KeyType &key)->std::optional<std::pair<page_id_t,int>>{
  for(int i = 0; i < leaf->GetSize(); i++){
    if(comparator_(key,leaf->KeyAt(i)) == 0){
      leaf->Deletepair(i);
      page_id_t leaf_page_id = leaf->GetPageId();
      if(leaf_page_id == GetRootPageId() ){
        // this leaf is the root4
        //if the leaf is empty then we need to set the root page id to invalid page id
          return std::make_pair(INVALID_PAGE_ID,leaf->GetSize() == 0?1:0);
      }

      if(leaf->GetSize() >= (leaf_max_size_ + 1)/2 ){
        //after delete the key the leaf page is not underflow
        return std::make_pair(leaf_page_id,0);
      }
    //underflow
     return std::make_pair(leaf_page_id,1);
    }
    // we can't find the node 
    if(comparator_(key,leaf->KeyAt(i)) < 0){
      return std::nullopt;
    }
  }
  return std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::HandleUnderflow(InternalPage *parent ,int child_idx)-> std::optional<std::pair<page_id_t, int>>{
  // from left 
  const int min_leaf_size_ = leaf_max_size_ /2;
  const int min_internal_size_ = internal_max_size_ /2;
  if(child_idx > 0){
    printf("take from the left and the child_idx is %d\n",child_idx);
    page_id_t current_id = parent->ValueAt(child_idx);
    WritePageGuard current_guard = bpm_->WritePage(current_id);
    auto *current_page = current_guard.AsMut<BPlusTreePage>();

    page_id_t left_id = parent->ValueAt(child_idx -1);
    printf("the left_id is %d\n",left_id);
    WritePageGuard  left_guard = bpm_->WritePage(left_id);
    auto *left_page = left_guard.AsMut<BPlusTreePage>();
    // the left page have more value to sild
    if(current_page->IsLeafPage()){
      if(left_page->GetSize()> min_leaf_size_){
        RotateFromLeftLeaf(parent,left_guard.AsMut<LeafPage>(),current_guard.AsMut<LeafPage>(),child_idx);
        return std::make_pair(parent->GetPageId(),0);
      }
    }else{
      if(left_page->GetSize() > min_internal_size_){
        RotateFromLeftInternal(parent,left_guard.AsMut<InternalPage>(),current_guard.AsMut<InternalPage>(),child_idx);
        return std::make_pair(parent->GetPageId(),0);
      }
    }
  }

//from the right which means either the node is the first one 
//or the left node hasn't enough size 
if(child_idx < parent->GetSize()){
  printf("take from the right child_idx::%d\n",child_idx);
  page_id_t current_id = parent->ValueAt(child_idx);
  WritePageGuard current_guard = bpm_->WritePage(current_id);
  auto *current_page = current_guard.AsMut<BPlusTreePage>();

  page_id_t right_id = parent->ValueAt(child_idx + 1);
  printf("the right_id is %d\n",right_id);
  WritePageGuard right_guard = bpm_->WritePage(right_id);
  auto *right_page = right_guard.AsMut<BPlusTreePage>();
  if(current_page->IsLeafPage()){
      if(right_page->GetSize()> min_leaf_size_){
        RotateFromRightLeaf(parent,current_guard.AsMut<LeafPage>(),right_guard.AsMut<LeafPage>(),child_idx);
        return std::make_pair(parent->GetPageId(),0);
      }
  }else{
      if(right_page->GetSize() > min_internal_size_){
        RotateFromRightInternal(parent,current_guard.AsMut<InternalPage>(),right_guard.AsMut<InternalPage>(),child_idx);
        return std::make_pair(parent->GetPageId(),0);
      }
    }
}
// which means we need to merge the node
    return MergeNodes(parent,child_idx);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RotateFromLeftLeaf(InternalPage *parent,LeafPage*left,LeafPage* current,int child_idx) ->void{
  auto moved_key = left->KeyAt(left->GetSize() -1);
  auto moved_value = left->ValueAt(left->GetSize() -1);
   current->Addpair(moved_key,moved_value,0);
   left->Deletepair(left->GetSize() -1);
   // update the parent key
   parent->SetKeyAt(child_idx,current->KeyAt(0));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RotateFromRightLeaf(InternalPage *parent,LeafPage*current,LeafPage* right,int child_idx) ->void{
    auto moved_key = right->KeyAt(0);
    auto moved_value = right->ValueAt(0);
    current->Addpair(moved_key,moved_value,current->GetSize());
    right->Deletepair(0);
    // update the parent key
    parent->SetKeyAt(child_idx + 1,right->KeyAt(0));
  }


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RotateFromLeftInternal(InternalPage *parent,InternalPage*left,InternalPage*current,int child_idx) ->void{
  auto left_last_idx = left->GetSize();
  auto moved_key = parent->KeyAt(child_idx);

  current->Addpair(moved_key,left->ValueAt(left_last_idx),current->ValueAt(0),1);
  parent->SetKeyAt(child_idx,left->KeyAt(left_last_idx));
  left->Deletepair(left_last_idx);
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RotateFromRightInternal(InternalPage *parent,InternalPage*current,InternalPage*right,int child_idx) ->void{
  auto moved_key = parent->KeyAt(child_idx+1);
  auto right_pos = 0;
  auto insert_pos = current->GetSize() + 1;
  current->Addpair(moved_key,current->ValueAt(insert_pos-1),right->ValueAt(right_pos),insert_pos);
  parent->SetKeyAt(child_idx + 1,right->KeyAt(right_pos + 1));
  right->Deletepair(right_pos);
}


// which means we need to merge the node
// which means there is a confilct between the parent and the child on latch
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeNodes(InternalPage *parent, int child_idx)   -> std::optional<std::pair<page_id_t,int>>{
  printf("In the MergeNodes function the child_idx is %d\n",child_idx);
  page_id_t current_id = parent->ValueAt(child_idx);
  page_id_t sibling_id;
  page_id_t update_page_id;
  bool merge_with_left = false;
  bool is_leaf = false;
  if(child_idx > 0){
    sibling_id = parent->ValueAt(child_idx -1);
    merge_with_left = true;
  }else{
    sibling_id = parent->ValueAt(child_idx +1);
    merge_with_left = false;
  }

  if(true){
    WritePageGuard current_guard = bpm_->WritePage(current_id);
    WritePageGuard sibling_guard = bpm_->WritePage(sibling_id);

    auto *current_page = current_guard.AsMut<BPlusTreePage>();
    is_leaf = current_page->IsLeafPage();
  }
  std::cout<<current_id<<"  "<<sibling_id<<std::endl;
  //merge the leaf node
  if(is_leaf){
    WritePageGuard current_guard = bpm_->WritePage(current_id);
    WritePageGuard sibling_guard = bpm_->WritePage(sibling_id);
    auto *current_leaf = current_guard.AsMut<LeafPage>();
    auto *sibling_leaf = sibling_guard.AsMut<LeafPage>();
    
    if(merge_with_left){
      for(int i = 0; i < current_leaf->GetSize();i++){
        sibling_leaf->Addpair(current_leaf->KeyAt(i),current_leaf->ValueAt(i),sibling_leaf->GetSize());
      }
      // update the next page id
      update_page_id = current_leaf->GetNextPageId();
      sibling_leaf->SetNextPageId(current_leaf->GetNextPageId());
      if(update_page_id != INVALID_PAGE_ID){
        WritePageGuard update_guard = bpm_->WritePage(update_page_id);
        auto *update_Leaf = update_guard.AsMut<LeafPage>();
        update_Leaf->SetPrevPageId(sibling_id);
      }
    }else{
      for(int i = 0; i < sibling_leaf->GetSize();i++){
        current_leaf->Addpair(sibling_leaf->KeyAt(i),sibling_leaf->ValueAt(i),current_leaf->GetSize());
      }
      // update the next page id
      current_leaf->SetNextPageId(sibling_leaf->GetNextPageId());
      update_page_id = sibling_leaf->GetNextPageId();
      if(update_page_id != INVALID_PAGE_ID){
        WritePageGuard update_guard = bpm_->WritePage(update_page_id);
        auto *update_Leaf = update_guard.AsMut<LeafPage>();
        update_Leaf->SetPrevPageId(current_id);
    }
  }
}
    //merge the internal node
  else{
      WritePageGuard current_guard = bpm_->WritePage(current_id);
      WritePageGuard sibling_guard = bpm_->WritePage(sibling_id);
      auto *current_internal = current_guard.AsMut<InternalPage>();
      auto *sibling_internal = sibling_guard.AsMut<InternalPage>();

      if(merge_with_left){
        sibling_internal->Addpair(parent->KeyAt(child_idx),sibling_internal->ValueAt(sibling_internal->GetSize()),current_internal->ValueAt(0),sibling_internal->GetSize() + 1);
        for(int i = 0; i<current_internal->GetSize();i++){
          sibling_internal->Addpair(current_internal->KeyAt(i+1),current_internal->ValueAt(i),current_internal->ValueAt(i+1),sibling_internal->GetSize() + 1);
        }
      }
      // merge with right
      else{
        current_internal->Addpair(parent->KeyAt(child_idx + 1),current_internal->ValueAt(current_internal->GetSize()),sibling_internal->ValueAt(0),current_internal->GetSize() + 1);
        for(int i = 0; i < sibling_internal->GetSize();i++){
          current_internal->Addpair(sibling_internal->KeyAt(i+1),sibling_internal->ValueAt(i),sibling_internal->ValueAt(i+1),current_internal->GetSize() + 1);
        }
      }
  }
  //updata the next page id 





  //update the parent node
  int remove_idx = merge_with_left?child_idx:child_idx + 1;
  parent->Deletepair(remove_idx);
  // release the page
  bpm_->DeletePage(merge_with_left?current_id:sibling_id);
  //after delete the key we need to judge if the parent is underflow
  if(parent->GetSize() < (internal_max_size_ + 1) /2){
    if(parent->GetPageId() == GetRootPageId()){
      //this parent is the root
      if(parent->GetSize() == 0){
        return std::make_pair(merge_with_left?sibling_id:current_id,1);
      }
      return std::make_pair(parent->GetPageId(),0);
    }
    return std::make_pair(parent->GetPageId(),1);
  }
  return std::make_pair(parent->GetPageId(),0);
}



/*****************{
 * ************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> std::optional<INDEXITERATOR_TYPE> {
  if(IsEmpty()){
    return  std::nullopt;
  }
  page_id_t root_page_id = GetRootPageId();
  
  auto  begin = FindLeftMost(root_page_id);
  return Iterator(begin,0,bpm_);
   UNIMPLEMENTED("TODO(P2): Add implementation."); }



INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftMost(page_id_t page_id) -> LeafPage* {
  WritePageGuard  page_guard = bpm_->WritePage(page_id);
  auto page = page_guard.AsMut<BPlusTreePage> ();
  if(page->IsLeafPage()){
    return page_guard.AsMut<LeafPage> ();
  }else{
    // this is a internal page
    auto internal = page_guard.AsMut<InternalPage>();
    return FindLeftMost(internal->ValueAt(0));
  }

   UNIMPLEMENTED("TODO(P2): Add implementation."); }
/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> std::optional<INDEXITERATOR_TYPE> {
  if(IsEmpty()){
    return std::nullopt;
  }
  page_id_t root_page_id = GetRootPageId();
  auto page = FindLeafPage(root_page_id,key);
  if(!page.has_value()){
    return End();
  }
    auto [begin,index] = page.value();
    return Iterator(begin,index,bpm_);
   UNIMPLEMENTED("TODO(P2): Add implementation."); 
}


   INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(page_id_t page_id,const KeyType &key) -> std::optional<std::pair<LeafPage*,int>> {
  WritePageGuard  page_guard = bpm_->WritePage(page_id);
  auto page = page_guard.AsMut<BPlusTreePage> ();
  if(page->IsLeafPage()){
     LeafPage *leaf = page_guard.AsMut<LeafPage> ();
     for(int i = 0; i < leaf->GetSize();i++){
      if(comparator_(key,leaf->KeyAt(i)) == 0){
        return std::pair<LeafPage*,int> (leaf,i);
      }
     }
     return std::nullopt;
  }else{
    // this is a internal page
    InternalPage *internal = page_guard.AsMut<InternalPage>();
    for(int i = 1;i< internal->GetSize() + 1;i++){
      if(comparator_(key,internal->KeyAt(i)) < 0){
        return FindLeafPage(internal->ValueAt(i-1),key);
      }
    }
    return FindLeafPage(internal->ValueAt(internal->GetSize()),key);

  }

   UNIMPLEMENTED("TODO(P2): Add implementation."); 
}
/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> std::optional<INDEXITERATOR_TYPE> {
  if(IsEmpty()){
    return std::nullopt;
  }
  page_id_t root_page_id = GetRootPageId();
  auto [end,index] = FindRightMost(root_page_id);
  std::cout<<end->GetNextPageId()<<" the end index is "<<index<<std::endl;
    return Iterator(end,index,bpm_);
  UNIMPLEMENTED("TODO(P2): Add implementation."); }



INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindRightMost(page_id_t page_id) -> std::pair<LeafPage*,int> {
  WritePageGuard  page_guard = bpm_->WritePage(page_id);
  auto page = page_guard.AsMut<BPlusTreePage> ();
  if(page->IsLeafPage()){
    LeafPage *leaf =  page_guard.AsMut<LeafPage> ();
    int size = leaf->GetSize();
    return std::pair<LeafPage*,int> (leaf,size);
  }else{
    // this is a internal page
    auto internal = page_guard.AsMut<InternalPage>();
    int size = internal->GetSize();
    return FindRightMost(internal->ValueAt(size));
  }
}
/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
  INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  //printf("the root page_id is %d\n",root_page_id_);
  return root_page_id_;
   UNIMPLEMENTED("TODO(P2): Add implementation."); }


template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
