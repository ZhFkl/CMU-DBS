//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_page.cpp
//
// Identification: src/storage/page/b_plus_tree_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool {
    if(page_type_ == bustub::IndexPageType::LEAF_PAGE){
        return true;
    }
    return  false;
    UNIMPLEMENTED("TODO(P2): Add implementation."); 
}



void BPlusTreePage::SetPageType(IndexPageType page_type) {
    page_type_ = page_type;
    return;
     UNIMPLEMENTED("TODO(P2): Add implementation."); }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { 
    return size_;
    UNIMPLEMENTED("TODO(P2): Add implementation.");
 }
void BPlusTreePage::SetSize(int size) {
    size_ = size;
    return;
    UNIMPLEMENTED("TODO(P2): Add implementation."); }
void BPlusTreePage::ChangeSizeBy(int amount) {
    size_ += amount;
    if(size_ < 0){
        size_ = 0;
    }else if(size_ > max_size_){
        size_ = max_size_;
    }
    return ;
    UNIMPLEMENTED("TODO(P2): Add implementation.");
 }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int {
    return max_size_;
     UNIMPLEMENTED("TODO(P2): Add implementation."); }
void BPlusTreePage::SetMaxSize(int size) {
    max_size_ = size;
    return; 
    UNIMPLEMENTED("TODO(P2): Add implementation."); }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * But whether you will take ceil() or floor() depends on your implementation
 */
auto BPlusTreePage::GetMinSize() const -> int {
    return max_size_+1/2; 
    UNIMPLEMENTED("TODO(P2): Add implementation.");
 }

}  // namespace bustub
