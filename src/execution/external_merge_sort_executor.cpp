//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <vector>
#include "common/macros.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(std::move(plan)),cmp_(plan->GetOrderBy()),child_(std::move(child_executor)){
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {

  if(!initialed){
    child_->Init();
    Tuple child_tuple;
    auto orders = plan_->GetOrderBy();
    RID child_rid;
    bpm_ = exec_ctx_->GetBufferPoolManager();
    Schema schema = child_->GetOutputSchema();
    size_t tuple_size = schema.GetInlinedStorageSize();
    size_t max_size = (BUSTUB_PAGE_SIZE * 4  - 128)/tuple_size;
    std::vector<SortEntry> child_entry;

  //get child_tuple from the child
    while(child_->Next(&child_tuple,&child_rid)){
      // if the sortpage is not full then we insert the tuple to the page directly
      // first we define a max_size of the tuple vector 
      if(child_entry.size() >= max_size){
        // sort the tuple and then store it in the sortpage
        // but fisrst
        std::sort(child_entry.begin(),child_entry.end(),cmp_);
        // after sort we need to put them to sort page
        auto run_pages = Push(child_entry);
        child_entry.clear();
        MergeSortRun initial_run(run_pages,bpm_,schema);
        initial_runs.push_back(initial_run);
      }else{
        SortKey key = GenerateSortKey(child_tuple,orders,schema);
        SortEntry entry(key,child_tuple);
        child_entry.push_back(entry);
      }
    }
    if(!child_entry.empty()){
      std::sort(child_entry.begin(),child_entry.end(),cmp_);
      // after sort we need to put them to sort page
      auto run_pages = Push(child_entry);
      child_entry.clear();
      MergeSortRun initial_run(run_pages,bpm_,schema);
      initial_runs.push_back(initial_run);
    }

    Sort();
    initialed = true;
  }
  else{
    iter_initialized_ = false;
  }
    

  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::Push(std::vector<SortEntry> entries) ->std::vector<page_id_t>  {

  std::vector<page_id_t> pages;
  Schema schema = child_->GetOutputSchema();
  size_t tuple_size = schema.GetInlinedStorageSize();
  page_id_t sort_page_id = bpm_->NewPage();
  pages.push_back(sort_page_id);
  auto current_page_guard = bpm_->WritePage(sort_page_id);



  SortPage* current_sort_page = current_page_guard.AsMut<SortPage>();
  current_sort_page->Init(tuple_size);
  for(const auto & entry:entries){
    if(current_sort_page->IsFull()){
      current_page_guard.Drop();
      page_id_t next_page_id = bpm_->NewPage();
      pages.push_back(next_page_id);
      current_page_guard = bpm_->WritePage(next_page_id); 
      current_sort_page = current_page_guard.AsMut<SortPage>();
      current_sort_page->Init(tuple_size);
      current_sort_page->InsertTuple(&entry.second,schema);
    }else{
      current_sort_page->InsertTuple(&entry.second,schema);
    }
  }
  return pages;


  UNIMPLEMENTED("TODO(P3): Add implementation.");
}


/**
 * Yield the next tuple from the external merge sort.
 * @param[out] tuple The next tuple produced by the external merge sort.
 * @param[out] rid The next tuple RID produced by the external merge sort.
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  if(!iter_initialized_){
    if(initial_runs.empty()){
      return false;
    }
    const MergeSortRun &final_run = initial_runs[0];
    iter_ = final_run.Begin();
    iter_initialized_ = true;
  }
  auto iter = iter_.run_->End();
  if(iter_ == iter){
    return false;
  }
  *tuple = *iter_;
  ++iter_;
  return true;
  UNIMPLEMENTED("TODO(P3): Add implementation.");
}



template <size_t K>
auto ExternalMergeSortExecutor<K>::Sort(){
  // first of all get the k runs iterator
  if(initial_runs.empty()){
    printf("the initial_runs is empty\n");
  }
  std::vector<MergeSortRun::Iterator> run_iters;
  for(const auto & run:initial_runs){
    auto iter = run.Begin();
    // judge if the iter is null
    run_iters.push_back(iter);
  }
  auto order = plan_->GetOrderBy();
  Schema schema = child_->GetOutputSchema();
  struct MergeElement{
    SortEntry entry;
    MergeSortRun::Iterator iter;
    TupleComparator cmp_;
    MergeElement(SortEntry entry_,MergeSortRun::Iterator iter_,const TupleComparator& cmp):
    entry(std::move(entry_)),iter(iter_),cmp_(cmp){}


    bool operator<(const MergeElement &other) const {
      return !cmp_(entry,other.entry);
    }
  };


  std::priority_queue<MergeElement> pq;
  for(auto it:run_iters){
    auto tuple = *it;
    auto key = GenerateSortKey(tuple,order,schema);
    SortEntry entry(key,tuple);
    MergeElement mergenode(entry,it,cmp_);
    pq.push(mergenode);
  }
  std::vector<page_id_t> merged_run_pages;
  SortPage* current_sort_page = nullptr;
  WritePageGuard current_page_guard;
  size_t Tuple_size = schema.GetInlinedStorageSize();


  while(!pq.empty()){
    auto min_element = pq.top();
    pq.pop();

    // get the min tuple
    const Tuple &current_tuple = min_element.entry.second;
    if(current_sort_page == nullptr || current_sort_page->IsFull()){
      // if the page is full or the page is nullptr
      // then we need to arange a new page and drop the page_guard before //
      //the page guard is none arangeed tuple
      if(current_sort_page != nullptr){
        merged_run_pages.push_back(current_page_guard.GetPageId());
        current_page_guard.Drop();
      }
      page_id_t new_page_id = bpm_->NewPage();
      current_page_guard = bpm_->WritePage(new_page_id);
      current_sort_page = current_page_guard.AsMut<SortPage>();
      current_sort_page->Init(Tuple_size);
    }
    current_sort_page->InsertTuple(&current_tuple,schema);
    auto &current_iter = min_element.iter;
    ++current_iter;
    if(current_iter != current_iter.run_->End()){
      Tuple next_tuple = *current_iter;
      SortKey next_key = GenerateSortKey(next_tuple,order,schema);
      SortEntry next_entry(next_key,next_tuple);
      pq.emplace(next_entry,current_iter,cmp_);
    }else{
      const auto& completed_run = *current_iter.run_;
      for(page_id_t page_id: completed_run.GetPages()){
        bpm_->DeletePage(page_id);
      }
    }
    // i need iter to move to the next tuple of the mergesortrun
    // but the queue only have a sortentry because sort entry have cmp 
    // i need to construct a new struct which contain a iter and the tuple
     // and we can sort it in prioritqueue
  }
  if(current_sort_page !=nullptr){
    merged_run_pages.push_back(current_page_guard.GetPageId());
    current_page_guard.Drop();
  }
  // then load the pages into the new mergesortrun
  if(!merged_run_pages.empty()){
    initial_runs.clear();
    initial_runs.emplace_back(merged_run_pages,bpm_,plan_->OutputSchema());
  }



}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
