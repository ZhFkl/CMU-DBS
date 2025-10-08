//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Spring 2025.
 */
class SortPage {
 public:
  /**
   * TODO(P3): Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */

  SortPage() = delete;
  ~SortPage() = delete;
  SortPage(const SortPage &other) = delete;
  inline auto Init(size_t tuple_size){
    size = 0;
    tuple_size_ = tuple_size  + sizeof(uint32_t);
    SetTuplecount(tuple_size_);
  }   


  inline auto InsertTuple(const Tuple* tuple,const Schema& schema){
    //
    if(size>= max_size || tuple_size_ != schema.GetInlinedStorageSize() + sizeof(uint32_t)){
      throw std::runtime_error("the size of the tuple_size_ isn't equal to the scheam");
    }
    size_t offset =  sizeof(size_t)*3 + size*tuple_size_;
    if(offset > BUSTUB_PAGE_SIZE){
      throw std::runtime_error("the size of th offset is run out of the page");
    }
    tuple->SerializeTo(reinterpret_cast<char*>(this) + offset);
    size++;
    return true;
  
  }


  inline auto GetTuple(size_t idx,const Schema& schema) -> Tuple{
    if(idx >= max_size || tuple_size_ != schema.GetInlinedStorageSize() + sizeof(uint32_t)){
      throw std::runtime_error("out of range or the size is not equal");
    }
    size_t offset = 3*sizeof(size_t) + idx*tuple_size_;
    if(offset > BUSTUB_PAGE_SIZE){
      throw std::runtime_error("out of range for the offset");
    }
    Tuple tuple;
    tuple.DeserializeFrom(reinterpret_cast<char*>(this) + offset);
    return tuple;
  }


  inline auto GetSize() const ->size_t{
    return size;
  }

  inline auto IsFull() ->bool{
    return size>= max_size;
  }

private:
/**
 * TODO(P3): Define the private members. You may want to have some necessary metadata for
 * the sort page before the start of the actual data.
 */
  inline auto SetTuplecount(size_t size) ->void{
    size_t data_area_size = BUSTUB_PAGE_SIZE - sizeof(size_t)*3;
    max_size = data_area_size / size;
  }
size_t max_size;
size_t size;
size_t tuple_size_;
};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
  friend class Iterator;

 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm,const Schema & schema) : pages_(std::move(pages)), bpm_(bpm),schema_(schema) {}

  auto GetPageCount() -> size_t { return pages_.size(); }
  auto GetSchema() const->const Schema &{
    return schema_;
  }
  
  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     */
    auto operator++() -> Iterator & { 
      auto page_guard = run_->bpm_->WritePage(page_id_);
      SortPage* sort_page = page_guard.AsMut<SortPage>();
      size_t current_id = run_->pages_.size();
      idx_++;
      if(idx_>=sort_page->GetSize()){
        for(size_t i = 0; i < run_->pages_.size();i++){
          if(page_id_ == run_->pages_[i]) {
            current_id = i;
            break;
          }
        }
        if(current_id >= run_->pages_.size()){
          throw std::runtime_error("Iterator: page_id" + std::to_string(page_id_) + "not found");
        }
        bool has_next_page = (current_id + 1 < run_->pages_.size());
        if(has_next_page){
          this->page_id_ = run_->pages_[current_id + 1];
          this->idx_ = 0;
        }else{
          this->idx_ = sort_page->GetSize();
        }
      }


      return *this;
      
      UNIMPLEMENTED("TODO(P3): Add implementation."); }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     */
    auto operator*() const-> Tuple { 
      // the page_id of the iterator should in the pages_ array
      
      auto page_guard = run_->bpm_->WritePage(page_id_);
      SortPage* sort_page = page_guard.AsMut<SortPage>();
      return sort_page->GetTuple(idx_,run_->schema_);


      UNIMPLEMENTED("TODO(P3): Add implementation."); }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     */
    auto operator==(const Iterator &other) const -> bool { 
      return (page_id_ == other.page_id_) && (idx_ == other.idx_);
      
      UNIMPLEMENTED("TODO(P3): Add implementation."); }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     */
    auto operator!=(const Iterator &other) const -> bool { 
      return !(*this == other);
      
      
      UNIMPLEMENTED("TODO(P3): Add implementation."); }

      
    [[maybe_unused]] const MergeSortRun *run_;


   private:
    explicit Iterator(const MergeSortRun *run,page_id_t page_id,size_t idx) : run_(run),page_id_(page_id),idx_(idx){}

    /** The sorted run that the iterator is iterating on. */
    page_id_t page_id_;
    size_t idx_;
    /*
     * TODO(P3): Add your own private members here. You may want something to record your current
     * position in the sorted run. Also feel free to add additional constructors to initialize
     * your private members.
     */
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   */
  auto Begin() const-> Iterator {
    if(pages_.empty()){
      throw std::out_of_range("there is no pages in the heap");
    } 
    return  Iterator(this,pages_[0],0);
    UNIMPLEMENTED("TODO(P3): Add implementation."); }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   */
  auto End() const-> Iterator { 
    if(pages_.empty()){
      throw std::out_of_range(" the page_id vector is empty");
    }
    page_id_t end_page_id = pages_.back();
    auto page_guard = bpm_->ReadPage(end_page_id);
    const SortPage* sort_end_page = page_guard.As<SortPage>();
    const size_t idx = sort_end_page->GetSize();
    return Iterator(this,end_page_id,idx);

    
    UNIMPLEMENTED("TODO(P3): Add implementation."); }



auto GetPages() const-> std::vector<page_id_t>{
  return pages_;
}
 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  BufferPoolManager *bpm_;
  const Schema schema_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Spring 2025, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  auto Sort() ;
  auto GetSortEntry(const Tuple & tuple,const std::vector<OrderBy> *orderbys,const Schema schema) ->SortEntry{

    std::vector<Value> vals;
    for(auto const &order:*orderbys){
      auto val = order.second->Evaluate(&tuple,schema);
      vals.push_back(val);
    }
    SortEntry entry =  std::make_pair(vals,tuple);
    return entry;
  }

  auto Push(std::vector<SortEntry> entry) ->std::vector<page_id_t>;


 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;
  std::unique_ptr<AbstractExecutor> child_;
  BufferPoolManager* bpm_;
  std::vector<MergeSortRun> initial_runs;
  bool iter_initialized_ = false;
  MergeSortRun::Iterator iter_;
  bool initialed = false;
  /** TODO(P3): You will want to add your own private members here. */
};

}  // namespace bustub
