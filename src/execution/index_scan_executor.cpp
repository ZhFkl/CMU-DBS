//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) ,plan_(plan),is_full_scan(false),point_scan_idx_(0)
    {

  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

// first achieve the point scan and then the range scan
void IndexScanExecutor::Init() { 
  keys.clear();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  BUSTUB_ASSERT(tree_ != nullptr, "Index is not BPlusTreeIndexForTwoIntegerColumn");
  // if there is no predicate, then it is a full scan
  
  for(const auto &key_expr :plan_->pred_keys_){
    auto *const_expr = dynamic_cast<ConstantValueExpression *>(key_expr.get());
    keys.push_back(const_expr->val_);
  }

  const auto &key_schema = index_info_->key_schema_;
  is_full_scan = keys.empty();

  point_scan_rids_.clear();
  point_scan_idx_ = 0;

  if(is_full_scan){
    iter_ = tree_->GetBeginIterator();
    
  }else if(!plan_->filter_predicate_){

  }else{
    std::vector<RID> key_rids_;
    std::vector<Value> key_;
    for(Value  &key:keys){
      key_rids_.clear();
      key_.clear();
      key_.push_back(key);
      const Tuple key_tuple(key_,&key_schema);
      tree_->ScanKey(key_tuple,&key_rids_,exec_ctx_->GetTransaction());
      if(!key_rids_.empty())
        point_scan_rids_.insert(point_scan_rids_.end(),key_rids_.begin(),key_rids_.end());
    }

  }

  return ;
  UNIMPLEMENTED("TODO(P3): Add implementation."); }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if(is_full_scan){
    while( iter_.has_value() && !iter_->IsEnd()){
      auto [key,cur_rid] = *iter_.value();
      *rid = cur_rid;
      ++iter_.value();
      auto [meta,cur_tuple] = table_info_->table_->GetTuple(*rid);
      if(!meta.is_deleted_){
        *tuple = cur_tuple;
        return true;
    }
  }
  return false;
}
while(point_scan_idx_ < point_scan_rids_.size()){
  *rid = point_scan_rids_[point_scan_idx_++];
  auto [meta,cur_tuple] = table_info_->table_->GetTuple(*rid);
  if(!meta.is_deleted_){
    *tuple = cur_tuple;
    return true;
    }
  }
  return false;
  UNIMPLEMENTED("TODO(P3): Add implementation."); }

}  // namespace bustub
