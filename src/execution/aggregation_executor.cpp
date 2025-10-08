//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) ,plan_(plan),child_executor_(std::move(child_executor)),aht_(plan_->GetAggregates(),plan_->GetAggregateTypes()),aht_iter_(aht_.Begin())
    {

  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the aggregation */
void AggregationExecutor::Init() { 
  aht_.Clear();
  child_executor_->Init();
  Tuple child_tuple;
  RID child_rid;
  AggregateKey agg_key;
  AggregateValue agg_value;
while(child_executor_->Next(&child_tuple,&child_rid)){
    agg_key = MakeAggregateKey(&child_tuple);
    agg_value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(agg_key,agg_value);
    //find it in the hash table 
    // if there isn't a map then creat the map 
    // if there is then we add the sum
  }

  if(aht_.IsEmpty()){
    agg_key = MakeDefultAggreagteKey();
    agg_value = aht_.GenerateInitialAggregateValue();
    aht_.InsertEmpty(agg_key,agg_value);
  }
  aht_iter_ = aht_.Begin();
  
  
  //UNIMPLEMENTED("TODO(P3): Add implementation."); 

}

/**
 * Yield the next tuple from the insert.
 * @param[out] tuple The next tuple produced by the aggregation
 * @param[out] rid The next tuple RID produced by the aggregation
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  while(aht_iter_ != aht_.End()){
    std::vector<Value> output_val;
    const AggregateKey cur_key = aht_iter_.Key();
    const AggregateValue cur_vals = aht_iter_.Val();
    if(!plan_->GetGroupBys().empty() && cur_key.group_bys_.empty()){
      return false;
    }
    for(auto const &key : cur_key.group_bys_){
        output_val.push_back(key);
    }
    for(auto const &val : cur_vals.aggregates_){
      output_val.push_back(val);
    }
    *tuple = Tuple(output_val,&plan_->OutputSchema());
    ++aht_iter_;
    return true;
  }
  return false;
  UNIMPLEMENTED("TODO(P3): Add implementation."); }

/** Do not use or remove this function, otherwise you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
