//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = std::move(plan);
  left_child_ = std::move(left_child);
  right_child_ = std::move(right_child);
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the join */
void HashJoinExecutor::Init() { 
  left_child_->Init();
  right_child_->Init();
  left_plan = plan_->GetLeftPlan();
  right_plan = plan_->GetRightPlan();

  Schema right_schema = right_child_->GetOutputSchema();
  auto right_exprs = plan_->RightJoinKeyExpressions();

  Tuple right_tuple;
  RID right_rid;
  while(right_child_->Next(&right_tuple,&right_rid)){
    std::vector<Value> key_values;
    for(const auto & expr:right_exprs){
      Value val = expr->Evaluate(&right_tuple,right_schema);
      BUSTUB_ASSERT(!val.IsNull(),"the key should not be null");
      key_values.push_back(val);
    }
    HashKey key_{key_values};
    ht_[key_].push_back(right_tuple);
  }

 // UNIMPLEMENTED("TODO(P3): Add implementation."); 
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join.
 * @param[out] rid The next tuple RID, not used by hash join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(true){
  if(match_queue_.empty()){
    Tuple left_tuple;
    RID left_rid;
    Schema left_schema = left_child_->GetOutputSchema();
    auto left_exprs = plan_->LeftJoinKeyExpressions();
  
    std::vector<Value> key_vals;
    if(!left_child_->Next(&left_tuple,&left_rid)){
      return false;
    }
    for(const auto & expr:left_exprs){
        Value val = expr->Evaluate(&left_tuple,left_schema);
        BUSTUB_ASSERT(!val.IsNull(),"the key should not be null");
        key_vals.push_back(val);
    }
    HashKey key_{key_vals};
    if(ht_.find(key_)!= ht_.end()){
      for(const Tuple &right_tuple:ht_[key_]){
        Tuple val = CombineTuple(&left_tuple, &right_tuple,false);
        match_queue_.push(val);
      }
    }else{
      if(plan_->GetJoinType() == JoinType::LEFT){
        Tuple val = CombineTuple(&left_tuple,nullptr,true);
        match_queue_.push(val);
      }
    }
    
  }
  if(!match_queue_.empty()){
    *tuple = match_queue_.front();
    match_queue_.pop();
    return true;
  }



  }
  
}

auto HashJoinExecutor::CombineTuple(const Tuple* left_tuple,const Tuple* right_tuple,bool IsNull) ->Tuple{
    Schema left_schema = left_plan->OutputSchema();
    Schema right_schema = right_plan->OutputSchema();
    Schema join_schema = plan_->OutputSchema();
    std::vector<Value> val;
    for(uint32_t i = 0; i < left_schema.GetColumnCount();i++){
      val.push_back(left_tuple->GetValue(&left_schema,i));
    }
    if(IsNull){
      for(uint32_t i = 0; i < right_schema.GetColumnCount();i++){
        Column col = right_schema.GetColumn(i);
        val.push_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
    }else{
      for(uint32_t i = 0; i < right_schema.GetColumnCount();i++){
        val.push_back(right_tuple->GetValue(&right_schema,i));
      }
    }
    return Tuple(val,&join_schema);




}
}  // namespace bustub
