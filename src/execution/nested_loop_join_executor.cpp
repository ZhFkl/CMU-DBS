//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ =std::move(right_executor);

  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() { 
  left_executor_->Init();
  right_executor_->Init();
  left_plan_node_ = plan_->GetLeftPlan();
  right_plan_node_ = plan_->GetRightPlan();
  if(!match_queue_.empty()){
    match_queue_.pop();
  }
  has_current_left_ = false;
}
 // UNIMPLEMENTED("TODO(P3): Add implementation."); }

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join
 * @param[out] rid The next tuple RID produced, not used by nested loop join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  while(true){
    if(match_queue_.empty()){
      RID  left_rid;
      if(!left_executor_->Next(&current_left_tuple_, &left_rid)){
        has_current_left_ = false;
        return false;
      }


      has_current_left_ = true;
      right_executor_->Init();
      

      Tuple right_tuple ;
      RID right_rid;
      while(right_executor_->Next(&right_tuple,&right_rid)){
        Value is_match = plan_->predicate_->EvaluateJoin(
          &current_left_tuple_,left_plan_node_->OutputSchema(),
          &right_tuple,right_plan_node_->OutputSchema()
        );

        if(is_match.IsNull() || !is_match.GetAs<bool>()){
          continue;
        } 
        Tuple val = CombineTuple(&current_left_tuple_,&right_tuple,false);
        match_queue_.push(val);
      }
    }


    if(!match_queue_.empty()){
      *tuple = match_queue_.front();
      match_queue_.pop();
      return true;
    }


    if (plan_->GetJoinType() == JoinType::LEFT && has_current_left_) {
        *tuple = CombineTuple(&current_left_tuple_, nullptr, true);
        return true;
    }


  }




  UNIMPLEMENTED("TODO(P3): Add implementation."); 

}





auto const  NestedLoopJoinExecutor::CombineTuple(const Tuple *left_tuple,const Tuple* right_tuple,bool null) ->Tuple{
    Schema left_schema = left_plan_node_->OutputSchema();
    Schema right_schema = right_plan_node_->OutputSchema();
    Schema join_schema = plan_->OutputSchema();
    std::vector<Value> val;
    for(uint32_t i = 0; i < left_schema.GetColumnCount();i++){
      val.push_back(left_tuple->GetValue(&left_schema,i));
    }
    if(null){
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

