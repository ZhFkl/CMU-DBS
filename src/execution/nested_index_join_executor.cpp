//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) ,child_executor_(std::move(child_executor)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = std::move(plan);
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

void NestedIndexJoinExecutor::Init() { 
  child_executor_->Init();
  inner_table = exec_ctx_->GetCatalog()->GetTable(plan_->inner_table_oid_);
  inner_idx = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  auto predicate_ = plan_->KeyPredicate();
  left_predicate = dynamic_cast<ColumnValueExpression *>(predicate_.get());
  

  while(!match_queue_.empty()){
    match_queue_.pop();
  }



  //UNIMPLEMENTED("TODO(P3): Add implementation."); 
  }

auto NestedIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(true){
    if(match_queue_.empty()){
      RID left_rid;
      if(!child_executor_->Next(&current_left_tuple_,&left_rid)){
        return false;
      }
      const Schema &left_schema = child_executor_->GetOutputSchema();
      Value join_key_val = current_left_tuple_.GetValue(&left_schema,left_predicate->GetColIdx());
      std::vector<Value> key_values = {join_key_val};

      const Schema &index_key_schema = inner_idx->key_schema_;
      Tuple index_key(key_values,&index_key_schema);

      std::vector<RID> right_rid_list;
      inner_idx->index_->ScanKey(index_key,&right_rid_list,exec_ctx_->GetTransaction());

      for(auto const & rid:right_rid_list){
        auto [right_meta,right_tuple] = inner_table->table_->GetTuple(rid);
        if(right_meta.is_deleted_ ){
          continue;
        }
        Tuple matched_tuple = CombineTuple(&current_left_tuple_,&right_tuple,false);
        match_queue_.push(matched_tuple);
      }
      if(plan_->GetJoinType() == JoinType::LEFT && match_queue_.empty()){
        *tuple = CombineTuple(&current_left_tuple_,nullptr,true);
        return true;
      }
    }

    if(!match_queue_.empty()){
      *tuple = match_queue_.front();
      match_queue_.pop();
      return true;
    }

    
  }
  
  
  
  
  
  //UNIMPLEMENTED("TODO(P3): Add implementation."); 



}


auto NestedIndexJoinExecutor::CombineTuple(const Tuple *left_tuple,const Tuple *right_tuple,bool null ) -> Tuple{
  Schema left_schema = child_executor_->GetOutputSchema();
  Schema right_schema = plan_->InnerTableSchema();
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
  Schema join_schema = plan_->OutputSchema();
  return Tuple(val,&join_schema);


}

}  // namespace bustub
