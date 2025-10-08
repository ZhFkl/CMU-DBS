//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) ,plan_(plan),child_executor_(std::move(child_executor)){
      
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the update */
void UpdateExecutor::Init() { 
  table_oid_t updata_table_oid = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(updata_table_oid);
  index_info  = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if(table_info_ == nullptr){
    throw Exception("Table to be updated does not exist");
  }
  child_executor_->Init();

  return;
  
  UNIMPLEMENTED("TODO(P3): Add implementation."); }

/**
 * Yield the next tuple from the update.
 * @param[out] tuple The next tuple produced by the update
 * @param[out] rid The next tuple RID produced by the update (ignore this)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
 */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  //
  if(no_more_tuples){
    return false;
  }
  Tuple child_tuple;
  RID child_rid;
  int32_t updated_count = 0;
  TupleMeta insert_meta;
  insert_meta.is_deleted_ = false;
  insert_meta.ts_ = 0;
  const Schema &table_schema = table_info_->schema_;
  const auto &exprssions = plan_->target_expressions_;

  while(child_executor_->Next(&child_tuple,&child_rid)){
    //delte the old tuple
    auto [old_meta,old_tuple] = table_info_->table_->GetTuple(child_rid);

    std::vector<Value> values;
    for(uint32_t i = 0; i < table_schema.GetColumnCount();i++){
      // get the new value
      const auto &expr = exprssions[i];
      Value new_value = expr->Evaluate(&old_tuple,table_schema);
      values.push_back(new_value); 
    }
    Tuple new_tuple(values,&table_schema);
    old_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(old_meta,child_rid);
    //this might be wrong because we need to insert it at the same place
    auto inserted_rid = table_info_->table_->InsertTuple(insert_meta,new_tuple,exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), table_info_->oid_);
    if(!inserted_rid.has_value()){
      throw Exception("Failed to insert tuple: no space left in the table.");
    }
    for(auto& index : index_info){
      const auto delete_tuple = old_tuple.KeyFromTuple(table_schema, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      const auto insert_tuple = new_tuple.KeyFromTuple(table_schema, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(delete_tuple,child_rid,exec_ctx_->GetTransaction());
      
      index->index_->InsertEntry(insert_tuple,inserted_rid.value(),exec_ctx_->GetTransaction());
    }
    updated_count++;

    //construct the new tuple
    // insert the new tuple
    // update the index
  }
  const Schema &schema = plan_->OutputSchema();
  std::vector<Value> values;
  values.emplace_back(Value(TypeId::INTEGER, updated_count));
  *tuple = Tuple(values,&schema);
  no_more_tuples = true;
  return true;


  UNIMPLEMENTED("TODO(P3): Add implementation.");
}

}  // namespace bustub
