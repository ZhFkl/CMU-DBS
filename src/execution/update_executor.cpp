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
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr = exec_ctx_->GetTransactionManager();
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

  // in this function i use a 
  Tuple child_tuple;
  RID child_rid;
  int32_t updated_count = 0;
  TupleMeta update_meta;
  const Schema &table_schema = table_info_->schema_;
  const auto &exprssions = plan_->target_expressions_;
  while(child_executor_->Next(&child_tuple,&child_rid)){
    std::vector<uint32_t> modified;
    //delte the old tuple
    auto [old_meta,old_tuple] = table_info_->table_->GetTuple(child_rid);
    std::vector<Value> values;
    for(uint32_t i = 0; i < table_schema.GetColumnCount();i++){
      // get the new value
      const auto &expr = exprssions[i];
      
      Value new_value = expr->Evaluate(&old_tuple,table_schema);
      values.push_back(new_value);
      if(!new_value.CompareExactlyEquals(old_tuple.GetValue(&table_schema,i))){
        modified.push_back(i);
      }
    }
    if(!check_double_write_conflict(txn_,old_meta,txn_mgr,child_rid,modified)){
      return false;
    }
    update_meta.is_deleted_ = old_meta.is_deleted_;
    update_meta.ts_ = txn_->GetTransactionId();
    Tuple new_tuple(values,&table_schema);
    new_tuple.SetRid(child_rid);
    table_info_->table_->UpdateTupleInPlace(update_meta,new_tuple,child_rid,nullptr);
    if(txn_->IsModified(table_info_->oid_,child_rid)){
      auto undolink = txn_mgr->GetUndoLink(child_rid);
        // we don't need to update the state
        auto undolog = txn_mgr->GetUndoLog(undolink.value());
        auto updateundolog = GenerateUpdatedUndoLog(&table_schema,&old_tuple,&new_tuple,undolog);
        txn_->UpdateUndolog(undolink.value().prev_log_idx_,updateundolog);
        if(!txn_->IsInsert(child_rid)){
          txn_->SetState(child_rid,STATE::UPDATE);
        }
      }else{
        // which means the it's not been modified then we need to add a undolog
        auto undolink = txn_mgr->GetUndoLink(child_rid);
        auto undolog = GenerateNewUndoLog(&table_schema,&old_tuple,&new_tuple,txn_->GetReadTs(),undolink.value());
        auto new_undolink = txn_->AppendUndoLog(undolog);
        txn_mgr->UpdateUndoLink(child_rid,new_undolink);
        txn_->AppendWriteSet(table_info_->oid_,child_rid);
        txn_->SetState(child_rid,STATE::UPDATE);
      }
    for(auto& index : index_info){
      // the index need to be updated but 
      // we need to delete the old tuple and insert the new tuple 
      // and the child_rid is the same because we update not delete and insert a new
      // one 
      const auto delete_tuple = old_tuple.KeyFromTuple(table_schema, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      const auto insert_tuple = new_tuple.KeyFromTuple(table_schema, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(delete_tuple,child_rid,exec_ctx_->GetTransaction());
      index->index_->InsertEntry(insert_tuple,child_rid,exec_ctx_->GetTransaction());


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
