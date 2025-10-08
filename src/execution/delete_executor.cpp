//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) ,plan_(plan),child_executor_(std::move(child_executor)){
      
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the delete */
void DeleteExecutor::Init() {
  //get the table info
  //first get the table oid
  table_oid_t delete_table_oid = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(delete_table_oid);
  index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if(table_info_ == nullptr){
    throw Exception("Table to be deleted does not exist");
  }
  child_executor_->Init();
  
  
  
  return;
  UNIMPLEMENTED("TODO(P3): Add implementation."); }

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple The integer tuple indicating the number of rows deleted from the table
 * @param[out] rid The next tuple RID produced by the delete (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(no_more_tuples){
    return false;
  }
  Tuple child_tuple;
  RID child_rid;
  int32_t deleted_count = 0;
  while(child_executor_->Next(&child_tuple, &child_rid)){
    auto [delete_meta, delete_tuple] = table_info_->table_->GetTuple(child_rid);
    if(delete_meta.is_deleted_){
      continue;
    }
    delete_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(delete_meta, child_rid);
    for(auto &index : index_info){
      // 
      auto const & delete_tuple_ = child_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(delete_tuple_,child_rid,exec_ctx_->GetTransaction());
    }
    deleted_count++;
  }
  const Schema &schema = plan_->OutputSchema();
  std::vector<Value> values;
  values.push_back(Value(TypeId::INTEGER, deleted_count));
  *tuple = Tuple(values,&schema);
  no_more_tuples = true;
  return  true;
  UNIMPLEMENTED("TODO(P3): Add implementation.");
}

}  // namespace bustub
