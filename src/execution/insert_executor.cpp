//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) ,plan_(plan), child_executor_(std::move(child_executor)) {
      
      
    
      //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the insert */
void InsertExecutor::Init() {
  // from the Init we can get the table heap
  table_oid_t inserted_table_oid = plan_->GetTableOid();
  //we need get the table heap from the tableinfo 
  table_info = exec_ctx_->GetCatalog()->GetTable(inserted_table_oid);
  index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  if (table_info == nullptr){
    throw Exception("Table to be inserted does not exist");
  }
  child_executor_->Init();
  return;
  UNIMPLEMENTED("TODO(P3): Add implementation."); }

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // insert a tuple and rid to the table
  if(no_more_tuples){
    return false;
  }
  Tuple child_tuple;
  RID child_rid;
  TupleMeta meta;
  meta.is_deleted_ = false;
  meta.ts_  = exec_ctx_->GetTransaction()->GetTransactionId();
  int32_t inserted_count = 0;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto inserted_rid = table_info->table_->InsertTuple(meta,child_tuple,exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), table_info->oid_);
    for(auto &index :index_info){
      // updata the index is failed
      //index is only have one column so  we need to get the key from the tuple
      // we need to get the key from the tuple
      const auto insert_tuple = child_tuple.KeyFromTuple(table_info->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->InsertEntry(insert_tuple,inserted_rid.value(),exec_ctx_->GetTransaction());
    }
    if (!inserted_rid.has_value()) {
      throw Exception("Failed to insert tuple: no space left in the table.");
    }
    inserted_count++;
  }
  const Schema &schema = plan_->OutputSchema();
  std::vector<Value> values;
  values.emplace_back(Value(TypeId::INTEGER,inserted_count));
  *tuple = Tuple(values,&schema);
  no_more_tuples = true;
  return true;
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

}  // namespace bustub
