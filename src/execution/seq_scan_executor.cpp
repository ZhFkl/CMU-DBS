//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/macros.h"
#include <optional>
namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) ,plan_(plan),table_info_(nullptr) {
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {
  table_oid_t oid = plan_->GetTableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_  = catalog->GetTable(oid);
  BUSTUB_ASSERT(table_info_ != nullptr, "Table not found in catalog");
  iter_ = table_info_->table_->MakeIterator();
  
   //UNIMPLEMENTED("TODO(P3): Add implementation."); 
  }

/**
 * Yield the next tuple from the sequential scan.
 * @param[out] tuple The next tuple produced by the scan
 * @param[out] rid The next tuple RID produced by the scan
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // judge if the iterator is end
    if(iter_->IsEnd()){
      return false;
    }
    //through a loop to find the next valid tuple
    TransactionManager* txn_mgr = exec_ctx_->GetTransactionManager();
    while(!iter_->IsEnd()){ 
      auto [meta, cur_tuple] = (*iter_).GetTuple();
      RID tuple_rid = cur_tuple.GetRid();
      auto undolink = txn_mgr->GetUndoLink(tuple_rid);
      Transaction* txn = exec_ctx_->GetTransaction();
      auto undologs = CollectUndoLogs(cur_tuple.GetRid(),meta,cur_tuple,undolink,txn,txn_mgr);
      //first we need to reconstruct the tuple
      if(!undologs.has_value()){
        ++(*iter_);
        continue;
      }
      auto tuple_ = ReconstructTuple(&plan_->OutputSchema(),cur_tuple,meta,undologs.value());  
      if(!tuple_.has_value()){
        ++(*iter_);
        continue;
      }    
      Tuple cur_tuple_ = tuple_.value();
      AbstractExpressionRef predicate = plan_->filter_predicate_;
      bool is_match = false;
      if(predicate != nullptr){
          Value val = predicate->Evaluate(&cur_tuple_, table_info_->schema_);
          is_match = val.GetAs<bool>();
      }else{
          is_match = true;
      }
      
      if(is_match){
          *tuple = cur_tuple_;
          *rid = tuple_rid;
          ++(*iter_);
          return true;
      }

      
      ++(*iter_);
    }
    return false;
      UNIMPLEMENTED("TODO(P3): Add implementation."); 
  }


}  // namespace bustub
