//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

/** TODO(P3): Implement the comparison method */
auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool { 
  

  // achieve a way to compare the two  operator
  for(size_t i = 0; i < order_bys_.size();i++){
    auto val_a = entry_a.first[i];
    auto val_b = entry_b.first[i];
    auto order = order_bys_[i];
    //
    auto compare_result = val_a.CompareLessThan(val_b);
    auto notequal = val_a.CompareNotEquals(val_b);
    if(notequal == CmpBool::CmpTrue){
      return (order.first == OrderByType::ASC||order.first == OrderByType::DEFAULT)? (compare_result == CmpBool::CmpTrue):(compare_result == CmpBool::CmpFalse);
    }
  }
//which means the two tuples are exactly the same 
  return false; 
}

/**
 * Generate sort key for a tuple based on the order by expressions.
 *
 * TODO(P3): Implement this method.
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {

  std::vector<Value> vals;
  for(const auto &order: order_bys){
    Value val = order.second->Evaluate(&tuple,schema);
    if(val.IsNull()){
      printf("the val is null\n");
    }
    vals.push_back(val);
  }
  return {vals};
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
    // first of all check out 
    std::vector<Value> tuple_val;
    bool is_delete = base_meta.is_deleted_;
    for(size_t i = 0 ; i < schema->GetColumnCount();i++){
      tuple_val.push_back(base_tuple.GetValue(schema,i));
    }
    for(auto log : undo_logs){
      // roll back 
      std::vector<Column> unlog_schema;
      for(size_t i = 0; i < schema->GetColumnCount();i++){
        if(log.modified_fields_[i]){
          // which means the data is changed so we need to get the scheam
          unlog_schema.push_back(schema->GetColumn(i));
        }
      }
      Schema unlog_schema_(unlog_schema);
      // since we have the schema then we need to update the value
      // if a data is deleted then we just roll back ?

      // updata the data of the tuple 
      uint32_t pos = 0;
      for(size_t i = 0; i< schema->GetColumnCount();i++){
        if(log.modified_fields_[i]){
          tuple_val[i] = log.tuple_.GetValue(&unlog_schema_,pos++);
        }
      }
      is_delete = log.is_deleted_;
    }
    if(is_delete){
      return std::nullopt;
    }
    return Tuple(tuple_val,schema);


  
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  
  
  
  //check if the base_meta is modifing but not commit
  std::vector<UndoLog> logs;
  // i need to judge if the tuple has been modified but not commit
  // if it is this kind of condition we need to check if it was modified by this txn if it is , then we can see it 
  // else we can't see if we , we need to roll back 
  if(base_meta.ts_  <= txn->GetReadTs() || base_meta.ts_ == txn->GetTransactionId()){
    // if the metats is smaller than the readts ,which means the tuple don't need to roll back 
    // else we need to roll back to find the lateset commit unlog array
    // else if the meta is modified by the txn then we can see it else we can't see is 
    // which means the commit_ts is smaller than my read so  we don't need to retrive 
    // the data of the tuple, we just return the base_meta;
    return logs;
  }
  // which means the ts is larger than my readts
  if(!undo_link.has_value()){
    return std::nullopt;
  }
  auto map = txn_mgr->txn_map_;
  UndoLink link= undo_link.value();
  // the link is the newest link for the tuple
  auto txn_id = link.prev_txn_;
  auto log_idx = link.prev_log_idx_;

  // find the prev unlog 
  if(map.find(txn_id) == map.end()){
    printf("can't find the txn in the map\n");
  }
  auto txn_ = map[txn_id];
  UndoLog log = txn_->GetUndoLog(log_idx);
  while(log.prev_version_.IsValid()){
    if(log.ts_ <= txn->GetReadTs()){
      //judge if the 
      if(!logs.empty()){
        auto log_ = logs.back();
        if(log_.ts_ > log.ts_){
          break;
        }
      }
      logs.push_back(log);
    }
    // get the link and get the next log
    txn_ = map[log.prev_version_.prev_txn_];
    log = txn_->GetUndoLog(log.prev_version_.prev_log_idx_);
  }

  // the last log 
  if(log.ts_<= txn->GetReadTs()){
    if(!logs.empty()){
        auto log_ = logs.back();
        if(log.ts_ == log_.ts_){
          logs.push_back(log);
        }
    }else{
      logs.push_back(log);
    }
  }
  if(logs.empty()){
    return std::nullopt;
  }
  return logs;

  UNIMPLEMENTED("not implemented");
}


/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  UNIMPLEMENTED("not implemented");
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  UNIMPLEMENTED("not implemented");
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
