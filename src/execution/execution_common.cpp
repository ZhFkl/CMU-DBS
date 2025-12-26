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
#include "type/value_factory.h"

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
    bool is_delete = base_meta.is_deleted_;
    Tuple tuple_ = base_tuple; 
    if(undo_logs.empty()){
      throw std::runtime_error("in reconstructTuple the undolog is empty is wrong\n");
    }
    for(auto log : undo_logs){
      is_delete = log.is_deleted_;
      tuple_ = GetUnlogTuple(*schema,&tuple_,log); 
    }
    if(is_delete || !IsnotNull(schema,tuple_)){
      return std::nullopt;
    }
    // if the base_value is null value then we return nullopt;
    return tuple_;
}


auto IsnotNull(const Schema* schema, const Tuple tuple) ->bool{
  bool IsnotNull = false;
  for(size_t i = 0; i< schema->GetColumnCount();i++){
    if(!tuple.GetValue(schema,i).IsNull()){
      IsnotNull = true;
      break;
    }
  }
  return IsnotNull;
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
  // first conditoin : the transaction is being modifing 
  if(base_meta.ts_ > TXN_START_ID ){
    // the tuple is modified by the txn
    if(base_meta.ts_ == txn->GetTransactionId()){
      return std::nullopt;
    }
    // the tuple was not modified by this txn
    // first roll back to the state before the modifing 
        auto link = txn_mgr->GetUndoLink(rid);
      // if a tuple don't have undolog which means it's never exist ;
      if(!link.has_value()){return std::nullopt;}

       auto undolog = txn_mgr->GetUndoLog(link.value());
       logs.push_back(undolog);
       UndoLink link_ = undolog.prev_version_;
       while(link_.IsValid()){
          auto undolog_ = txn_mgr->GetUndoLog(link_);
          if(undolog_.ts_ > txn->GetReadTs()){
            logs.push_back(undolog_);
          }else{
            break;
          }
          link_ = undolog_.prev_version_;
       }
  }
  else{
      auto link = txn_mgr->GetUndoLink(rid);
      // if a tuple don't have undolog which means it's never exist ;
      if(!link.has_value()){return std::nullopt;}

      auto link_ = link.value();
      while(link_.IsValid()){
        auto undolog = txn_mgr->GetUndoLog(link_);
        if(undolog.ts_ > txn->GetReadTs()){
          logs.push_back(undolog);
        }else{
          break;
        }
        link_ = undolog.prev_version_;
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


    // compare the data that has been modified 

    std::vector<Value> modif_val;
    std::vector<bool> modif_arr;
    std::vector<Column> modif_schema;
    if(base_tuple == nullptr){
      // if  the base tuple is
      //which means  the tuple is inserted the first time 
      modif_arr.resize(schema->GetColumnCount(),true);
      for(size_t i = 0;i < schema->GetColumnCount();i++){
        modif_val.push_back(ValueFactory::GetNullValueByType(schema->GetColumn(i).GetType()));
      }
      Tuple undo_tuple(modif_val,schema);
      undo_tuple.SetRid(target_tuple->GetRid());
      return UndoLog{false,modif_arr,undo_tuple,ts,{}};
    }
    if(target_tuple == nullptr){
      modif_arr.resize(schema->GetColumnCount(),true);
      return UndoLog{false,modif_arr,*base_tuple,ts,prev_version};
    }
    for(size_t i = 0; i < schema->GetColumnCount();i++){
      auto old_val = base_tuple->GetValue(schema,i);
      auto new_val = target_tuple->GetValue(schema,i);
      if(new_val.CompareExactlyEquals(old_val)){
        modif_arr.push_back(false);
        continue;
      }
      modif_val.push_back(old_val);
      modif_arr.push_back(true);
      modif_schema.push_back(schema->GetColumn(i));
    }
    Schema schema_(modif_schema);
    Tuple undo_tuple(modif_val,&schema_);
    undo_tuple.SetRid(base_tuple->GetRid());
    return UndoLog{false,modif_arr,undo_tuple,ts,prev_version};



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
  if(base_tuple == nullptr || target_tuple == nullptr){
    return log;
  }
  if(IsTupleContentEqual(*base_tuple,*target_tuple)){
    return log;
  }
  
  //not only compare the 
  std::vector<Value> modif_val;
  std::vector<bool> modif_arr;
  std::vector<Column> modif_schema;
  auto log_schema = GetUnlogSchema(schema,log);
  size_t pos = 0;
  for(size_t i = 0 ; i < schema->GetColumnCount();i++){
    if(log.modified_fields_[i]){
      modif_arr.push_back(true);
      modif_val.push_back(log.tuple_.GetValue(&log_schema,pos++));
      modif_schema.push_back(schema->GetColumn(i));
      continue;
    }
    auto old_val = base_tuple->GetValue(schema,i);
    auto new_val = target_tuple->GetValue(schema,i);
    if(old_val.CompareExactlyEquals(new_val)){
      modif_arr.push_back(false);
      continue;
    }
    modif_arr.push_back(true);
    modif_val.push_back(old_val);
    modif_schema.push_back(schema->GetColumn(i));

  }
  Schema schema_(modif_schema);
  return UndoLog{false,modif_arr,Tuple(modif_val,&schema_),log.ts_,log.prev_version_};
  UNIMPLEMENTED("not implemented");
}


auto GetUnlogTuple(const  Schema schema,  Tuple* base_tuple,const UndoLog & log) -> Tuple {
      std::vector<Value> tuple_val;
      size_t pos = 0;
      auto log_schema = GetUnlogSchema(&schema,log);
      for(size_t i = 0;i < schema.GetColumnCount();i++){
        if(log.modified_fields_[i]){
          tuple_val.push_back(log.tuple_.GetValue(&log_schema,pos++));
          continue;
        }
        if(base_tuple == nullptr){
          tuple_val.push_back(ValueFactory::GetNullValueByType(schema.GetColumn(i).GetType()));
        }else{
          tuple_val.push_back(base_tuple->GetValue(&schema,i));
        }
      }
      Tuple undologtuple(tuple_val,&schema);
      undologtuple.SetRid(log.tuple_.GetRid());
      return undologtuple;
}


auto GetUnlogSchema(const Schema *schema, const UndoLog & log) ->Schema{
      std::vector<Value> tuple_val; 
      std::vector<Column> col;
      for(size_t i = 0;i < schema->GetColumnCount();i++){
        if(log.modified_fields_[i]){
          col.push_back(schema->GetColumn(i));
          continue;
        }
      }
      return Schema(col);
}

bool check_double_write_conflict(Transaction* txn, TupleMeta tuple_meta,TransactionManager * txn_mgr,const RID tuple_rid,std::vector<uint32_t> modified){
  if(tuple_meta.ts_ > TXN_START_ID && tuple_meta.ts_ != txn->GetTransactionId()){
    // the tuple is being modifiying then we can't update this tuple 
    txn->SetTainted();
    throw ExecutionException("this txn is a tianed");
    return false;
  }
  if(tuple_meta.is_deleted_ && txn->GetReadTs() < tuple_meta.ts_){
    txn->SetTainted();
    throw ExecutionException("this txn is a tianed");
    return false;
  }
  // third condition if a ,b modified  tuple then a commit, then b commit at this time a  
  
  if(tuple_meta.ts_ > txn->GetReadTs() && !modified.empty()){
    auto link = txn_mgr->GetUndoLink(tuple_rid);
    auto logs = CollectUndoLogs(tuple_rid,tuple_meta,Tuple::Empty(),link.value(),txn,txn_mgr);
    if(!logs.has_value()){
      return true;
    }
    for(auto const & log: logs.value()){
      for(auto const idx : modified){
        if(log.modified_fields_[idx]){
            txn->SetTainted();
            throw ExecutionException("this txn is a tianed");
            return false;
        }
      }
    }
  }

  return true;
  
}





void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);
  
  //get the tuple preversion info
  
  auto iter = table_heap->MakeEagerIterator();
  Schema schema_ = table_info->schema_;
  while(!iter.IsEnd() ){
    RID tuple_rid = iter.GetRID();
    auto [tuple_meta,tuple ] = iter.GetTuple();
    std::cout<< tuple_rid<<"  ts_="<<tuple_meta.ts_;
    std::cout<< " Tuple="<<tuple.ToString(&table_info->schema_)<<"  deleted:"<<tuple_meta.is_deleted_<<std::endl;
    //find the undolink and go the undolog 
    //since we get the undolink then we need to get the undolog
    auto link = txn_mgr->GetUndoLink(tuple_rid);
    if(!link.has_value()){
      ++iter;
      continue;
    }
    UndoLink link_ = link.value();
    while(link_.IsValid()){
      auto log_ = txn_mgr->GetUndoLog(link_);
      auto unlogtuple  = GetUnlogTuple(schema_,nullptr,log_);
      std::cout<<"   txn"<<(link_.prev_txn_^TXN_START_ID)<<"@"<<link_.prev_log_idx_<<"  ts="<<log_.ts_<<"   "<<unlogtuple.ToString(&schema_)<<"  deleted:"<<log_.is_deleted_<<std::endl;
      link_ = log_.prev_version_;
    }
    ++iter;
  }

  // how to debug the hook
  


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
