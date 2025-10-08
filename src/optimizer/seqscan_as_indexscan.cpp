//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
namespace bustub {

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
    // optimize children first
    std::vector<AbstractPlanNodeRef> children;
    for(const auto &child : plan->GetChildren()){
      children.emplace_back(OptimizeSeqScanAsIndexScan(child));
    }
    //
    auto optimized_plan = plan->CloneWithChildren(std::move(children));
    if(optimized_plan->GetType() == PlanType::IndexScan){
      printf("Already an index scan plan node, skip seqscan to indexscan optimization\n");
      return optimized_plan;
    }
    if(optimized_plan->GetType() != PlanType::SeqScan){
      return optimized_plan;
    }

    // we have a seq scan plan node
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    auto table_name = seq_scan_plan.table_name_;
    auto indexes = catalog_.GetTableIndexes(table_name);
    if(indexes.size() == 0){
      return optimized_plan;
    }
    auto filter_predicate = seq_scan_plan.filter_predicate_;
    if(filter_predicate == nullptr){
      return optimized_plan;
    }
    for(const auto & index_info : indexes){
      const auto &index = index_info->index_;
      const auto &key_attr = index->GetKeyAttrs();
      if(key_attr.size() != 1){
        //we only support single column index
        continue;
      }
      const auto &key_idx = key_attr[0];
      auto constants = JudgeIndex(filter_predicate,key_idx);
      std::vector<AbstractExpressionRef> prev_keys;
      for(const auto &key : constants){
        prev_keys.push_back(std::make_shared<ConstantValueExpression>(key));
      }
      if(!constants.empty()){
        //we can use this index
        auto index_scan_plan = std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_,
                                                                   index_info->index_oid_,seq_scan_plan.filter_predicate_,
                                                                    prev_keys);
        return index_scan_plan;
      }
    }
      return optimized_plan;
    
  // TODO(P3): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
}

auto Optimizer::JudgeIndex(const AbstractExpressionRef &expr, uint32_t Index_key) -> std::vector<Value> {
  std::vector<Value> res;
  const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
  if(logic_expr != nullptr){
    // 2 condition 
    // or logic 
    if(logic_expr->logic_type_ == LogicType::Or){
      const auto &left_expr = logic_expr->children_[0];
      const auto &right_expr = logic_expr->children_[1];
      auto left_res = JudgeIndex(left_expr,Index_key);
      auto right_res = JudgeIndex(right_expr,Index_key);
      // if its a or logic , then we get the 
      if(!left_res.empty() || !right_res.empty()){
        res.insert(res.end(),left_res.begin(),left_res.end());
        res.insert(res.end(),right_res.begin(),right_res.end());
      }
      // delete the duplicate value
      std::sort(res.begin(),res.end(),[](const Value &a, const Value &b){
        return a.CompareLessThan(b) == CmpBool::CmpTrue;
      });
      auto last = std::unique(res.begin(),res.end(),[](const Value &a, const Value &b){
        return a.CompareEquals(b) == CmpBool::CmpTrue;
      });
      res.erase(last,res.end());
      return res;
    }
    // and logic 
  }

  
  const ComparisonExpression* cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if(cmp_expr == nullptr){
    return {};
  }
  if(cmp_expr->comp_type_ != ComparisonType::Equal){
    return {};
  }

  const AbstractExpressionRef &left_expr = cmp_expr->children_[0];
  const AbstractExpressionRef &right_expr = cmp_expr->children_[1];

  const ColumnValueExpression *col_expr = nullptr;
  const ConstantValueExpression *const_expr = nullptr;


  const auto *test_expr = dynamic_cast<const ColumnValueExpression *>(left_expr.get());
  if(test_expr == nullptr){
    col_expr = dynamic_cast<const ColumnValueExpression *>(right_expr.get());
    const_expr = dynamic_cast<const ConstantValueExpression *>(left_expr.get());
  } else{
    col_expr = dynamic_cast<const ColumnValueExpression *>(left_expr.get());
    const_expr = dynamic_cast<const ConstantValueExpression *>(right_expr.get());
  }
  if(col_expr == nullptr || const_expr == nullptr){
    return {};
  }
  if(col_expr->GetColIdx() != Index_key){
    return {};
  }
  res.push_back(const_expr->val_);
  return res;

  //获得了这个表达比较器可以通过evaluate

}
}  // namespace bustub
