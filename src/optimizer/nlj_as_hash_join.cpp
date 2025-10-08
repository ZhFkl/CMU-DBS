//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"
#include "execution/expressions/logic_expression.h"
#include "execution/expressions/comparison_expression.h"

namespace bustub {

/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for Spring 2025: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  // which means 
  std::vector<AbstractPlanNodeRef> children;
  for(const auto &child : plan->GetChildren()){
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if(optimized_plan->GetType() == PlanType:: NestedLoopJoin){
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2,"NLJ should have 2 child the left and the right");
    
    auto predicate = nlj_plan.Predicate();
    auto equi_conditions = ExtractEquiJoinConditions(predicate);
    if(!equi_conditions.has_value()){
      return optimized_plan;
    }
    auto [left_key_exprs,right_key_exprs] = equi_conditions.value();
    if(left_key_exprs.empty()|| right_key_exprs.empty()){
      return optimized_plan;
    }

    return std::make_shared<HashJoinPlanNode>(
      nlj_plan.output_schema_,
      nlj_plan.GetLeftPlan(),
      nlj_plan.GetRightPlan(),
      std::move(left_key_exprs),
      std::move(right_key_exprs),
      nlj_plan.GetJoinType()
    );
  
  }
    
  return optimized_plan;

}

auto Optimizer::ExtractEquiJoinConditions(const AbstractExpressionRef &expr)
 ->std::optional<std::pair<std::vector<AbstractExpressionRef>,std::vector<AbstractExpressionRef>>>
 {
    if(const auto* conj = dynamic_cast<const LogicExpression *>(expr.get());conj != nullptr){
      if(conj->logic_type_ !=  LogicType::And){
        return std::nullopt;
      }
  
    auto left_res = ExtractEquiJoinConditions(expr->children_[0]);
    auto right_res = ExtractEquiJoinConditions(expr->children_[1]);
    if(!left_res.has_value() || !right_res.has_value()){
      return std::nullopt;
    }
    auto [left_exprs0,right_exprs0] = left_res.value();
    auto [left_exprs1,right_exprs1] = right_res.value();
    left_exprs0.insert(left_exprs0.end(),left_exprs1.begin(),left_exprs1.end());
    right_exprs0.insert(right_exprs0.end(),right_exprs1.begin(),right_exprs1.end());
    return std::make_pair(left_exprs0,right_exprs0);
  }
    //condition 2 this is a compare equal cond
    if(const auto  *cmp = dynamic_cast<const ComparisonExpression *>(expr.get());cmp != nullptr){
      if(cmp->comp_type_ != ComparisonType::Equal){
        return std::nullopt;
      }
      const auto *left_col = dynamic_cast<const ColumnValueExpression *>(cmp->children_[0].get());
      const auto *right_col = dynamic_cast<const ColumnValueExpression *> (cmp->children_[1].get());
      if(left_col == nullptr || right_col == nullptr){
        return std::nullopt;
      }
      if(left_col->GetTupleIdx() == 0 && right_col->GetTupleIdx() == 1 ){
          return std::make_pair(
            std::vector<AbstractExpressionRef>{cmp->children_[0]},
            std::vector<AbstractExpressionRef>{cmp->children_[1]}
        );
      }
      if(left_col->GetTupleIdx() == 1 && right_col->GetTupleIdx() == 0){
        return std::make_pair(
          std::vector<AbstractExpressionRef>{cmp->children_[1]},
          std::vector<AbstractExpressionRef>{cmp->children_[0]}
        );
      }

    }
    return std::nullopt;

 }

}  // namespace bustub
