//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };
  
  auto const CombineTuple(const Tuple* left,const Tuple* right,bool null) ->  Tuple;
 private:
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  AbstractPlanNodeRef left_plan_node_;
  AbstractPlanNodeRef right_plan_node_;
  
  //to track the result and updata
  std::queue<Tuple> match_queue_;
  Tuple current_left_tuple_;
  bool has_current_left_;
};

}  // namespace bustub
