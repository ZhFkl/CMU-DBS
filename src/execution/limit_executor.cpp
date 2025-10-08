//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Construct a new LimitExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The limit plan to be executed
 * @param child_executor The child executor from which limited tuples are pulled
 */
LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(std::move(plan)),child_(std::move(child_executor))
    {
  //UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the limit */
void LimitExecutor::Init() { 
  child_->Init();
  limit_ = plan_->GetLimit();
  nums = 0;
 // UNIMPLEMENTED("TODO(P3): Add implementation."); 

}

/**
 * Yield the next tuple from the limit.
 * @param[out] tuple The next tuple produced by the limit
 * @param[out] rid The next tuple RID produced by the limit
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  while(true){
    Tuple child_tuple;
    RID child_rid;
    if(nums < limit_ &&child_->Next(&child_tuple,&child_rid))
    {
      *tuple = child_tuple;
      nums++;
      return true;
    }
    return false;
  }
  
  
  
  
  
  UNIMPLEMENTED("TODO(P3): Add implementation."); }

}  // namespace bustub
