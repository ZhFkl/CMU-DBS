//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"
#include "execution/expressions/constant_value_expression.h"
#include "storage/index/index_iterator.h"
namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  std::shared_ptr<TableInfo> table_info_;
  std::shared_ptr<IndexInfo> index_info_;
  std::unique_ptr<Index> index_;
  BPlusTreeIndexForTwoIntegerColumn *tree_;
  std::vector<Value> keys;
  bool is_full_scan =false;
  std::vector<RID> point_scan_rids_;
  size_t point_scan_idx_ = 0;
  std::optional<BPlusTreeIndexIteratorForTwoIntegerColumn> iter_;
  //IndexIterator<IntegerKeyType_BTree, IntegerValueType_BTree, IntegerComparatorType_BTree> iter_;
};
}  // namespace bustub
