//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 */
class NestIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new nested index join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the nested index join plan node
   * @param outer table child
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

  std::vector<uint32_t> getKeyattrs(const Schema *outer_schema, Schema *inner_schema);

 private:
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;
  // my data
  std::unique_ptr<AbstractExecutor> child_executor_;
  Catalog *catalog;
  TableMetadata *innerTable_info;
  IndexInfo *innerIndex_info;
  std::vector<uint32_t> Keyattrs{};
};
}  // namespace bustub
