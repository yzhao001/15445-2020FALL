//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * NestedLoopJoinExecutor joins two tables using nested loop.
 * The child executor can either be a sequential scan
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new NestedLoop join executor.
   * @param exec_ctx the executor context
   * @param plan the NestedLoop join plan to be executed
   * @param left_executor the child executor that produces tuple for the left side of join
   * @param right_executor the child executor that produces tuple for the right side of join
   *
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

  bool do_child_executor(std::vector<Tuple> &child_tuple, AbstractExecutor *child_executor);
  Tuple merge_left_right(Tuple &left, const Schema *left_schema, Tuple &right, const Schema *right_schema,
                         const Schema *output_schema);
  void getValues(Tuple &tuple, const Schema *schema, std::vector<Value> &values);

 private:
  /** The NestedLoop plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;
  Catalog *catalog;
  TableMetadata *table_info_;
  TableHeap *table_heap;

  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::vector<Tuple> res;  // return tuple in Next()
  uint32_t idx{0};
};
}  // namespace bustub
