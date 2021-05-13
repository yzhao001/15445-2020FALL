//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  std::vector<Tuple> left_tuple;
  std::vector<Tuple> right_tuple;
  do_child_executor(&left_tuple, left_executor_.get());
  do_child_executor(&right_tuple, right_executor_.get());
  for (auto &l1 : left_tuple) {
    for (auto &r1 : right_tuple) {
      // predicate null or evaluate true
      if (plan_->Predicate() == nullptr ||
          plan_->Predicate()
              ->EvaluateJoin(&l1, left_executor_->GetOutputSchema(), &r1, right_executor_->GetOutputSchema())
              .GetAs<bool>()) {
        res.push_back(merge_left_right(l1, left_executor_->GetOutputSchema(), r1, right_executor_->GetOutputSchema(),
                                       GetOutputSchema()));
      }
    }
  }
}
Tuple NestedLoopJoinExecutor::merge_left_right(const Tuple &left, const Schema *left_schema, const Tuple &right,
                                               const Schema *right_schema, const Schema *output_schema) {
  std::vector<Value> values;
  getValues(left, left_schema, &values);
  getValues(right, right_schema, &values);
  return Tuple(values, output_schema);
}

void NestedLoopJoinExecutor::getValues(const Tuple &tuple, const Schema *schema, std::vector<Value> *values) {
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    (*values).push_back(tuple.GetValue(schema, i));
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (idx < res.size()) {
    *tuple = res[idx++];
    return true;
  }
  return false;
}

bool NestedLoopJoinExecutor::do_child_executor(std::vector<Tuple> *child_tuple, AbstractExecutor *child_executor) {
  child_executor->Init();
  try {
    Tuple tuple;
    RID rid;
    while (child_executor->Next(&tuple, &rid)) {
      (*child_tuple).push_back(tuple);
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::CHILD_EXE_FAIL, "InsertExecutor:child execute error.");
    return false;
  }
  return true;
}

}  // namespace bustub
