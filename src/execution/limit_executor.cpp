//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() { child_executor_->Init(); }

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple child_tuple;
  RID child_rid;
  try {
    while (child_executor_->Next(&child_tuple, &child_rid)) {
      if (num < plan_->GetOffset()) {
        num++;
        continue;
      } else if (num < plan_->GetOffset() + plan_->GetLimit()) {
        num++;
        *tuple = child_tuple;
        *rid = child_rid;
        return true;
      } else {
        return false;
      }
    }
  } catch (Exception &e) {
    Exception(ExceptionType::CHILD_EXE_FAIL, "InsertExecutor:child execute error.");
  }
  return false;
}

}  // namespace bustub
