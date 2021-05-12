//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
  catalog = GetExecutorContext()->GetCatalog();
  innerTable_info = catalog->GetTable(plan_->GetInnerTableOid());
  innerIndex_info = catalog->GetIndex(plan_->GetIndexName(), innerTable_info->name_);
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) { return false; }

}  // namespace bustub
