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
  child_executor_->Init();
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple outer_tuple;
  Tuple inner_tuple;
  RID outer_rid;
  if (child_executor_->Next(&outer_tuple, &outer_rid)) {
    std::vector<Value> vals{
        plan_->Predicate()->GetChildAt(0)->Evaluate(&outer_tuple, child_executor_->GetOutputSchema())};
    Tuple key_tuple(vals, &innerIndex_info->key_schema_);
    // get RID
    std::vector<RID> value_rid;
    innerIndex_info->index_->ScanKey(key_tuple, &value_rid, GetExecutorContext()->GetTransaction());
    if (value_rid.empty()) {
      return Next(tuple, rid);
    }
    innerTable_info->table_->GetTuple(value_rid[0], &inner_tuple, GetExecutorContext()->GetTransaction());
    // check if match
    bool ismatch = plan_->Predicate()
                       ->EvaluateJoin(&outer_tuple, plan_->OuterTableSchema(), &inner_tuple, &innerTable_info->schema_)
                       .GetAs<bool>();
    if (!ismatch) {
      return Next(tuple, rid);
    }
    // get output tuple
    std::vector<Value> output_row;
    for (const auto &col : GetOutputSchema()->GetColumns()) {
      output_row.push_back(col.GetExpr()->EvaluateJoin(&outer_tuple, plan_->OuterTableSchema(), &inner_tuple,
                                                       &innerTable_info->schema_));
    }
    *tuple = Tuple(output_row, GetOutputSchema());
    return true;
  }
  return false;
}

}  // namespace bustub
