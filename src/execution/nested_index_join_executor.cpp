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

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple outer_tuple;
  Tuple inner_tuple;
  RID outer_rid;
  if (child_executor_->Next(&outer_tuple, &outer_rid)) {
    Tuple key_tuple = outer_tuple.KeyFromTuple(*plan_->OuterTableSchema(), innerIndex_info->key_schema_,
                                               getKeyattrs(plan_->OuterTableSchema(), &innerIndex_info->key_schema_));
    std::vector<RID> value_rid;
    innerIndex_info->index_->ScanKey(key_tuple, &value_rid, GetExecutorContext()->GetTransaction());
    // no need to predict
    assert(value_rid.size() == 1);
    innerTable_info->table_->GetTuple(value_rid[0], &inner_tuple, GetExecutorContext()->GetTransaction());
    *tuple = merge_left_right(outer_tuple, plan_->OuterTableSchema(), inner_tuple, plan_->OuterTableSchema(),
                              GetOutputSchema());
    return true;
  }
  return false;
}
std::vector<uint32_t> NestIndexJoinExecutor::getKeyattrs(const Schema *outer_schema, Schema *inner_schema) {
  std::vector<uint32_t> key_attrs;
  for (const auto &col : inner_schema->GetColumns()) {
    key_attrs.push_back(outer_schema->GetColIdx(col.GetName()));
  }
  return key_attrs;
}

Tuple NestIndexJoinExecutor::merge_left_right(const Tuple &left, const Schema *left_schema, const Tuple &right,
                                              const Schema *right_schema, const Schema *output_schema) {
  std::vector<Value> values;
  getValues(left, left_schema, &values);
  getValues(right, right_schema, &values);
  return Tuple(values, output_schema);
}

void NestIndexJoinExecutor::getValues(const Tuple &tuple, const Schema *schema, std::vector<Value> *values) {
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    (*values).push_back(tuple.GetValue(schema, i));
  }
}

}  // namespace bustub
