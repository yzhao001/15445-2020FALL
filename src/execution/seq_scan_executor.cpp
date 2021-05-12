//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_heap(nullptr), itor(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  Catalog *catalog = this->GetExecutorContext()->GetCatalog();
  table_info = catalog->GetTable(plan_->GetTableOid());
  table_heap = table_info->table_.get();
  itor = table_heap->Begin(GetExecutorContext()->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (itor == table_heap->End()) {
    return false;
  }
  const Schema *output_schema = plan_->OutputSchema();
  RID original_rid = itor->GetRid();
  std::vector<Value> vals;
  for (const auto &col : output_schema->GetColumns()) {
    Value col_val = col.GetExpr()->Evaluate(&(*itor), &(table_info->schema_));
    vals.push_back(col_val);
  }
  ++itor;
  Tuple out_tuple(vals, output_schema);
  const AbstractExpression *predict = plan_->GetPredicate();
  if (predict == nullptr || predict->Evaluate(&out_tuple, output_schema).GetAs<bool>()) {
    *tuple = out_tuple;
    *rid = original_rid;
    return true;
  }
  return Next(tuple, rid);
}

}  // namespace bustub
