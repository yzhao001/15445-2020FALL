//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), itor(nullptr) {}

void IndexScanExecutor::Init() {
  Catalog *catalog = this->GetExecutorContext()->GetCatalog();
  index_info = catalog->GetIndex(plan_->GetIndexOid());
  table_info = catalog->GetTable(index_info->table_name_);
  table_heap = table_info->table_.get();
  itor = dynamic_cast<BPlusTree_IndexIterator_TYPE *>(index_info->index_.get())->GetBeginIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (itor == dynamic_cast<BPlusTree_IndexIterator_TYPE *>(index_info->index_.get())->GetEndIterator()) {
    return false;
  }
  RID cur_rid = (*itor).second;
  Tuple tuple_all;
  bool get_tuple = table_heap->GetTuple(cur_rid, &tuple_all, GetExecutorContext()->GetTransaction());
  if (!get_tuple) {
    throw Exception(ExceptionType::TUPLE_ERROR, "IndexScanExecutor:can not get this tuple by RID.");
  }
  const Schema *output_schema = plan_->OutputSchema();
  std::vector<Value> vals;
  for (const auto &col : output_schema->GetColumns()) {
    Value col_val = col.GetExpr()->Evaluate(&tuple_all, &(table_info->schema_));
    vals.push_back(col_val);
  }
  ++itor;
  Tuple out_tuple(vals, output_schema);
  bool ismatch = plan_->GetPredicate()->Evaluate(&out_tuple, output_schema).GetAs<bool>();
  if (ismatch) {
    *tuple = out_tuple;
    *rid = cur_rid;
    return true;
  }
  return Next(tuple, rid);
}

}  // namespace bustub
