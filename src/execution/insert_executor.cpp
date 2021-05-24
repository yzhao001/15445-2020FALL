//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  catalog = this->GetExecutorContext()->GetCatalog();
  table_info = catalog->GetTable(plan_->TableOid());
  table_heap = table_info->table_.get();
  transaction = GetExecutorContext()->GetTransaction();
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // no child plan
  if (plan_->IsRawInsert()) {
    for (const auto &row_value : plan_->RawValues()) {
      insert_table_index(Tuple(row_value, &table_info->schema_));
    }
    return false;
  }
  // with a child executor
  std::vector<Tuple> child_tuple;
  if (do_child_executor(&child_tuple)) {
    for (auto &ele : child_tuple) {
      insert_table_index(ele);
    }
    return false;
  }
  throw Exception(ExceptionType::CHILD_EXE_FAIL, "InsertExecutor:child execute error.");
  return false;
}

void InsertExecutor::insert_table_index(Tuple cur_tuple) {
  RID cur_rid;
  // insert table
  bool is_insert = table_heap->InsertTuple(cur_tuple, &cur_rid, transaction);
  transaction->GetWriteSet()->push_back(TableWriteRecord(cur_rid, WType::INSERT, cur_tuple, table_heap));
  if (!is_insert) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertExecutor:no enough space for this tuple.");
  }
  // insert indexes
  for (const auto &index_info : catalog->GetTableIndexes(table_info->name_)) {
    auto bPlusTree_Index =
        /*dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>*/ (index_info->index_.get());
    bPlusTree_Index->InsertEntry(
        cur_tuple.KeyFromTuple(table_info->schema_, *bPlusTree_Index->GetKeySchema(), bPlusTree_Index->GetKeyAttrs()),
        cur_rid, transaction);
    transaction->GetIndexWriteSet()->push_back(
        IndexWriteRecord(cur_rid, plan_->TableOid(), WType::INSERT, cur_tuple, index_info->index_oid_, catalog));
  }
}
// get tuple from child
bool InsertExecutor::do_child_executor(std::vector<Tuple> *child_tuple) {
  child_executor_->Init();
  try {
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      (*child_tuple).push_back(tuple);
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::CHILD_EXE_FAIL, "InsertExecutor:child execute error.");
    return false;
  }
  return true;
}

}  // namespace bustub
