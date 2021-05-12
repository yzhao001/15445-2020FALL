//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  catalog = GetExecutorContext()->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  table_heap = table_info_->table_.get();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  child_executor_->Init();
  Transaction *transaction = GetExecutorContext()->GetTransaction();
  Tuple _tuple;
  RID _rid;
  try {
    while (child_executor_->Next(&_tuple, &_rid)) {
      table_heap->MarkDelete(_rid, transaction);
      for (const auto &index_info : catalog->GetTableIndexes(table_info_->name_)) {
        auto bPlusTree_Index =
            /*dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>*/ (index_info->index_.get());
        bPlusTree_Index->DeleteEntry(
            _tuple.KeyFromTuple(table_info_->schema_, *bPlusTree_Index->GetKeySchema(), bPlusTree_Index->GetKeyAttrs()),
            _rid, transaction);
      }
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::CHILD_EXE_FAIL, "InsertExecutor:child execute error.");
  }
  return false;
}

}  // namespace bustub
