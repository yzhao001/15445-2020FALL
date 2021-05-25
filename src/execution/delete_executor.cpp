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
  transaction = GetExecutorContext()->GetTransaction();
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple _tuple;
  RID _rid;
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  try {
    while (child_executor_->Next(&_tuple, &_rid)) {
      if (lock_mgr != nullptr) {
        if (transaction->IsSharedLocked(_rid)) {
          lock_mgr->LockUpgrade(transaction, _rid);
        } else if (!transaction->IsExclusiveLocked(_rid)) {
          lock_mgr->LockExclusive(transaction, _rid);
        }
      }
      table_heap->MarkDelete(_rid, transaction);
      // transaction->GetWriteSet()->push_back(TableWriteRecord(_rid, WType::DELETE, _tuple, table_heap));
      for (const auto &index_info : catalog->GetTableIndexes(table_info_->name_)) {
        auto bPlusTree_Index =
            /*dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>*/ (index_info->index_.get());
        bPlusTree_Index->DeleteEntry(
            _tuple.KeyFromTuple(table_info_->schema_, *bPlusTree_Index->GetKeySchema(), bPlusTree_Index->GetKeyAttrs()),
            _rid, transaction);
        transaction->GetIndexWriteSet()->push_back(
            IndexWriteRecord(_rid, plan_->TableOid(), WType::DELETE, _tuple, index_info->index_oid_, catalog));
      }
      if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {
        lock_mgr->Unlock(transaction, _rid);
      }
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::CHILD_EXE_FAIL, "InsertExecutor:child execute error.");
  }
  return false;
}

}  // namespace bustub
