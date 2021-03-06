//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  table_heap = table_info_->table_.get();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  child_executor_->Init();
  Transaction *transaction = GetExecutorContext()->GetTransaction();
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  Tuple old_tuple;
  Tuple new_tuple;
  RID _rid;
  try {
    while (child_executor_->Next(&old_tuple, &_rid)) {
      if (lock_mgr != nullptr) {
        if (transaction->IsSharedLocked(_rid)) {
          lock_mgr->LockUpgrade(transaction, _rid);
        } else if (!transaction->IsExclusiveLocked(_rid)) {
          lock_mgr->LockExclusive(transaction, _rid);
        }
      }
      new_tuple = GenerateUpdatedTuple(old_tuple);
      table_heap->UpdateTuple(new_tuple, _rid, transaction);
      // transaction->GetWriteSet()->push_back(TableWriteRecord(_rid, WType::UPDATE, old_tuple, table_heap));
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
