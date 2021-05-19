//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  while (lock_queue.upgrading_ || rid_exclusive_[rid]) {
    lock_queue.cv_.wait(ul);
  }
  assert(!lock_queue.upgrading_ && !rid_exclusive_[rid]);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  lock_queue.request_queue_.emplace_back(LockRequest{txn->GetTransactionId(), LockMode::SHARED});
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  // check upgrade ,exclusive and shared
  while (lock_queue.upgrading_ || rid_exclusive_[rid] || !lock_queue.request_queue_.empty()) {
    lock_queue.cv_.wait(ul);
  }
  assert(!lock_queue.upgrading_ && !rid_exclusive_[rid] && lock_queue.request_queue_.empty());
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  lock_queue.request_queue_.emplace_back(LockRequest{txn->GetTransactionId(), LockMode::EXCLUSIVE});
  txn->GetExclusiveLockSet()->emplace(rid);
  rid_exclusive_[rid] = true;
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  if (lock_queue.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // set upgrade true before check
  lock_queue.upgrading_ = true;
  while (lock_queue.request_queue_.size() != 1) {
    lock_queue.cv_.wait(ul);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // modify the state in request queue
  assert(lock_queue.request_queue_.size() == 1);
  LockRequest &request_item = lock_queue.request_queue_.front();
  assert(request_item.txn_id_ == txn->GetTransactionId());
  request_item.lock_mode_ = LockMode::EXCLUSIVE;
  request_item.granted_ = true;
  // modify the txn
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  // modify flag
  rid_exclusive_[rid] = true;
  lock_queue.upgrading_ = false;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  std::list<LockRequest> &request_queue = lock_queue.request_queue_;
  LockMode txn_lockmode;
  if (txn->IsSharedLocked(rid)) {
    txn_lockmode = LockMode::SHARED;
  } else if (txn->IsExclusiveLocked(rid)) {
    txn_lockmode = LockMode::EXCLUSIVE;
  } else {
    assert(0);
  }
  auto itor = request_queue.begin();
  while (itor != request_queue.end()) {
    if (itor->txn_id_ == txn->GetTransactionId()) {
      assert(itor->lock_mode_ == txn_lockmode);
      request_queue.erase(itor);
      break;
    }
    itor++;
  }
  switch (txn_lockmode) {
    case LockMode::SHARED: {
      txn->GetSharedLockSet()->erase(rid);
      if (request_queue.empty()) {
        // notiy_all or notify_one, if there is a e_lock waiting and  a upgrade_lock waiting
        lock_queue.cv_.notify_all();
      }
      break;
    }
    case LockMode::EXCLUSIVE: {
      txn->GetExclusiveLockSet()->erase(rid);
      assert(request_queue.empty());
      rid_exclusive_[rid] = false;
      lock_queue.cv_.notify_all();
      break;
    }
  }
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

}  // namespace bustub
