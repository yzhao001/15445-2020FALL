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
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  std::unique_lock<std::mutex> ul(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  // get wait id in vec,build graph
  std::vector<txn_id_t> vec;
  while ((lock_queue.upgrading_ || rid_exclusive_[rid]) && txn->GetState() != TransactionState::ABORTED) {
    // build graph
    vec = wait_to_release(lock_queue.request_queue_);
    AddEdge(txn->GetTransactionId(), vec);
    // add rid the txn is waiting
    txn_to_rid[txn->GetTransactionId()] = rid;
    // wait on cv
    lock_queue.cv_.wait(ul);
  }
  if (!vec.empty()) {
    txn_to_rid.erase(txn->GetTransactionId());
    RemoveEdge(txn->GetTransactionId(), vec);
  }
  // found aborted after wake up,this is a deadlock node
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  txn->SetState(TransactionState::GROWING);
  lock_queue.request_queue_.emplace_back(LockRequest{txn->GetTransactionId(), LockMode::SHARED});
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  std::unique_lock<std::mutex> ul(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  // get wait id in vec,build graph
  std::vector<txn_id_t> vec;
  // check upgrade ,exclusive and shared
  while ((lock_queue.upgrading_ || rid_exclusive_[rid] || !lock_queue.request_queue_.empty()) &&
         txn->GetState() != TransactionState::ABORTED) {
    // build graph
    vec = wait_to_release(lock_queue.request_queue_);
    AddEdge(txn->GetTransactionId(), vec);
    // add rid the txn is waiting
    txn_to_rid[txn->GetTransactionId()] = rid;
    // wait on cv
    lock_queue.cv_.wait(ul);
  }
  if (!vec.empty()) {
    txn_to_rid.erase(txn->GetTransactionId());
    RemoveEdge(txn->GetTransactionId(), vec);
  }
  // found aborted after wake up,this is a deadlock node
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  txn->SetState(TransactionState::GROWING);
  lock_queue.request_queue_.emplace_back(LockRequest{txn->GetTransactionId(), LockMode::EXCLUSIVE});
  txn->GetExclusiveLockSet()->emplace(rid);
  rid_exclusive_[rid] = true;
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  std::unique_lock<std::mutex> ul(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  if (lock_queue.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  // set upgrade true before check
  lock_queue.upgrading_ = true;
  // get wait id in vec,build graph
  std::vector<txn_id_t> vec;
  while (lock_queue.request_queue_.size() != 1 && txn->GetState() != TransactionState::ABORTED) {
    // build graph
    vec = wait_to_release(lock_queue.request_queue_);
    AddEdge(txn->GetTransactionId(), vec);
    // add rid the txn is waiting
    txn_to_rid[txn->GetTransactionId()] = rid;
    // wait on cv
    lock_queue.cv_.wait(ul);
  }
  // remove the id
  if (!vec.empty()) {
    txn_to_rid.erase(txn->GetTransactionId());
    RemoveEdge(txn->GetTransactionId(), vec);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    lock_queue.upgrading_ = false;
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  txn->SetState(TransactionState::GROWING);
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
  LockMode txn_lockmode = txn->IsSharedLocked(rid) ? LockMode::SHARED : LockMode::EXCLUSIVE;
  auto itor = request_queue.begin();
  while (itor != request_queue.end()) {
    if (itor->txn_id_ == txn->GetTransactionId()) {
      assert(itor->lock_mode_ == txn_lockmode);
      request_queue.erase(itor);
      break;
    }
    itor++;
  }
  // set txn state to shrink,current state may be commited ,aborted or growing
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
  // notify
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
std::vector<txn_id_t> LockManager::wait_to_release(const std::list<LockRequest> &request_queue_) {
  assert(!request_queue_.empty());
  std::vector<txn_id_t> res;
  for (const auto &ele : request_queue_) {
    res.emplace_back(ele.txn_id_);
  }
  return res;
}

// cur waits wait
void LockManager::AddEdge(txn_id_t cur, const std::vector<txn_id_t> &wait) {
  for (auto ele : wait) {
    AddEdge(cur, ele);
  }
}

void LockManager::RemoveEdge(txn_id_t cur, const std::vector<txn_id_t> &wait) {
  for (auto ele : wait) {
    RemoveEdge(cur, ele);
  }
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  for (const auto &ele : waits_for_[t1]) {
    if (ele == t2) {
      return;
    }
  }
  waits_for_[t1].emplace_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.count(t1) == 0) {
    return;
  }
  auto &t1_list = waits_for_[t1];
  for (auto itor = t1_list.begin(); itor != t1_list.end(); itor++) {
    if (*itor == t2) {
      t1_list.erase(itor);
      if (t1_list.empty()) {
        waits_for_.erase(t1);
      }
      return;
    }
  }
}
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> res;
  for (const auto &ele : waits_for_) {
    for (const auto &value : ele.second) {
      res.emplace_back(std::make_pair(ele.first, value));
    }
  }
  return res;
}
bool LockManager::dfsUtil(txn_id_t id, std::unordered_set<txn_id_t> *visited, std::unordered_set<txn_id_t> *recstack) {
  if ((*visited).count(id) == 0) {
    (*visited).emplace(id);
    (*recstack).emplace(id);
    sort(waits_for_[id].begin(), waits_for_[id].end());
    for (const auto &next : waits_for_[id]) {
      if ((*visited).count(next) == 0 && dfsUtil(next, visited, recstack)) {
        return true;
      }
      if ((*recstack).count(next) != 0) {
        return true;
      }
    }
  }
  (*recstack).erase(id);
  return false;
}
bool LockManager::dfs(txn_id_t *txn_id) {
  std::vector<txn_id_t> key_wait;
  std::unordered_set<txn_id_t> visited;
  std::unordered_set<txn_id_t> recstack;
  for (const auto &ele : waits_for_) {
    key_wait.emplace_back(ele.first);
  }
  sort(key_wait.begin(), key_wait.end());
  for (const auto &ele : key_wait) {
    if (dfsUtil(ele, &visited, &recstack)) {
      // get the max id in recstack,max id means the youngest
      txn_id_t res = INT_MIN;
      assert(!recstack.empty());
      for (const auto &id : recstack) {
        res = std::max(res, id);
      }
      *txn_id = res;
      return true;
    }
  }
  return false;
}
bool LockManager::HasCycle(txn_id_t *txn_id) {
  std::unique_lock<std::mutex> l(latch_);
  return dfs(txn_id);
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      txn_id_t dead_txn_id;
      if (dfs(&dead_txn_id)) {
        Transaction *trans = TransactionManager::GetTransaction(dead_txn_id);
        trans->SetState(TransactionState::ABORTED);
        // modify waits_for
        // waits_for_.erase(dead_txn_id);
        // notify trans
        assert(txn_to_rid.count(dead_txn_id) != 0);
        lock_table_[txn_to_rid[dead_txn_id]].cv_.notify_all();
      }
    }
  }
}

}  // namespace bustub
