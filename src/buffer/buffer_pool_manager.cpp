//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  latch_.lock();

  frame_id_t frame_num = -1;
  Page *ptr = nullptr;
  auto itor = page_table_.find(page_id);
  if (itor != page_table_.end()) {
    frame_num = itor->second;
    ptr = pages_ + frame_num;
    replacer_->Pin(frame_num);
    ptr->pin_count_++;
    latch_.unlock();
    return ptr;
  }
  page_id_t dirty_pageId = -1;
  if (!free_list_.empty()) {
    frame_num = free_list_.back();
    free_list_.pop_back();
    ptr = pages_ + frame_num;
  } else if (replacer_->Victim(&frame_num)) {
    ptr = pages_ + frame_num;
    assert(ptr->GetPinCount() == 0);
    page_table_.erase(ptr->GetPageId());
    if (ptr->IsDirty()) {
      dirty_pageId = ptr->GetPageId();
    }
  } else {
    latch_.unlock();

    return nullptr;
  }
  page_table_[page_id] = frame_num;
  ptr->page_id_ = page_id;
  ptr->pin_count_ = 1;
  ptr->is_dirty_ = false;
  // io operation
  if (dirty_pageId != -1) {
    disk_manager_->WritePage(dirty_pageId, ptr->GetData());
  }
  ptr->ResetMemory();
  disk_manager_->ReadPage(page_id, ptr->GetData());
  latch_.unlock();

  return ptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  frame_id_t frame_num = page_table_[page_id];
  // check for lock ,release lock before unpin reach 0
  //  assert(pages_[frame_num].GetPinCount() > 0);
  Page *ptr = pages_ + frame_num;
  ptr->is_dirty_ |= is_dirty;
  if (--ptr->pin_count_ == 0) {
    replacer_->Unpin(frame_num);
  }
  latch_.unlock();

  return true;
}
// flush the page whether the page is dirty or not
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();

    return false;
  }
  frame_id_t frame_num = page_table_[page_id];
  Page *ptr = pages_ + frame_num;
  ptr->is_dirty_ = false;
  // io
  disk_manager_->WritePage(page_id, ptr->GetData());
  latch_.unlock();

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  // all pined
  if (free_list_.empty() && replacer_->Size() == 0) {
    *page_id = INVALID_PAGE_ID;
    latch_.unlock();

    return nullptr;
  }
  frame_id_t frame_num = -1;
  Page *ptr = nullptr;
  page_id_t dirty_pageId = -1;
  if (!free_list_.empty()) {
    frame_num = free_list_.back();
    free_list_.pop_back();
    ptr = pages_ + frame_num;
  } else if (replacer_->Victim(&frame_num)) {
    ptr = pages_ + frame_num;
    assert(ptr->GetPinCount() == 0);
    page_table_.erase(ptr->GetPageId());
    if (ptr->IsDirty()) {
      dirty_pageId = ptr->GetPageId();
    }
  }
  page_id_t newid = disk_manager_->AllocatePage();
  page_table_[newid] = frame_num;
  assert(ptr != nullptr);
  ptr->page_id_ = newid;
  ptr->pin_count_ = 1;
  ptr->is_dirty_ = false;
  // io
  if (dirty_pageId != -1) {
    disk_manager_->WritePage(dirty_pageId, ptr->GetData());
  }
  ptr->ResetMemory();
  disk_manager_->WritePage(newid, ptr->GetData());
  *page_id = newid;
  latch_.unlock();

  return ptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  auto itor = page_table_.find(page_id);
  if (itor == page_table_.end()) {
    latch_.unlock();

    return true;
  }
  frame_id_t frame_num = itor->second;
  Page *ptr = pages_ + frame_num;
  if (ptr->GetPinCount() > 0) {
    latch_.unlock();

    return false;
  }
  assert(ptr->GetPinCount() == 0);
  page_table_.erase(page_id);
  ptr->page_id_ = INVALID_PAGE_ID;
  // ptr->pin_count_ = 0;
  replacer_->Pin(frame_num);
  disk_manager_->DeallocatePage(page_id);
  free_list_.push_back(frame_num);
  ptr->is_dirty_ = false;

  // io
  if (ptr->IsDirty()) {
    disk_manager_->WritePage(page_id, ptr->GetData());
  }
  ptr->ResetMemory();

  latch_.unlock();

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  latch_.lock();
  page_id_t page_num = -1;
  frame_id_t frame_num = -1;
  Page *ptr = nullptr;
  for (const auto &itor : page_table_) {
    page_num = itor.first;
    frame_num = itor.second;
    ptr = pages_ + frame_num;
    // if (ptr->IsDirty()) {
    disk_manager_->WritePage(page_num, ptr->GetData());
    ptr->is_dirty_ = false;
    //}
  }
  latch_.unlock();
}

}  // namespace bustub
