//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lg(lru_mutex);
  if (umap.empty()) {
    *frame_id = -1;
    return false;
  }
  frame_id_t num_remove = li.back();
  li.pop_back();
  umap.erase(num_remove);
  *frame_id = num_remove;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lg(lru_mutex);
  auto itor = umap.find(frame_id);
  if (itor != umap.end()) {
    li.erase(itor->second);
    umap.erase(itor);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lg(lru_mutex);
  auto itor = umap.find(frame_id);
  if (itor != umap.end()) {
    return;
  }
  li.push_front(frame_id);
  umap[frame_id] = li.begin();
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lg(lru_mutex);
  return umap.size();
}

}  // namespace bustub
