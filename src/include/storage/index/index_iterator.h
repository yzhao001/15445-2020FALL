//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator(BufferPoolManager *buffer_pool_manager, int idx = -1,
                         B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = nullptr);
  ~IndexIterator();

  // what's the definition of isEnd()
  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    return this->leaf_node == itr.leaf_node && this->kv_idx == itr.kv_idx;
  }

  bool operator!=(const IndexIterator &itr) const {
    return this->leaf_node != itr.leaf_node || this->kv_idx != itr.kv_idx;
  }

 private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_;
  int kv_idx;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node;
};

}  // namespace bustub
