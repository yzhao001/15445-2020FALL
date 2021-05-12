/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, int idx, B_PLUS_TREE_LEAF_PAGE_TYPE *leaf)
    : buffer_pool_manager_(buffer_pool_manager), kv_idx(idx), leaf_node(leaf) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_node != nullptr && buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  return leaf_node == nullptr;
  // throw std::runtime_error("unimplemented");
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  assert(leaf_node != nullptr && kv_idx >= 0 && kv_idx < static_cast<int>(LEAF_PAGE_SIZE));
  return leaf_node->GetItem(kv_idx);
  // throw std::runtime_error("unimplemented");
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  assert(leaf_node != nullptr);
  kv_idx++;
  if (kv_idx < leaf_node->GetSize()) {
    return *this;
  }
  page_id_t next_page = leaf_node->GetNextPageId();
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  if (next_page == INVALID_PAGE_ID) {
    kv_idx = -1;
    leaf_node = nullptr;
    return *this;
  }
  Page *pe = buffer_pool_manager_->FetchPage(next_page);
  assert(pe != nullptr);
  leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(pe->GetData());
  kv_idx = 0;
  return *this;
  // throw std::runtime_error("unimplemented");
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
