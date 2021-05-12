//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_leaf_page.h"
#include <sstream>
#include "common/exception.h"
#include "common/rid.h"
namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  assert(static_cast<uint32_t>(max_size) <= LEAF_PAGE_SIZE);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int left = 0;
  int right = GetSize() - 1;
  // find right border
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(array[mid].first, key) > 0) {
      right = mid - 1;
    } else if (comparator(array[mid].first, key) < 0) {
      left = mid + 1;
    } else {
      return mid;
    }
  }
  // maybe left == getsize(),
  assert(left < GetMaxSize());
  return left;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index >= 0 && index < GetSize());

  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  assert(index >= 0 && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  assert(GetSize() + 1 <= GetMaxSize());
  int idx = KeyIndex(key, comparator);
  if (comparator(array[idx].first, key) == 0) {
    array[idx].second = value;
    return GetSize();
  }
  // if idx == getsize(), means the key to insert is greater than any key in the leafnode
  if (idx != GetSize()) {
    memmove(static_cast<void *>(array + idx + 1), static_cast<const void *>(array + idx),
            sizeof(MappingType) * (GetSize() - idx));
  }
  array[idx].first = key;
  array[idx].second = value;
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() == GetMaxSize());
  int off = GetSize() / 2;
  recipient->CopyNFrom(array + off, GetSize() - off);

  SetSize(off);
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  int off = GetSize();
  memmove(static_cast<void *>(array + off), static_cast<const void *>(items), size * sizeof(MappingType));
  IncreaseSize(size);
  assert(GetSize() < GetMaxSize());
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  int idx = KeyIndex(key, comparator);
  if (idx == GetSize() || comparator(array[idx].first, key) != 0) {
    return false;
  }
  *value = array[idx].second;
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int idx = KeyIndex(key, comparator);
  // not exist
  if (idx == GetSize() || comparator(array[idx].first, key) != 0) {
    // LOG_DEBUG("not found  %ld pageid:%d", key.ToString(), GetPageId());
    return GetSize();
  }
  /* for (int i = idx + 1; i < GetSize(); i++) {
    array[i - 1] = array[i];
  } */
  int movesize = GetSize() - idx - 1;
  memmove(static_cast<void *>(array + idx), static_cast<const void *>(array + idx + 1), movesize * sizeof(MappingType));
  IncreaseSize(-1);
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
// middle_key and buffer_pool not use,just match the internal
// recipient is the pre node,delete cur node in its parent
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                           BufferPoolManager *buffer_pool_manager) {
  // LOG_DEBUG("move %d -> %d middlekey %ld", GetPageId(), recipient->GetPageId(), middle_key.ToString());
  int node_size = GetSize();
  assert(node_size + recipient->GetSize() < GetMaxSize());
  recipient->CopyNFrom(array, node_size);
  SetSize(0);
  recipient->SetNextPageId(GetNextPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                                  BufferPoolManager *buffer_pool_manager) {
  // in leaf page middle_key == array[0].first
  // assert(middle_key == array[0].first);
  recipient->CopyLastFrom(array[0]);
  IncreaseSize(-1);
  size_t len = sizeof(MappingType) * GetSize();
  memmove(static_cast<void *>(array), static_cast<const void *>(array + 1), len);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  int idx = GetSize();
  assert(idx + 1 < GetMaxSize());
  array[idx] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                                   BufferPoolManager *buffer_pool_manager) {
  MappingType lastMap = array[GetSize() - 1];
  recipient->CopyFirstFrom(lastMap);
  IncreaseSize(-1);
  assert(GetSize() >= GetMinSize());
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  assert(GetSize() + 1 < GetMaxSize());
  size_t len = sizeof(MappingType) * GetSize();
  memmove(static_cast<void *>(array + 1), static_cast<const void *>(array), len);
  array[0] = item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
