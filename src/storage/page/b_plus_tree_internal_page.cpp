//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  assert(static_cast<uint32_t>(max_size) <= INTERNAL_PAGE_SIZE);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); i++) {
    if (array[i].second == value) {
      return i;
    }
  }
  return INVALID_PAGE_ID;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  assert(index >= 0 && index < GetSize());
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  int left = 1;
  int right = GetSize() - 1;
  // find right border
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(key, array[mid].first) < 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return array[right].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  // array[0].first = INVALID_PAGE_ID;
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  assert(GetSize() + 1 <= GetMaxSize());
  int new_idx = ValueIndex(old_value) + 1;
  assert(new_idx > 0 && new_idx <= GetSize());  // if new_idx == maxsize() - 1,cause split
  // memmove
  size_t movesize = (GetSize() - new_idx);  // data to move
  memmove(static_cast<void *>(array + new_idx + 1), static_cast<const void *>(array + new_idx),
          sizeof(MappingType) * movesize);
  array[new_idx].first = new_key;
  array[new_idx].second = new_value;
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() == GetMaxSize());
  int off = GetSize() / 2;
  recipient->CopyNFrom(array + off, GetSize() - off, buffer_pool_manager);
  SetSize(off);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  int off = GetSize();
  memcpy(static_cast<void *>(array + off), static_cast<const void *>(items), size * sizeof(MappingType));
  for (int i = 0; i < size; i++) {
    page_id_t child_page_id = array[off + i].second;
    Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
    if (child_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "no space in bufferPool.");
    }
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
  IncreaseSize(size);
  assert(GetSize() < GetMaxSize());
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(index >= 0 && index < GetSize());
  int movesize = GetSize() - index - 1;
  memmove(static_cast<void *>(array + index), static_cast<const void *>(array + index + 1),
          movesize * sizeof(MappingType));
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() { return INVALID_PAGE_ID; }
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(array, GetSize(), buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  MappingType item{middle_key, array[0].second};
  recipient->CopyLastFrom(item, buffer_pool_manager);
  IncreaseSize(-1);
  memmove(static_cast<void *>(array), static_cast<const void *>(array + 1), sizeof(MappingType) * GetSize());

  // change the child's parent
  page_id_t child_page_id = item.second;
  Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
  if (child_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "no space in bufferPool.");
  }
  BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child_page_id, true);

  /* //update the parent
  page_id_t parent_id = GetParentPageId();
  Page* parent_page = buffer_pool_manager->FetchPage(parent_id);
  B_PLUS_TREE_INTERNAL_PAGE_TYPE* parent_node =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(parent_page->GetData());
  //what is middle for
  //assert(middle_key == array[0].first);
  parent_node->SetKeyAt(parent_node->ValueIndex(GetPageId()),array[0].first);
  buffer_pool_manager->UnpinPage(parent_id,true);   */
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int idx = GetSize();
  assert(idx < GetMaxSize() - 1);  //
  array[idx] = pair;
  IncreaseSize(1);
  // update the chldnode's parentid
  page_id_t child_page_id = pair.second;
  Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
  if (child_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "no space in bufferPool.");
  }
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *child_node =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page_id, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() < GetMaxSize());
  recipient->SetKeyAt(0, middle_key);
  MappingType item = array[GetSize() - 1];
  recipient->CopyFirstFrom(item, buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() < GetMaxSize() - 1);
  size_t len = sizeof(MappingType) * GetSize();
  memmove(static_cast<void *>(array + 1), static_cast<const void *>(array), len);
  array[0] = pair;
  IncreaseSize(1);
  // update child
  page_id_t child_idx = pair.second;
  Page *child_page = buffer_pool_manager->FetchPage(child_idx);
  if (child_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "no space in bufferPool.");
  }
  BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_idx, true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
