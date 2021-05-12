//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page *leaf_page = FindLeafPage(key, false, OperationType::READ, transaction);
  assert(leaf_page != nullptr);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  result->resize(1);
  ValueType temp;
  bool symbol = leaf_node->Lookup(key, &temp, comparator_);
  (*result)[0] = temp;
  if (transaction == nullptr) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  } else {
    UnpinAncestor_transaction(true, transaction);
  }

  return symbol;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    if (transaction == nullptr) {
      StartNewTree(key, value);
      return true;
    }
    assert(!transaction->isRootLocked());
    root_mutex.lock();
    if (IsEmpty()) {
      StartNewTree(key, value);
      root_mutex.unlock();
      return true;
    }
    root_mutex.unlock();
    // return InsertIntoLeaf(key, value, transaction);
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t newid = -1;
  Page *newpage = buffer_pool_manager_->NewPage(&newid);
  if (newpage == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory.");
  }
  LeafPage *leafnode = reinterpret_cast<LeafPage *>(newpage->GetData());
  leafnode->Init(newid, INVALID_PAGE_ID, leaf_max_size_);
  root_page_id_ = newid;
  UpdateRootPageId(1);
  leafnode->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(newid, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction,
                                    OperationType ot) {
  bool ret = false;
  Page *leaf_page = FindLeafPage(key, false, ot, transaction);
  if (leaf_page == nullptr) {
    return Insert(key, value, transaction);
  }
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  switch (ot) {
    case OperationType::OPTIMISTIC_READ: {
      ValueType tmp;
      if (!leaf_node->Lookup(key, &tmp, comparator_)) {
        if (leaf_node->GetSize() + 1 < leaf_node->GetMaxSize()) {
          leaf_node->Insert(key, value, comparator_);
          ret = true;
        } else {
          if (transaction == nullptr) {
            buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
          } else {
            transaction_aftermath(false, transaction);
          }
          return InsertIntoLeaf(key, value, transaction, OperationType::INSERT);
        }
      }
      break;
    }

    case OperationType::INSERT: {
      if (leaf_node->Insert(key, value, comparator_) >= leaf_node->GetMaxSize()) {
        // remember to unpin split_page,unpin in insertintoparent
        LeafPage *split_node = Split<LeafPage>(leaf_node);  // return newly allocated page
        InsertIntoParent(leaf_node, split_node->KeyAt(0), split_node, transaction);
      }
      ret = true;
      break;
    }

    default:
      assert(0);
  }
  if (transaction == nullptr) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  } else {
    transaction_aftermath(false, transaction);
  }
  return ret;
}  // namespace bustub

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t newid;
  Page *split_page = buffer_pool_manager_->NewPage(&newid);
  if (split_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory.");
  }
  N *split_node = reinterpret_cast<N *>(split_page->GetData());
  split_node->Init(newid, node->GetParentPageId(), node->GetMaxSize());
  node->MoveHalfTo(split_node, buffer_pool_manager_);
  // remember unpin split_page after call Split
  return split_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    // get newroot
    page_id_t newrootId = -1;
    Page *new_root_page = buffer_pool_manager_->NewPage(&newrootId);
    if (new_root_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory.");
    }
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_node->Init(newrootId, INVALID_PAGE_ID, internal_max_size_);
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // set root id
    root_page_id_ = newrootId;
    old_node->SetParentPageId(newrootId);
    new_node->SetParentPageId(newrootId);
    UpdateRootPageId();
    // unpin
    buffer_pool_manager_->UnpinPage(newrootId, true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    return;
  }
  // get parent node
  page_id_t parentId = old_node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parentId);
  if (parent_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "no space in bufferPool.");
  }
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // set parent id
  new_node->SetParentPageId(parentId);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  // insert
  int cursize = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  if (cursize >= parent_node->GetMaxSize()) {  //
    InternalPage *split_page = Split<InternalPage>(parent_node);
    InsertIntoParent(parent_node, split_page->KeyAt(0), split_page, transaction);
  }
  buffer_pool_manager_->UnpinPage(parentId, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction, OperationType ot) {
  // unpin leaf_page after findleafpage
  Page *leaf_page = FindLeafPage(key, false, ot, transaction);
  if (leaf_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Remove: not found in key.");
  }
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // delete and check size
  switch (ot) {
    case OperationType::OPTIMISTIC_READ: {
      ValueType tmp;
      if (leaf_node->Lookup(key, &tmp, comparator_)) {
        if (leaf_node->GetSize() - 1 >= leaf_node->GetMinSize()) {
          leaf_node->RemoveAndDeleteRecord(key, comparator_);
        } else {
          if (transaction != nullptr) {
            transaction_aftermath(false, transaction);
          }
          return Remove(key, transaction, OperationType::DELETE);
        }
      }
      break;
    }

    case OperationType::DELETE: {
      if (leaf_node->RemoveAndDeleteRecord(key, comparator_) < leaf_node->GetMinSize()) {
        CoalesceOrRedistribute<LeafPage>(leaf_node, transaction);
      }
      break;
    }

    default:
      assert(0);
  }
  if (transaction != nullptr) {
    transaction_aftermath(false, transaction);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    return AdjustRoot(node, transaction);
  }
  // find sibling page
  // get parent node
  page_id_t parent_page_id = node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);  // remember unpin
  if (parent_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "no space in bufferPool.");
  }
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // get index in parent
  int childIdx = parent_node->ValueIndex(node->GetPageId());
  assert(childIdx != INVALID_PAGE_ID);
  int siblingIdx = (childIdx == 0) ? childIdx + 1 : childIdx - 1;
  // get sibling node
  page_id_t sibling_page_id = parent_node->ValueAt(siblingIdx);
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);  // remember unpin
  // must get sibling page's wlock
  if (transaction != nullptr) {
    sibling_page->WLatch();
    transaction->AddIntoPageSet(sibling_page);
  }
  N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());
  // coalesce
  if (sibling_node->GetSize() + node->GetSize() < node->GetMaxSize()) {
    // let the first para in coalesce be prenode
    if (childIdx == 0) {
      // sibling delete
      Coalesce(&node, &sibling_node, &parent_node, siblingIdx, transaction);
    } else {
      // node delete
      Coalesce(&sibling_node, &node, &parent_node, childIdx, transaction);
    }
    buffer_pool_manager_->UnpinPage(parent_page_id, true);  // unpin
    return true;
  }
  // redistribute
  Redistribute(sibling_node, node, childIdx);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);  // unpin
  if (transaction == nullptr) {
    buffer_pool_manager_->UnpinPage(sibling_page_id, true);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  }
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  // node is after neighbor_node and is to be deleted
  (*node)->MoveAllTo(*neighbor_node, (*parent)->KeyAt(index), buffer_pool_manager_);
  page_id_t node_page_id = (*node)->GetPageId();
  if (transaction == nullptr) {
    buffer_pool_manager_->UnpinPage(node_page_id, false);
    buffer_pool_manager_->DeletePage(node_page_id);
    buffer_pool_manager_->UnpinPage((*neighbor_node)->GetPageId(), true);
  } else {
    transaction->AddIntoDeletedPageSet(node_page_id);
  }
  (*parent)->Remove(index);
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute(*parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // get parent node
  page_id_t parent_page_id = node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  if (parent_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "no space in bufferPool.");
  }
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // borrow kids,update parent
  if (index == 0) {
    // neighbor idx in parent is 1
    neighbor_node->MoveFirstToEndOf(node, parent_node->KeyAt(1), buffer_pool_manager_);
    // change the key in parent
    parent_node->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node, parent_node->KeyAt(index), buffer_pool_manager_);
    parent_node->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node, Transaction *transaction) {
  assert(old_root_node->IsRootPage());
  page_id_t old_root_id = old_root_node->GetPageId();
  if (old_root_node->IsLeafPage()) {
    assert(old_root_node->GetSize() == 0);
    if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(old_root_id, false);
      buffer_pool_manager_->DeletePage(old_root_id);
    } else {
      transaction->AddIntoDeletedPageSet(old_root_id);
    }
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  }
  assert(old_root_node->GetSize() == 1);
  InternalPage *root_internal = reinterpret_cast<InternalPage *>(old_root_node);
  page_id_t child_page_id = root_internal->ValueAt(0);
  Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
  if (child_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory.");
  }
  BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(INVALID_PAGE_ID);
  root_page_id_ = child_page_id;
  UpdateRootPageId();

  if (transaction == nullptr) {
    buffer_pool_manager_->UnpinPage(old_root_id, false);
    buffer_pool_manager_->DeletePage(old_root_id);
    buffer_pool_manager_->UnpinPage(child_page_id, true);
  } else {
    transaction->AddIntoDeletedPageSet(old_root_id);
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType tmp;
  Page *left_leaf_page = FindLeafPage(tmp, true, OperationType::READ, nullptr);
  if (left_leaf_page == nullptr) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_);
  }
  LeafPage *left_leaf_node = reinterpret_cast<LeafPage *>(left_leaf_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, 0, left_leaf_node);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *left_leaf_page = FindLeafPage(key, false, OperationType::READ, nullptr);
  if (left_leaf_page == nullptr) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_);
  }
  LeafPage *left_leaf_node = reinterpret_cast<LeafPage *>(left_leaf_page->GetData());
  //>= key
  int kv_idx = left_leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, kv_idx, left_leaf_node);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(buffer_pool_manager_); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */

//* transaction may be nullptr, in indexiterator function
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, OperationType ot, Transaction *transaction) {
  Page *curPage = nullptr;
  BPlusTreePage *tree_node = nullptr;
  if (transaction == nullptr) {
    curPage = FetchPage_transaction(root_page_id_, ot, transaction);
    assert(curPage != nullptr);
    tree_node = reinterpret_cast<BPlusTreePage *>(curPage->GetData());
  } else {
    assert(!transaction->isRootLocked());
    root_mutex.lock();
    if (IsEmpty()) {
      root_mutex.unlock();
      return nullptr;
    }
    transaction->setRootLock(true);
    curPage = FetchPage_transaction(root_page_id_, ot, transaction);
    assert(curPage != nullptr);
    tree_node = reinterpret_cast<BPlusTreePage *>(curPage->GetData());
    if (ot == OperationType::READ || (ot == OperationType::OPTIMISTIC_READ && !tree_node->IsLeafPage())) {
      root_mutex.unlock();
      transaction->setRootLock(false);
    }
  }
  if (tree_node->IsLeafPage()) {
    return curPage;
  }
  while (!tree_node->IsLeafPage()) {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(tree_node);
    page_id_t pit = -1;
    if (leftMost) {
      pit = internal_node->ValueAt(0);
    } else {
      pit = internal_node->Lookup(key, comparator_);
    }
    assert(pit != -1);
    if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(internal_node->GetPageId(), false);
      curPage = buffer_pool_manager_->FetchPage(pit);
    } else {
      curPage = FetchPage_transaction(pit, ot, transaction);
    }
    assert(curPage != nullptr);
    tree_node = reinterpret_cast<BPlusTreePage *>(curPage->GetData());
    if (tree_node->IsLeafPage()) {
      return curPage;
    }
  }
  assert(0);
  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}
// fetch page with transaction,do safe check
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FetchPage_transaction(page_id_t page_id, OperationType ot, Transaction *transaction) {
  // get this page latch
  Page *curPage = buffer_pool_manager_->FetchPage(page_id);
  assert(curPage != nullptr);
  if (transaction == nullptr) {
    return curPage;
  }
  BPlusTreePage *tree_node = reinterpret_cast<BPlusTreePage *>(curPage->GetData());
  bool isread = (ot == OperationType::READ || ot == OperationType::OPTIMISTIC_READ);
  if (tree_node->IsLeafPage() && ot == OperationType::OPTIMISTIC_READ) {
    curPage->WLatch();
  } else if (isread) {
    curPage->RLatch();
  } else {
    curPage->WLatch();
  }
  // do safe check
  if (page_id != root_page_id_ && tree_node->checkSafe(ot)) {
    if (transaction->isRootLocked()) {
      root_mutex.unlock();
      transaction->setRootLock(false);
    }
    UnpinAncestor_transaction(isread, transaction);
  }
  transaction->AddIntoPageSet(curPage);
  return curPage;
}

// unpin ancestors if this node is safe
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnpinAncestor_transaction(bool isread, Transaction *transaction) {
  if (transaction == nullptr || transaction->GetPageSet()->empty()) {
    return;
  }
  auto page_set = transaction->GetPageSet();
  auto delete_set = transaction->GetDeletedPageSet();
  // traverse and unlock
  for (auto curpage : *page_set) {
    if (isread) {
      curpage->RUnlatch();
    } else {
      curpage->WUnlatch();
    }
    page_id_t curpage_id = curpage->GetPageId();
    buffer_pool_manager_->UnpinPage(curpage_id, !isread);
    // delete if exist in deletepage
    if (delete_set->find(curpage_id) != delete_set->end()) {
      buffer_pool_manager_->DeletePage(curpage_id);
      delete_set->erase(curpage_id);
    }
  }
  assert(delete_set->empty());
  page_set->clear();
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::transaction_aftermath(bool isread, Transaction *transaction) {
  assert(transaction != nullptr);
  UnpinAncestor_transaction(isread, transaction);
  if (transaction->isRootLocked()) {
    root_mutex.unlock();
    transaction->setRootLock(false);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
