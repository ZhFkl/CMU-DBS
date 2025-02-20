//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// skiplist.cpp
//
// Identification: src/primer/skiplist.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/skiplist.h"
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "common/macros.h"
#include "fmt/core.h"
namespace bustub {

/** @brief Checks whether the container is empty. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Empty() -> bool {
  return size_ == 0;
  // UNIMPLEMENTED("TODO(P0): Add implementation.");
}

/** @brief Returns the number of elements in the skip list. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Size() -> size_t {
  return size_;
  // UNIMPLEMENTED("TODO(P0): Add implementation.");
}

/**
 * @brief Iteratively deallocate all the nodes.
 *
 * We do this to avoid stack overflow when the skip list is large.
 *
 * If we let the compiler handle the deallocation, it will recursively call the destructor of each node,
 * which could block up the the stack.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Drop() {
  for (size_t i = 0; i < MaxHeight; i++) {
    auto curr = std::move(header_->links_[i]);
    while (curr != nullptr) {
      // std::move sets `curr` to the old value of `curr->links_[i]`,
      // and then resets `curr->links_[i]` to `nullptr`.
      curr = std::move(curr->links_[i]);
    }
  }
}

/**
 * @brief Removes all elements from the skip list.
 *
 * Note: You might want to use the provided `Drop` helper function.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Clear() {
  Drop();
  size_ = 0;
  // UNIMPLEMENTED("TODO(P0): Add implementation.");
}

/**
 * @brief Inserts a key into the skip list.
 *
 * Note: `Insert` will not insert the key if it already exists in the skip list.
 *
 * @param key key to insert.
 * @return true if the insertion is successful, false if the key already exists.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Insert(const K &key) -> bool {
  if (!header_) {
    throw std::runtime_error("Header is null");
  }
  std::unique_lock<std::shared_mutex> lock(rwlock_);
  std::vector<std::shared_ptr<SkipNode>> update(MaxHeight, nullptr);
  auto current = header_;
  for (size_t level = height_; level > 0; level--) {
    while (current->Next(level - 1) && compare_(current->Next(level - 1)->Key(), key)) {
      current = current->Next(level - 1);
    }
    update[level - 1] = current;
  }
  // check the node wheather exist
  current = current->Next(0);
  if (current && current->Key() == key) {
    return false;
  }

  // the key doesn't exist create a new one
  size_t new_height = RandomHeight();
  if (new_height > height_) {
    for (size_t i = height_; i < new_height; i++) {
      update[i] = header_;
    }
    height_ = new_height;
  }
  auto new_Node = std::make_shared<SkipNode>(new_height, key);

  // update the skiplist
  for (size_t level = 0; level < new_height; level++) {
    new_Node->SetNext(level, update[level]->Next(level));
    update[level]->SetNext(level, new_Node);
  }

  ++size_;
  return true;
  // UNIMPLEMENTED("TODO(P0): Add implementation.");
}

/**
 * @brief Erases the key from the skip list.
 *
 * @param key key to erase.
 * @return bool true if the element got erased, false otherwise.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Erase(const K &key) -> bool {
  std::unique_lock<std::shared_mutex> lock(rwlock_);  // 获取独占锁
  std::vector<std::shared_ptr<SkipNode>> update(MaxHeight, nullptr);
  auto current = header_;

  // search the skipnode and find the node we want to delete then update
  for (size_t level = height_; level > 0; level--) {
    while (current->Next(level - 1) && compare_(current->Next(level - 1)->Key(), key)) {
      current = current->Next(level - 1);
    }
    update[level - 1] = current;
  }

  // check the node
  current = current->Next(0);
  if (current == nullptr || current->key_ != key) {
    // don't find the node
    return false;
  }
  // delete the node
  for (size_t level = 0; level < current->Height(); level++) {
    update[level]->SetNext(level, current->Next(level));
  }
  // update the height
  while (height_ > 1 && header_->Next(height_ - 1) == nullptr) {
    --height_;
  }
  --size_;
  return true;
  // UNIMPLEMENTED("TODO(P0): Add implementation.");
}

/**
 * @brief Checks whether a key exists in the skip list.
 *
 * @param key key to look up.
 * @return bool true if the element exists, false otherwise.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Contains(const K &key) -> bool {
  // Following the standard library: Key `a` and `b` are considered equivalent if neither compares less
  // than the other: `!compare_(a, b) && !compare_(b, a)`.
  std::shared_lock<std::shared_mutex> lock(rwlock_);
  auto current = header_;
  for (size_t level = height_; level > 0; --level) {
    // 在当前层找到小于或等于目标键的最后一个节点
    while (current->Next(level - 1) && compare_(current->Next(level - 1)->Key(), key)) {
      current = current->Next(level - 1);
    }
  }

  current = current->Next(0);
  if (current == nullptr) {
    return false;
  }
  return !compare_(current->Key(), key) && !compare_(key, current->Key());
  // UNIMPLEMENTED("TODO(P0): Add implementation.");
}

/**
 * @brief Prints the skip list for debugging purposes.
 *
 * Note: You may modify the functions in any way and the output is not tested.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Print() {
  auto node = header_->Next(LOWEST_LEVEL);
  while (node != nullptr) {
    fmt::println("Node {{ key: {}, height: {} }}", node->Key(), node->Height());
    node = node->Next(LOWEST_LEVEL);
  }
}

/**
 * @brief Generate a random height. The height should be cappped at `MaxHeight`.
 * Note: we implement/simulate the geometric process to ensure platform independence.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::RandomHeight() -> size_t {
  // Branching factor (1 in 4 chance), see Pugh's paper.
  static constexpr unsigned int BRANCHING_FACTOR = 4;
  // Start with the minimum height
  size_t height = 1;
  while (height < MaxHeight && (rng_() % BRANCHING_FACTOR == 0)) {
    height++;
  }
  return height;
}

/**
 * @brief Gets the current node height.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Height() const -> size_t {
  return links_.size();
  // UNIMPLEMENTED("TODO(P0): Add implementation.");
}

/**
 * @brief Gets the next node by following the link at `level`.
 *
 * @param level index to the link.
 * @return std::shared_ptr<SkipNode> the next node, or `nullptr` if such node does not exist.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Next(size_t level) const
    -> std::shared_ptr<SkipNode> {
  if (level >= links_.size()) {
    return nullptr;
  }
  return links_[level];
  // UNIMPLEMENTED("TODO(P0): Add implementation.");
}

/**
 * @brief Set the `node` to be linked at `level`.
 *
 * @param level index to the link.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::SkipNode::SetNext(
    size_t level, const std::shared_ptr<SkipNode> &node) {
  if (level >= links_.size()) {
    throw std::out_of_range("Level is out of range for this node.");
  }
  links_[level] = node;
  // UNIMPLEMENTED("TODO(P0): Add implementation.");
}

/** @brief Returns a reference to the key stored in the node. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Key() const -> const K & {
  return key_;
  // UNIMPLEMENTED("TODO(P0): Add implementation.");
}

// Below are explicit instantiation of template classes.
template class SkipList<int>;
template class SkipList<std::string>;
template class SkipList<int, std::greater<>>;
template class SkipList<int, std::less<>, 8>;

}  // namespace bustub
