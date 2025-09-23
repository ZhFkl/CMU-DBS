//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : current_timestamp_(0),curr_size_(0),replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
    std::lock_guard<std::mutex> guard(latch_);
    if (curr_size_<= 0) {
      return std::nullopt;
    }
  
    frame_id_t victim = -1;
    size_t max_distance = 0;
    size_t oldest_timestamp = std::numeric_limits<size_t>::max();
  
    for (const auto &pair : node_store_) {
      auto frame_id = pair.first;
      auto &node = pair.second;
  
      if (!node.is_evictable_) {
        continue;
      }
  
      size_t distance;
      if (node.history_.size() >= k_) {
        distance = current_timestamp_ - node.history_.front();
      } else {
        distance = std::numeric_limits<size_t>::max();
      }
  
      if (distance > max_distance) {
        max_distance = distance;
        victim = frame_id;
      } else if (distance == std::numeric_limits<size_t>::max() && node.history_.front() < oldest_timestamp) {
        oldest_timestamp = node.history_.front();
        victim = frame_id;
      }
    }
  
    if (victim == -1) {
      return std::nullopt;
    }
  
    node_store_.erase(victim);
    curr_size_--;
    return victim;
  }

/**-1
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(latch_);
    if(static_cast<size_t>(frame_id) >= replacer_size_){
        throw Exception(" Invalid frame ID");
    }
    // then find if the node is already exist in the list

    if(node_store_.find(frame_id) == node_store_.end()){
        //if not create a new node

        // is this a way to creat the lruknode ? 
        node_store_[frame_id] =  LRUKNode{std::list<size_t>(),k_,frame_id,false};
    }
    
    // if already have just update the timestamp
    auto &node = node_store_[frame_id];
    node.history_.push_back(current_timestamp_);
    if(node.history_.size() > k_)
    {
        node.history_.pop_front();
    }
    ++current_timestamp_;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    // get the lock to avoid competition
    std::lock_guard<std::mutex> guard(latch_);
    // judge if the frame_id is out of the range
    if(static_cast<size_t>(frame_id) >= replacer_size_){
        throw Exception("Invalid fram ID");
    }
    //if not our of range then set
    if(node_store_.find(frame_id)  == node_store_.end())
    {
        return ;
    }
    auto &node = node_store_[frame_id];
    bool old_evictable = node.is_evictable_;
    //curr_size is the node size left = replacer_size - used_size
    node.is_evictable_ = set_evictable;
    if(set_evictable){
        if(!old_evictable){

            curr_size_++;
        }
    }else{
        if(old_evictable){
            curr_size_--;
        }
    }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(latch_);
    if(static_cast<size_t>(frame_id) >= replacer_size_){
        throw Exception("Invaild frame ID");
    }
    if(node_store_.find(frame_id) == node_store_.end()){
        return;
    }
    auto &node = node_store_[frame_id];
    if(!node.is_evictable_){
        throw Exception("This node is nonevcitable");
    }else{
        node_store_.erase(node.fid_);
        curr_size_--;
    }
    
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { 
    std::lock_guard<std::mutex> guard(latch_);
    return curr_size_;
}
}  // namespace bustub
