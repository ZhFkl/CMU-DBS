//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// watermark.cpp
//
// Identification: src/concurrency/watermark.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  if(current_reads_.find(read_ts) != current_reads_.end()){
    current_reads_[read_ts]++;
    return;
  }
  current_reads_[read_ts] = 1;
  return;
  // TODO(fall2023): implement me!
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  if(current_reads_.find(read_ts) != current_reads_.end()){
    // the read_ts is in the table
    if(current_reads_[read_ts] == 1){
      current_reads_.erase(read_ts);
    }else{
      current_reads_[read_ts]--;
    }
  }
  if(current_reads_.empty()){
    watermark_ = commit_ts_;
  }else{
    auto it  = current_reads_.begin();
    watermark_ = it->first;
  }
  return;

  // TODO(fall2023): implement me!
}

}  // namespace bustub
