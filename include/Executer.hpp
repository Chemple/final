#pragma once

#include "Buffer.hpp"
#include "ChunkManager.hpp"
#include "InputReader.hpp"
#include "ThreadPool.hpp"
#include "config.hpp"
#include "spdlog/spdlog.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <queue>
#include <sys/types.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace final {

enum class TaskState { Pending, Runing, Done, Moved };

struct Task {
  TaskState task_state_;

  // consider the performance cost. when allocate tasks and deconstruct.
  // std::shared_ptr<BufferPool> buffer_pool_;
  BufferPool *no_owner_buffer_pool_;
  ChunkManager *no_owner_chunk_manager_;

  Task() = delete;

  Task(BufferPool *buffer_pool, ChunkManager *chunk_manager)
      : task_state_(TaskState::Pending), no_owner_buffer_pool_(buffer_pool),
        no_owner_chunk_manager_(chunk_manager) {}

  virtual ~Task() {
    // if (task_state_ != TaskState::Done) {
    //   spdlog::critical("the task is deconstruct before it is done");
    // }
  }

  // Task(const Task &other) = delete;
  // Task(Task &&other) {
  //   this->task_state_ = other.task_state_;
  //   no_owner_buffer_pool_ = other.no_owner_buffer_pool_;

  //   other.task_state_ = TaskState::Moved;
  //   other.no_owner_buffer_pool_ = nullptr;
  // }
  // Task &operator=(Task &&) = delete;
};

struct ChunkSortTask : public Task {
  uint32_t chunk_id_;
  uint32_t frame_id_;
  ChunkSortTask(uint32_t chunk_id, uint32_t frame_id, BufferPool *buffer_pool,
                ChunkManager *chunk_manager)
      : Task(buffer_pool, chunk_manager), chunk_id_(chunk_id),
        frame_id_(frame_id) {}
};

struct MergeSortTask : public Task {
  uint32_t begin_chunk_id_;
  uint32_t chunk_num_;
  uint32_t frame_num_;
  uint32_t begin_frame_id_;
  MergeSortTask(uint32_t begin_chunk_id, uint32_t chunk_num, uint32_t frame_num,
                uint32_t begin_frame_id, BufferPool *buffer_pool,
                ChunkManager *chunk_manager)
      : Task(buffer_pool, chunk_manager), begin_chunk_id_(begin_chunk_id),
        chunk_num_(chunk_num), frame_num_(frame_num),
        begin_frame_id_(begin_frame_id) {}
};

struct HeapNode {
  uint64_t value_;
  uint32_t chunk_id_;
  uint32_t in_chunk_loc_;

  HeapNode(uint64_t value, uint32_t chunk_id, uint32_t in_chunk_loc)
      : value_(value), chunk_id_(chunk_id), in_chunk_loc_(in_chunk_loc) {
    assert(in_chunk_loc < PAGE_VALUE_NUM);
  }

  auto operator>(const HeapNode &other) const {
    return this->value_ > other.value_;
  }
};

struct BlockInfo {
  uint32_t start_chunk_id_;
  uint32_t current_chunk_id_;
  uint32_t chunk_num_;
};

inline auto ChunkSort(int32_t fd, uint64_t offset, uint64_t realsize,
                      char *buffer, int32_t output_fd, uint64_t output_offset,
                      uint64_t output_realsize) {
  auto res = pread(fd, buffer, realsize, offset);
  if (res != realsize) [[unlikely]] {
    exit(-1);
  }
  std::sort((uint64_t *)buffer, (uint64_t *)(buffer + realsize));
  res = pwrite(output_fd, buffer, output_realsize, output_offset);
  if (res != realsize) [[unlikely]] {
    exit(-1);
  }
}

inline auto MergeSort(uint32_t start_chunk_id, uint32_t end_chunk_id,
                      file_descrip_t output_fd) {}

class Executer {
private:
  std::unique_ptr<BufferPool> buffer_pool_;
  std::unique_ptr<ChunkManager> chunk_manager_;
  std::unique_ptr<ThreadPool> thread_pool_;

  uint32_t worker_num_;
  uint32_t merge_factor_{4};

public:
  Executer(const std::string &folder_name,
           const std::string &output_file_name) {
    buffer_pool_ = std::make_unique<BufferPool>();
    thread_pool_ = std::make_unique<ThreadPool>();
    auto input_reader = std::make_unique<InputReader>(folder_name);
    auto file_opener = std::make_unique<FileOpener>(std::move(input_reader));
    chunk_manager_ = std::make_unique<ChunkManager>(std::move(file_opener),
                                                    output_file_name);

    worker_num_ = thread_pool_->GetWorkerNum();
  }

  auto ChunkSortWork(ChunkSortTask task) {
    auto chunk_id = task.chunk_id_;
    const auto &frame_id = task.frame_id_;
    const auto &chunk_info = chunk_manager_->GetChunkInfo()[chunk_id];
    auto fid = chunk_info.file_id_;
    auto offset = chunk_info.file_offset_;
    auto real_size = chunk_info.real_data_size_;
    // Bugs. because the fd maybe be closed during execution...
    auto fd = chunk_manager_->GetFileDescript(fid);
    auto page_guard = buffer_pool_->GetFrame(frame_id);
    ChunkSort(fd, offset, real_size, page_guard.page_->data,
              chunk_manager_->GetOutputFileFd(), chunk_id * PAGE_SIZE,
              PAGE_SIZE);
    return chunk_id;
  }

  auto LaunchChunkSort() {
    auto results = std::vector<std::future<uint32_t>>();
    auto chunk_sort_task_num = chunk_manager_->GetChunkInfo().size();
    std::function<uint32_t(ChunkSortTask)> task_func =
        [this](ChunkSortTask task) -> uint32_t {
      return this->ChunkSortWork(task);
    };
    for (auto i = 0; i < chunk_sort_task_num; i++) {
      auto task = ChunkSortTask(i, i % FRAME_NUM, buffer_pool_.get(),
                                chunk_manager_.get());
      results.push_back(thread_pool_->enqueue(task_func, task));
    }
    for (auto &&result : results) {
      result.get();
      // spdlog::info("the chunk {} finished sort", result.get());
    }
  }

  // doing
  auto MergerSortTask(MergeSortTask task) {
    auto chunk_num = task.chunk_num_;
    auto frame_num = task.frame_num_;
    auto merge_factor_k = frame_num;
    while (true) {
    }
  }

  // doing
  auto MergeSortKBlock(const std::vector<BlockInfo> &blockinfos,
                       const uint32_t &begin_input_frame_id,
                       const uint32_t &input_frame_num,
                       const uint32_t &output_frame_id,
                       const file_descrip_t &source_file_fd,
                       const file_descrip_t &output_file_id) {
    std::priority_queue<HeapNode, std::vector<HeapNode>, std::greater<>>
        min_heap;
    // first get all frames(input and output)
    auto page_guards = std::vector<PageGuard>{};
    for (auto i = begin_input_frame_id; i < (input_frame_num + 1); i++) {
      page_guards.emplace_back(buffer_pool_->GetFrame(i));
    }
    for (auto i = 0; i < input_frame_num; i++) {
      assert(blockinfos[i].current_chunk_id_ == blockinfos[i].start_chunk_id_);
      pread(source_file_fd, page_guards[i].page_->data, PAGE_SIZE,
            blockinfos[i].current_chunk_id_ * PAGE_SIZE);
      auto data = page_guards[i].page_->data;
      auto fist_value = ((uint64_t *)(data))[0];
      min_heap.push({fist_value, blockinfos[i].current_chunk_id_, 0});
    }
    auto output_frame_loc = 0;
    while (!min_heap.empty()) {
      auto top_node = min_heap.top();
      min_heap.pop();
    }
  }
};

} // namespace final
