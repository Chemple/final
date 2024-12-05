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
#include <future>
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
  file_descrip_t fd0_;
  file_descrip_t fd1_;
  MergeSortTask(uint32_t begin_chunk_id, uint32_t chunk_num, uint32_t frame_num,
                uint32_t begin_frame_id, BufferPool *buffer_pool,
                ChunkManager *chunk_manager, file_descrip_t fd0,
                file_descrip_t fd1)
      : Task(buffer_pool, chunk_manager), begin_chunk_id_(begin_chunk_id),
        chunk_num_(chunk_num), frame_num_(frame_num),
        begin_frame_id_(begin_frame_id), fd0_(fd0), fd1_(fd1) {}
};

struct HeapNode {
  uint64_t value_;
  uint32_t block_info_id_; // relative id, start with 0.

  auto operator>(const HeapNode &other) const {
    return this->value_ > other.value_;
  }
};

struct BlockInfo {
  uint32_t start_chunk_id_;
  uint32_t current_chunk_id_;
  uint32_t current_loc_in_chunk_; // uint64_t value loc
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

inline auto ceil(uint32_t a, uint32_t b) { return (a + b - 1) / b; }

class Executer {
private:
  std::unique_ptr<BufferPool> buffer_pool_;
  std::unique_ptr<ChunkManager> chunk_manager_;
  std::unique_ptr<ThreadPool> thread_pool_;

  uint32_t worker_num_;

public:
  Executer(const std::string &folder_name, const std::string &output_file_name0,
           const std::string &output_file_name1) {
    buffer_pool_ = std::make_unique<BufferPool>();
    thread_pool_ = std::make_unique<ThreadPool>();
    auto input_reader = std::make_unique<InputReader>(folder_name);
    auto file_opener = std::make_unique<FileOpener>(std::move(input_reader));
    chunk_manager_ = std::make_unique<ChunkManager>(
        std::move(file_opener), output_file_name0, output_file_name1);

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
              chunk_manager_->GetOutputFileFd0(), chunk_id * PAGE_SIZE,
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

  // each block is one merge section. maybe use merge section is better.
  // assert(input_frame_num == block_num);
  // k-way mergesort, k = block_num.
  auto MergeSortKBlocks(std::vector<BlockInfo> &blockinfos,
                        const uint32_t &begin_input_frame_id,
                        const uint32_t &input_frame_num,
                        const file_descrip_t &source_file_fd,
                        const file_descrip_t &output_file_id) {
    std::priority_queue<HeapNode, std::vector<HeapNode>, std::greater<>>
        min_heap;
    // first get all frames(input and output)
    auto page_guards = std::vector<PageGuard>{};
    auto block_num = blockinfos.size();
    // check, the process of merge is sequential.
    // check, the input_file and output_file is same size and same format
    auto output_chunk_id = blockinfos[0].start_chunk_id_;
    auto end_output_chunk_id = blockinfos[block_num - 1].start_chunk_id_ +
                               blockinfos[block_num - 1].chunk_num_;
    spdlog::info("the block info num is {}", block_num);
    spdlog::info("start output chunk id is {}", output_chunk_id);
    spdlog::info("end_output_chunk_id is {}", end_output_chunk_id);
    for (auto i = begin_input_frame_id; i < (input_frame_num + 1); i++) {
      page_guards.emplace_back(buffer_pool_->GetFrame(i));
    }
    // assert(input_frame_num == block_num);
    auto real_input_frame_used = input_frame_num;
    if (block_num < input_frame_num) {
      real_input_frame_used = block_num;
    }
    for (uint32_t i = 0; i < real_input_frame_used; i++) {
      assert(blockinfos[i].current_chunk_id_ == blockinfos[i].start_chunk_id_);
      assert(blockinfos[i].current_loc_in_chunk_ == 0);
      pread(source_file_fd, page_guards[i].page_->data, PAGE_SIZE,
            blockinfos[i].current_chunk_id_ * PAGE_SIZE);
      auto data = page_guards[i].page_->data;
      auto fist_value = ((uint64_t *)(data))[0];
      min_heap.push({fist_value, i});
    }
    auto output_frame_loc = 0;
    auto out_put_buffer =
        (uint64_t *)page_guards[real_input_frame_used].page_->data;
    // check the condition.
    while (output_chunk_id < end_output_chunk_id) {
      assert(!min_heap.empty());
      auto top_node = min_heap.top();
      auto the_top_block_info_id = top_node.block_info_id_;
      auto &the_block_info = blockinfos[the_top_block_info_id];
      the_block_info.current_loc_in_chunk_++;
      min_heap.pop();
      // write into output buffer.
      out_put_buffer[output_frame_loc++] = top_node.value_;
      // if the buffer is full, write data into disk.
      if (output_frame_loc == PAGE_VALUE_NUM) {
        pwrite(output_file_id, out_put_buffer, PAGE_SIZE,
               output_chunk_id * PAGE_SIZE);
        output_frame_loc = 0;
        output_chunk_id++;
      }
      // add next value in the input frame to heap.
      if (the_block_info.current_loc_in_chunk_ == PAGE_VALUE_NUM) {
        the_block_info.current_chunk_id_++;
        the_block_info.current_loc_in_chunk_ = 0;
        // if the block has chunk to preocess.
        if (the_block_info.current_chunk_id_ <
            (the_block_info.chunk_num_ + the_block_info.start_chunk_id_)) {
          pread(source_file_fd,
                page_guards[the_top_block_info_id].GetPage()->data, PAGE_SIZE,
                the_block_info.current_chunk_id_ * PAGE_SIZE);
        } else {
          continue;
        }
      }
      auto value = ((uint64_t *)page_guards[the_top_block_info_id]
                        .GetPage()
                        ->data)[the_block_info.current_loc_in_chunk_];
      min_heap.push({value, the_top_block_info_id});
    }
    // spdlog::info("finish one ");
    if (!min_heap.empty()) {
      spdlog::critical("min_heap is not empty");
    }
    assert(min_heap.empty());
  }

  // each thread generate its final sorted file.
  auto MergerSortWork(MergeSortTask task) {
    auto chunk_num = task.chunk_num_;
    auto frame_num = task.frame_num_;
    auto merge_factor_k = frame_num - 1;
    if (chunk_num < merge_factor_k) {
      merge_factor_k = chunk_num;
    }
    if (chunk_num < frame_num - 1) {
      frame_num = chunk_num + 1;
    }
    auto begin_chunk_id = task.begin_chunk_id_;
    auto begin_frame_id = task.begin_frame_id_;

    auto merge_section_size = (uint32_t)1;
    auto block_infos = std::vector<BlockInfo>{};
    // auto itr_num = chunk_num / (merge_section_size * merge_factor_k);
    auto source_fd = task.fd0_;
    auto target_fd = task.fd1_;
    // check the condition.
    while (merge_section_size < chunk_num) {
      auto itr_num = ceil(chunk_num, (merge_section_size * merge_factor_k));
      for (auto i = 0; i < itr_num; i++) {
        block_infos.clear();
        // auto this_round_merge_factor_k = merge_factor_k;
        // if (i == (itr_num - 1)) [[unlikely]] {
        //   this_round_merge_factor_k =
        //       chunk_num - (merge_factor_k * merge_section_size * (itr_num -
        //       1));
        // }
        for (auto iid = 0; iid < merge_factor_k; iid++) {
          // 💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩
          auto start_chunk_id = begin_chunk_id +
                                merge_section_size * merge_factor_k * i +
                                merge_section_size * iid;
          // auto end_chunk_id = begin_chunk_id +
          //                     merge_section_size * merge_factor_k * i +
          //                     merge_section_size * (iid + 1) - 1;
          auto block_chunk_num = merge_section_size;
          if (chunk_num < merge_section_size * merge_factor_k * i +
                              merge_section_size * (iid + 1)) {
            block_chunk_num =
                chunk_num - (merge_section_size * merge_factor_k * i +
                             merge_section_size * iid);
            if (block_chunk_num != 0) {
              block_infos.push_back({BlockInfo{start_chunk_id, start_chunk_id,
                                               0, block_chunk_num}});
            }
            break;
          } else {
            block_infos.push_back({BlockInfo{start_chunk_id, start_chunk_id, 0,
                                             block_chunk_num}});
          }
          // 💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩
        }
        // for (const auto &elme : block_infos) {
        //   spdlog::info("the vec is {}, {}, {}, {}", elme.chunk_num_,
        //                elme.current_chunk_id_, elme.chunk_num_,
        //                elme.start_chunk_id_);
        // }
        MergeSortKBlocks(block_infos, begin_frame_id, frame_num - 1, source_fd,
                         target_fd);
      }
      merge_section_size *= merge_factor_k;
      std::swap(source_fd, target_fd);
    }
    spdlog::info("the sorted file is {}", source_fd);
    return task.begin_chunk_id_;
  }

  auto LaunchMergeSort() {
    auto results = std::vector<std::future<uint32_t>>();
    auto task_num = thread_pool_->GetWorkerNum();
    std::function<uint32_t(MergeSortTask)> task_func =
        [this](MergeSortTask task) -> uint32_t {
      return this->MergerSortWork(task);
    };

    auto task_chunk_num = chunk_manager_->GetChunkNum() / task_num;
    auto last_task_chunk_num = task_chunk_num + chunk_manager_->GetChunkNum() -
                               task_num * task_chunk_num;
    // waste some frame.
    auto task_frame_num = FRAME_NUM / task_num;
    assert(task_frame_num >= 3);

    for (auto i = 0; i < task_num; i++) {
      auto this_task_chunk_num = task_chunk_num;
      if (i == (task_num - 1)) {
        this_task_chunk_num = last_task_chunk_num;
      }
      auto task = MergeSortTask(task_chunk_num * i, this_task_chunk_num,
                                task_frame_num, task_frame_num * i,
                                buffer_pool_.get(), chunk_manager_.get(),
                                chunk_manager_->GetOutputFileFd0(),
                                chunk_manager_->GetOutputFileFd1());
      results.push_back(thread_pool_->enqueue(task_func, task));
    }

    for (auto &&result : results) {
      spdlog::info("the task started with {} finished sort", result.get());
    }
  }
};

} // namespace final
