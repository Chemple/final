#pragma once

#include "InputReader.hpp"
#include "LRUReplacer.hpp"
#include "config.hpp"
#include "spdlog/spdlog.h"

#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace final {

// for fucking 10w files open and close 😂😂😂
// fuck the requirements 😂😂😂
// fuck only 64M buffer size 😂😂😂
class FileOpener {
private:
  using Guard = std::lock_guard<std::mutex>;
  // opened file table (no thread-safe)
  std::shared_ptr<OpenedFileCache_t> lru_cache_;
  std::shared_ptr<InputReader> input_reader_;
  const uint32_t open_file_nums_ = OPEN_FILE_NUMS;
  // use mutex to ensure thread-safe
  std::mutex lock_;

public:
  FileOpener(std::shared_ptr<InputReader> &&input_reader)
      : input_reader_(std::move(input_reader)) {
    lru_cache_ = std::make_shared<OpenedFileCache_t>(open_file_nums_);
  }
  FileOpener(const FileOpener &) = delete;
  FileOpener &operator=(const FileOpener &) = delete;
  FileOpener(FileOpener &&other) {
    lru_cache_ = std::move(lru_cache_);
    input_reader_ = std::move(other.input_reader_);
  }
  FileOpener &operator=(FileOpener &&other) {
    lru_cache_ = std::move(lru_cache_);
    input_reader_ = std::move(other.input_reader_);
    return *this;
  }

  auto GetInputReader() { return input_reader_; }

  auto GetFd(project_file_id_t fid) {
    Guard g(lock_);
    auto fd = -1;
    if (lru_cache_->tryGet(fid, fd)) {
      return fd;
    }
    const auto &file_names = input_reader_->GetFileNames();
    const auto &file_name = file_names[fid].c_str();
    fd = open(file_name, O_RDWR);
    auto kv = lru_cache_->insert(fid, fd);
    if (kv != std::nullopt) {
      auto close_fd = kv->value;
      close(close_fd);
    }
    return fd;
  }
};

struct ChunkInfo {
  uint32_t file_id_{0};
  uint32_t real_data_size_{0};
  uint64_t file_offset_{0};
};

class ChunkManager {
private:
  std::vector<ChunkInfo> chunk_infos_;
  uint64_t chunk_number_;
  std::unique_ptr<FileOpener> file_opener_;
  // std::shared_ptr<InputReader> input_file_reader_;
  std::string output_file_name_0_;
  std::string output_file_name_1_;

  file_descrip_t output_file_fd_0_;
  file_descrip_t output_file_fd_1_;

  auto OpenOrCreateOutputFile(const std::string &output_file_name,
                              uint64_t file_size) {
    auto fd =
        open(output_file_name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd == -1) {
      spdlog::critical("the output file can not be opened");
      exit(-1);
    }
    struct stat fileStat;
    if (fstat(fd, &fileStat) == -1) {
      close(fd);
      spdlog::critical("the file stat can not be opened");
      exit(-1);
    }

    size_t actualSize = fileStat.st_size;

    if (actualSize != file_size) {
      spdlog::info("the file size is not expected, use ftruncate");
      auto res = ftruncate(fd, file_size);
      if (res != 0) {
        spdlog::critical("ftruncate failed");
      }
    }
    return fd;
  }

public:
  ChunkManager(std::unique_ptr<FileOpener> &&file_opener,
               const std::string &output_file_name_0,
               const std::string &output_file_name_1) {
    file_opener_ = std::move(file_opener);
    const auto &files_names = file_opener_->GetInputReader()->GetFileNames();
    auto chunk_num = (uint64_t)0;
    auto file_id = 0;
    for (const auto &file_name : files_names) {
      std::string full_file_path = file_name;

      std::ifstream file(full_file_path, std::ios::binary | std::ios::ate);
      if (file.is_open()) {
        uint64_t file_bytes = file.tellg();
        file.close();

        uint64_t remaining_bytes = file_bytes;
        uint64_t offset = 0;

        while (remaining_bytes > 0) {
          ChunkInfo chunk_info;

          uint64_t current_chunk_size =
              std::min(static_cast<uint64_t>(PAGE_SIZE), remaining_bytes);

          chunk_info.file_id_ = file_id;
          chunk_info.real_data_size_ = current_chunk_size;
          chunk_info.file_offset_ = offset;

          chunk_infos_.push_back(chunk_info);

          remaining_bytes -= current_chunk_size;
          offset += current_chunk_size;

          ++chunk_num;
        }
      } else {
        spdlog::critical("can not open file: {}", full_file_path);
      }
      file_id++;
    }
    chunk_number_ = chunk_num;

    spdlog::info("the chunk number is {}", chunk_num);
    spdlog::info("the output file size is {} GB", chunk_num * 4 / 1024);

    output_file_name_0_ = output_file_name_0;
    output_file_name_1_ = output_file_name_1;
    output_file_fd_0_ =
        OpenOrCreateOutputFile(output_file_name_0, chunk_number_ * PAGE_SIZE);
    output_file_fd_1_ =
        OpenOrCreateOutputFile(output_file_name_1, chunk_number_ * PAGE_SIZE);
  }

  ChunkManager(ChunkManager &&) = delete;
  ChunkManager &operator=(ChunkManager &&) = delete;
  ~ChunkManager() = default;

  const auto &GetChunkInfo() { return chunk_infos_; }
  const auto &GetChunkNum() { return chunk_number_; }
  auto GetFileDescript(project_file_id_t fid) {
    return file_opener_->GetFd(fid);
  }
  const auto &GetOutputFileFd0() { return output_file_fd_0_; }
  const auto &GetOutputFileName0() { return output_file_name_0_; }
  const auto &GetOutputFileName1() { return output_file_name_1_; }
  const auto &GetOutputFileFd1() { return output_file_fd_1_; }
};

} // namespace final
