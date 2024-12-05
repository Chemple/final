#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// 每个chunk的大小为4MB，每个uint64_t占8字节，所以每个chunk包含524288个uint64_t
constexpr size_t CHUNK_SIZE = 4 * 1024 * 1024;    // 4MB
constexpr size_t ELEMENT_SIZE = sizeof(uint64_t); // 每个元素8字节
constexpr size_t ELEMENTS_PER_CHUNK =
    CHUNK_SIZE / ELEMENT_SIZE; // 每个chunk包含的元素数量

// 判断单个chunk是否升序
bool isChunkSorted(const uint64_t *chunk_data, size_t size) {
  for (size_t i = 1; i < size; ++i) {
    if (chunk_data[i] < chunk_data[i - 1]) {
      return false; // 如果有一个元素比前一个小，返回false
    }
  }
  return true;
}

// 读取文件并检查所有chunk的有序性
void checkFileChunksSorted(const fs::path &file_path) {
  std::ifstream file(file_path, std::ios::binary);

  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << file_path << std::endl;
    return;
  }

  uint64_t *buffer = new uint64_t[ELEMENTS_PER_CHUNK];
  size_t chunk_index = 0;

  // 逐个读取chunk并检查有序性
  while (file.read(reinterpret_cast<char *>(buffer), CHUNK_SIZE)) {
    if (!isChunkSorted(buffer, ELEMENTS_PER_CHUNK)) {
      std::cerr << "File " << file_path << " chunk " << chunk_index
                << " is not sorted!" << std::endl;
    }
    chunk_index++;
  }

  // 如果文件最后剩余部分不满一个完整的chunk，也需要检查
  size_t bytes_read = file.gcount();
  if (bytes_read > 0) {
    size_t elements_read = bytes_read / ELEMENT_SIZE;
    if (!isChunkSorted(buffer, elements_read)) {
      std::cerr << "File " << file_path << " last partial chunk is not sorted!"
                << std::endl;
    }
  }

  delete[] buffer;
  file.close();
}

// 遍历文件夹，检查所有文件的chunk
void checkFolderChunksSorted(const fs::path &folder_path) {
  for (const auto &entry : fs::directory_iterator(folder_path)) {
    if (entry.is_regular_file()) {
      checkFileChunksSorted(entry.path());
    }
  }
}

int main() {
  std::string folder_path =
      "/home/shiwen/project/final/output"; // 设置目标文件夹路径
  checkFolderChunksSorted(folder_path);
  return 0;
}
