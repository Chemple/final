#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

namespace fs = std::filesystem;

// 每个chunk的大小为4MB，每个uint64_t占8字节，所以每个chunk包含524288个uint64_t
constexpr size_t CHUNK_SIZE = 4 * 1024 * 1024;    // 4MB
constexpr size_t ELEMENT_SIZE = sizeof(uint64_t); // 每个元素8字节
constexpr size_t ELEMENTS_PER_CHUNK =
    CHUNK_SIZE / ELEMENT_SIZE;        // 每个chunk包含的元素数量
constexpr size_t CHUNKS_PER_FILE = 4; // 每个文件包含4个chunk
constexpr size_t TOTAL_FILES = 32;    // 总共生成32个文件

// 按照不同的方式打乱每个chunk的数据
void shuffleChunk(std::vector<uint64_t> &chunk_data, size_t chunk_index) {
  // 使用 chunk_index 作为随机数种子，确保每个 chunk 的打乱方式不同
  std::mt19937 rng(static_cast<uint32_t>(chunk_index));
  std::shuffle(chunk_data.begin(), chunk_data.end(), rng);
}

// 生成数据并写入文件
void generateData(const fs::path &folder_path) {
  // 总共需要生成的 chunk 数量
  const size_t total_chunks = TOTAL_FILES * CHUNKS_PER_FILE;

  // 按文件生成
  for (size_t file_idx = 0; file_idx < TOTAL_FILES; ++file_idx) {
    std::string file_name = "file_" + std::to_string(file_idx + 1) + ".bin";
    fs::path file_path = folder_path / file_name;
    std::ofstream file(file_path, std::ios::binary);

    if (!file.is_open()) {
      std::cerr << "Failed to open file: " << file_path << std::endl;
      return;
    }

    // 每个文件包含 CHUNKS_PER_FILE 个 chunk
    for (size_t chunk_idx = 0; chunk_idx < CHUNKS_PER_FILE; ++chunk_idx) {
      size_t global_chunk_index = file_idx * CHUNKS_PER_FILE + chunk_idx;

      // 生成当前 chunk 的数据（1 到 524288）
      std::vector<uint64_t> chunk_data(ELEMENTS_PER_CHUNK);
      for (size_t i = 0; i < ELEMENTS_PER_CHUNK; ++i) {
        chunk_data[i] = global_chunk_index * ELEMENTS_PER_CHUNK + i + 1;
      }

      // 打乱当前 chunk 的数据
      shuffleChunk(chunk_data, global_chunk_index);

      // 将打乱后的数据写入文件
      file.write(reinterpret_cast<const char *>(chunk_data.data()), CHUNK_SIZE);
    }

    file.close();
    std::cout << "Generated " << file_name << std::endl;
  }
}

int main() {
  fs::path folder_path = "/home/shiwen/project/final/debug"; // 存放数据的文件夹

  // 如果文件夹不存在，则创建
  if (!fs::exists(folder_path)) {
    fs::create_directory(folder_path);
  }

  generateData(folder_path);

  return 0;
}
