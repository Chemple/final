#include <chrono>
#include <cstdint>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>
#include <system_error>
#include <thread>
#include <unistd.h>

bool isFileSorted(const std::string &filePath) {
  std::ifstream file(filePath, std::ios::binary);

  if (!file.is_open()) {
    std::cerr << "无法打开文件: " << filePath << std::endl;
    return false;
  }

  uint64_t prevValue;
  bool firstElement = true;
  size_t position = 0; // 文件中的当前位置（按 uint64_t 计算）

  while (file.read(reinterpret_cast<char *>(&prevValue), sizeof(uint64_t))) {
    position++; // 当前元素位置

    if (firstElement) {
      firstElement = false;
    } else {
      uint64_t currentValue;
      file.read(reinterpret_cast<char *>(&currentValue), sizeof(uint64_t));

      // 如果文件结束，确保不会读取超过文件内容
      if (!file) {
        break;
      }

      if (currentValue < prevValue) {
        std::cout << "文件在位置 " << position << " 处无序："
                  << "当前值 = " << currentValue << ", 前一个值 = " << prevValue
                  << std::endl;
        return false; // 发现无序直接返回 false
      }

      prevValue = currentValue; // 更新前一个值为当前值
    }
  }

  file.close(); // 关闭文件（虽然 std::ifstream 会自动处理）
  return true;  // 如果遍历完所有元素，文件是有序的
}

TEST(test, test) {
  std::string filePath =
      "/home/shiwen/project/final/output/output1"; // 需要判断的文件路径
  if (isFileSorted(filePath)) {
    std::cout << "文件是有序的。" << std::endl;
  } else {
    std::cout << "文件不是有序的。" << std::endl;
  }
}
