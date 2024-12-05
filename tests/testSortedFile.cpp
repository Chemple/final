#include <cstdint>
#include <fstream>
#include <iostream>

bool isFileSorted(const std::string &filePath) {
  std::ifstream file(filePath, std::ios::binary);

  if (!file.is_open()) {
    std::cerr << "无法打开文件: " << filePath << std::endl;
    return false;
  }

  uint64_t prevValue;
  bool firstElement = true;
  size_t position = 0; // 文件中的当前位置（按 uint64_t 计算）

  int wrong = 0;

  while (file.read(reinterpret_cast<char *>(&prevValue), sizeof(uint64_t))) {
    if (firstElement) {
      firstElement = false;
    } else {
      uint64_t currentValue;
      file.read(reinterpret_cast<char *>(&currentValue), sizeof(uint64_t));
      position++; // 当前元素位置

      if (currentValue < prevValue) {
        wrong++;
        // 如果当前值小于前一个值，说明文件不是有序的
        std::cout << "文件在位置 " << position << " 处无序："
                  << "当前值 = " << currentValue << ", 前一个值 = " << prevValue
                  << std::endl;
        // return false;
      }
      prevValue = currentValue; // 更新前一个值为当前值
    }
  }

  file.close();
  return true; // 如果遍历完所有元素，文件是有序的
}

int main() {
  std::string filePath =
      "/home/shiwen/project/final/output/output0"; // 需要判断的文件路径
  if (isFileSorted(filePath)) {
    std::cout << "文件是有序的。" << std::endl;
  } else {
    std::cout << "文件不是有序的。" << std::endl;
  }

  return 0;
}
