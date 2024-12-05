#pragma once

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace final {
class InputReader {
private:
  std::string input_folder_;
  std::vector<std::string> file_names_;

public:
  InputReader(const std::string &folder_name) : input_folder_(folder_name) {
    if (std::filesystem::exists(folder_name) &&
        std::filesystem::is_directory(folder_name)) {
      for (const auto &file :
           std::filesystem::directory_iterator(folder_name)) {
        if (std::filesystem::is_regular_file(file)) {
          file_names_.push_back(file.path().string());
        }
      }
    }
  }
  ~InputReader() = default;
  InputReader(InputReader &&other) {
    input_folder_ = std::move(other.input_folder_);
    file_names_ = std::move(other.file_names_);
  };

  const auto &GetFileNames() { return file_names_; }
  const auto &GetInputFolder() const { return input_folder_; }
};
} // namespace final
