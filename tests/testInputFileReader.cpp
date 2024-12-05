#include "ChunkManager.hpp"
#include "InputReader.hpp"
#include "spdlog/spdlog.h"

#include <gtest/gtest.h>
#include <memory>
#include <utility>

TEST(TestInputReader, SimpleTest) {
  auto input_obj = final::InputReader("/home/shiwen/project/final/input");
  auto file_names = input_obj.GetFileNames();
  for (const auto &file_name : file_names) {
    spdlog::info(file_name);
  }
}

TEST(TestOpener, SimpleTest) {
  auto input_obj =
      std::make_shared<final::InputReader>("/home/shiwen/project/final/input");
  auto file_names = input_obj->GetFileNames();
  for (const auto &file_name : file_names) {
    spdlog::info(file_name);
  }
  const auto &file_num = input_obj->GetFileNames().size();
  auto opener = final::FileOpener(std::move(input_obj));

  for (auto i = 0; i < file_num; i++) {
    spdlog::info("the fd is {}", opener.GetFd(i));
  }
}
