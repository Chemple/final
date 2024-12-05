#include "ChunkManager.hpp"
#include "spdlog/spdlog.h"
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <thread>
#include <utility>

TEST(TestSlice, TetsInfo) {
  auto input_reader_ptr = std::make_unique<final::InputReader>(
      final::InputReader("/home/shiwen/project/final/input"));
  auto opener_ptr =
      std::make_unique<final::FileOpener>(std::move(input_reader_ptr));
  auto chunk_manager = final::ChunkManager(
      std::move(opener_ptr),
      std::string("/home/shiwen/project/final/output/output0"),
      std::string("/home/shiwen/project/final/output/output1"));
  auto chunk_infos = chunk_manager.GetChunkInfo();
  spdlog::info("the number of slice is {}", chunk_manager.GetChunkNum());
  spdlog::info("the size of infos vec is {}", chunk_infos.size());
  // for (const auto &slice_info : slice_infos) {
  //   spdlog::info("the slice_info is ");
  // }

  auto thread = std::thread{[]() { spdlog::info("test"); }};
  thread.join();
}