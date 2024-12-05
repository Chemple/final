
#include "Executer.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <utility>

TEST(ExecuterTest, SimpleTest) {
  auto excuter_ptr = std::make_unique<final::Executer>(
      std::string("/home/shiwen/project/final/input"),
      std::string("/home/shiwen/project/final/output/output0"),
      std::string("/home/shiwen/project/final/output/output1"));
  excuter_ptr->LaunchChunkSort();
  excuter_ptr->LaunchMergeSort();
}
