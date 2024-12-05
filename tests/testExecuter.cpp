
#include "Executer.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <utility>

TEST(ExecuterTest, SimpleTest) {
  auto excuter_ptr = std::make_unique<final::Executer>(
      std::string("/home/shiwen/project/final/debug"),
      std::string("/home/shiwen/project/final/output/output"));
  excuter_ptr->LaunchChunkSort();
}
