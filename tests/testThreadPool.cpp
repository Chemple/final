#include "ThreadPool.hpp"
#include "spdlog/spdlog.h"
#include <future>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

TEST(TestThreadPool, Test_simple) {
  auto threadPool = final::ThreadPool(16);
  auto results = std::vector<std::future<int>>();

  auto in_memory_sort = [](int i) {
    spdlog::info("the i {}", i);
    return i * i;
  };

  for (auto i = 0; i < 8; i++) {
    results.push_back(threadPool.enqueue(in_memory_sort, i));
  }

  for (auto &&result : results)
    spdlog::info("the result is {}", result.get());
}