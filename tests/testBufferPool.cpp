#include "Buffer.hpp" // 包含 BufferPool 和 PageGuard 的定义
#include <atomic>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

namespace final {

TEST(PageGuardTest, MoveConstructor) {

  BufferPage page;
  page.data = (char *)std::malloc(PAGE_SIZE);
  PageGuard guard1(&page);

  PageGuard guard2(std::move(guard1));

  EXPECT_EQ(guard1.GetPage(), nullptr);
  EXPECT_NE(guard2.GetPage(), nullptr);
}

TEST(PageGuardTest, MoveAssignmentOperator) {

  BufferPage page;
  page.data = (char *)std::malloc(PAGE_SIZE);
  PageGuard guard1(&page);

  BufferPage page2;
  page2.data = (char *)std::malloc(PAGE_SIZE);
  PageGuard guard2(&page2);

  guard2 = std::move(guard1);

  EXPECT_EQ(guard1.GetPage(), nullptr);
  EXPECT_NE(guard2.GetPage(), nullptr);
}

TEST(BufferPoolTest, GetFrame) {
  BufferPool pool;

  auto guard = pool.GetFrame(0);
  EXPECT_NE(guard.GetPage(), nullptr);

  auto guard2 = pool.GetFrame(1);
  EXPECT_NE(guard2.GetPage(), guard.GetPage());
}

TEST(BufferPoolTest, MultiThreadedAccess) {
  BufferPool pool;
  // std::atomic<int> counter(0);
  int counter = 0;

  auto task = [&]() {
    for (int i = 0; i < 1000; ++i) {
      auto guard = pool.GetFrame(0);
      if (guard.GetPage() != nullptr) {
        // counter.fetch_add(1, std::memory_order_relaxed);
        counter++;
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < 64; ++i) {
    threads.emplace_back(task);
  }

  for (auto &t : threads) {
    t.join();
  }

  // EXPECT_EQ(counter.load(), 64000);
  EXPECT_EQ(counter, 64000);
}

} // namespace final