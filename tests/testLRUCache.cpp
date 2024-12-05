#include "LRUReplacer.hpp"
#include "spdlog/spdlog.h"
#include <cstdint>
#include <gtest/gtest.h>

TEST(LRUCacheTest, SimpleTest0) {
  auto lrucache = final::Cache<uint32_t, int32_t>(4);
  lrucache.insert(1, 1);
  lrucache.insert(2, 2);
  lrucache.insert(3, 3);
  lrucache.insert(4, 4);
  auto ret_v = -1;
  auto obj0 = lrucache.tryGet(1, ret_v);
  spdlog::info("the return is {}", obj0);
  lrucache.insert(5, 5);
  auto obj1 = lrucache.tryGet(1, ret_v);
  spdlog::info("the return is {}", obj1);
}

TEST(LRUCacheTest, SimpleTest1) {
  auto lrucache = final::Cache<uint32_t, int32_t>(4);
  lrucache.insert(1, 1);
  lrucache.insert(2, 2);
  lrucache.insert(3, 3);
  lrucache.insert(4, 4);
  auto ret_v = -1;
  // auto obj0 = lrucache.tryGet(1, ret_v);
  // spdlog::info("the return is {}", obj0);
  lrucache.insert(5, 5);
  auto obj1 = lrucache.tryGet(1, ret_v);
  spdlog::info("the return is {}", obj1);
}

TEST(LRUCacheTest, ConcurrentTest) {
  using LCache = final::Cache<std::string, std::string, std::mutex>;
  auto cachePrint2 = [&](const LCache &c) {
    std::cout << "Cache (size: " << c.size() << ") (max=" << c.getMaxSize()
              << ") (allowed:" << c.getMaxAllowedSize() << ")" << std::endl;
    size_t index = 0;
    auto nodePrint = [&](const LCache::node_type &n) {
      std::cout << " ... [" << ++index << "] " << n.key << " => " << n.value
                << std::endl;
    };
    c.cwalk(nodePrint);
  };
  // with a lock
  LCache lc(25);
  auto worker = [&]() {
    std::ostringstream os;
    os << std::this_thread::get_id();
    std::string id = os.str();

    for (int i = 0; i < 10; i++) {
      std::ostringstream os2;
      os2 << "id:" << id << ":" << i;
      lc.insert(os2.str(), id);
    }
  };
  std::vector<std::unique_ptr<std::thread>> workers;
  workers.reserve(100);
  for (int i = 0; i < 100; i++) {
    workers.push_back(std::unique_ptr<std::thread>(new std::thread(worker)));
  }

  for (const auto &w : workers) {
    w->join();
  }
  std::cout << "... workers finished!" << std::endl;
  cachePrint2(lc);
}