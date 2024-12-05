#pragma once

#include "InputReader.hpp"
#include "config.hpp"
#include "spdlog/spdlog.h"

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace final {
struct BufferPage {
  char *data;
  std::mutex page_mutex_{};
};

struct PageGuard {
  using Guard = std::unique_lock<std::mutex>;
  BufferPage *page_;
  Guard guard_;
  PageGuard(BufferPage *page) : page_(page), guard_(page_->page_mutex_) {}
  PageGuard(const PageGuard &) = delete;
  PageGuard &operator=(const PageGuard &) = delete;
  PageGuard(PageGuard &&other) noexcept
      : page_(other.page_), guard_(std::move(other.guard_)) {
    other.page_ = nullptr;
  }
  PageGuard &operator=(PageGuard &&other) noexcept {
    if (this != &other) {
      page_ = other.page_;
      guard_ = std::move(other.guard_);
      other.page_ = nullptr;
    }
    return *this;
  };
  BufferPage *GetPage() { return page_; }
};

class BufferPool {
private:
  using Guard = std::lock_guard<std::mutex>;
  std::vector<BufferPage *> frames_;
  const uint32_t frame_num_ = FRAME_NUM;
  const uint32_t page_size_ = PAGE_SIZE;

public:
  BufferPool() {
    frames_.resize(frame_num_);

    for (auto &frame : frames_) {
      frame = new BufferPage;
      frame->data = (char *)aligned_alloc(4096, page_size_);
    }
  }

  BufferPool(BufferPool &&) = delete;
  BufferPool &operator=(BufferPool &&) = delete;
  ~BufferPool() = default;

  auto GetFrame(uint32_t frame_id) {
    assert(frame_id <= frame_num_);
    return PageGuard{frames_[frame_id]};
  }

  auto GetFrameNum() { return frame_num_; }
};
} // namespace final
