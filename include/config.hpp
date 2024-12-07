#pragma once

#include "LRUReplacer.hpp"

#include <cstdint>
#include <mutex>

namespace final {

// 1. suppose each file is multiple of frame size.

using project_file_id_t = uint32_t;
using file_descrip_t = int32_t;
using OpenedFileCache_t = final::Cache<project_file_id_t, file_descrip_t>;

const uint32_t MEMORY_LIMIT = 1024 * 1024;
// const uint32_t MEMORY_LIMIT = 16 * 1024;
const uint32_t PAGE_SIZE = 4 * 1024;
// const uint32_t PAGE_SIZE = 4 * 1024;
const uint32_t PAGE_VALUE_NUM = PAGE_SIZE / sizeof(uint64_t);
const uint32_t FRAME_NUM = MEMORY_LIMIT / PAGE_SIZE;

const uint32_t WORKER_NUM = 4;

const uint32_t OPEN_FILE_NUMS = 512;
} // namespace final