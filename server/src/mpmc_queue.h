#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

namespace dkv_server {

// Bounded MPMC ring buffer (Vyukov).
template <typename T>
class MpmcQueue {
 public:
  explicit MpmcQueue(std::size_t capacity)
      : capacity_(RoundUpPow2(capacity)), mask_(capacity_ - 1), buffer_(capacity_) {
    for (std::size_t i = 0; i < capacity_; ++i) {
      buffer_[i].seq.store(i, std::memory_order_relaxed);
    }
  }

  MpmcQueue(const MpmcQueue&) = delete;
  MpmcQueue& operator=(const MpmcQueue&) = delete;
  ~MpmcQueue() {
    if constexpr (std::is_default_constructible_v<T>) {
      T tmp;
      while (TryDequeue(tmp)) {
      }
    }
  }

  bool Enqueue(T value) {
    Cell* cell = nullptr;
    std::size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &buffer_[pos & mask_];
      std::size_t seq = cell->seq.load(std::memory_order_acquire);
      std::intptr_t diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
      if (diff == 0) {
        if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          break;
        }
      } else if (diff < 0) {
        return false;
      } else {
        pos = enqueue_pos_.load(std::memory_order_relaxed);
      }
    }
    new (&cell->storage) T(std::move(value));
    cell->seq.store(pos + 1, std::memory_order_release);
    return true;
  }

  bool TryDequeue(T& out) {
    Cell* cell = nullptr;
    std::size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &buffer_[pos & mask_];
      std::size_t seq = cell->seq.load(std::memory_order_acquire);
      std::intptr_t diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);
      if (diff == 0) {
        if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          break;
        }
      } else if (diff < 0) {
        return false;
      } else {
        pos = dequeue_pos_.load(std::memory_order_relaxed);
      }
    }
    T* data = std::launder(reinterpret_cast<T*>(&cell->storage));
    out = std::move(*data);
    data->~T();
    cell->seq.store(pos + mask_ + 1, std::memory_order_release);
    return true;
  }

 private:
  static std::size_t RoundUpPow2(std::size_t n) {
    if (n < 2) return 2;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    if (sizeof(std::size_t) == 8) n |= n >> 32;
    return n + 1;
  }

  struct Cell {
    std::atomic<std::size_t> seq{};
    std::aligned_storage_t<sizeof(T), alignof(T)> storage;
  };

  const std::size_t capacity_;
  const std::size_t mask_;
  std::vector<Cell> buffer_;
  std::atomic<std::size_t> enqueue_pos_{0};
  std::atomic<std::size_t> dequeue_pos_{0};
};

}  // namespace dkv_server
