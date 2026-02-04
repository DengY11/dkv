#include <dkv/resp_client.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

int main(int argc, char** argv) {
  const char* host = argc > 1 ? argv[1] : "127.0.0.1";
  int port = argc > 2 ? std::atoi(argv[2]) : 6379;
  int threads = argc > 3 ? std::atoi(argv[3]) : 20;
  int keys_per_thread = argc > 4 ? std::atoi(argv[4]) : 20000;
  int rounds = argc > 5 ? std::atoi(argv[5]) : 3;

  if (threads <= 0 || keys_per_thread <= 0 || rounds <= 0) {
    std::cerr << "usage: " << argv[0] << " [host] [port] [threads] [keys_per_thread] [rounds]\n";
    return 1;
  }

  std::atomic<bool> start{false};
  std::atomic<int> ready{0};
  std::atomic<int> errors{0};

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(threads));

  for (int t = 0; t < threads; ++t) {
    workers.emplace_back([&, t] {
      try {
        dkv::RespClient client(host, static_cast<std::uint16_t>(port));
        auto kv = client.kv();
        ready.fetch_add(1, std::memory_order_relaxed);
        while (!start.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }

        const int base = t * keys_per_thread;
        for (int r = 0; r < rounds; ++r) {
          for (int i = 0; i < keys_per_thread; ++i) {
            std::string key = "k" + std::to_string(base + i);
            std::string val = "v" + std::to_string(base + i);
            if (!kv.SetOk(key, val)) {
              errors.fetch_add(1, std::memory_order_relaxed);
            }
          }
          for (int i = 0; i < keys_per_thread; ++i) {
            std::string key = "k" + std::to_string(base + i);
            std::string expect = "v" + std::to_string(base + i);
            auto got = kv.GetString(key);
            if (!got || *got != expect) {
              errors.fetch_add(1, std::memory_order_relaxed);
            }
          }
        }
      } catch (const std::exception&) {
        errors.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  while (ready.load(std::memory_order_relaxed) != threads) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  auto start_ts = std::chrono::steady_clock::now();
  start.store(true, std::memory_order_release);

  for (auto& t : workers) {
    t.join();
  }

  auto end_ts = std::chrono::steady_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_ts - start_ts).count();

  const std::int64_t ops = static_cast<std::int64_t>(threads) * keys_per_thread * rounds * 2;
  const double seconds = ms / 1000.0;
  const double ops_per_sec = seconds > 0.0 ? ops / seconds : 0.0;
  std::cout << "threads=" << threads << " keys_per_thread=" << keys_per_thread
            << " rounds=" << rounds << " elapsed_ms=" << ms
            << " ops=" << ops << " ops_per_sec=" << ops_per_sec
            << " errors=" << errors.load() << "\n";
  return 0;
}
