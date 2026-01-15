#pragma once

#include <cstddef>
#include <memory>

namespace dkv {
class DB;
}  // namespace dkv

namespace dkv_server {

class ThreadPool;
struct ServerConfig;

class SubReactor {
 public:
  SubReactor(std::size_t index, dkv::DB* db, const ServerConfig* cfg, ThreadPool* workers);
  SubReactor(const SubReactor&) = delete;
  SubReactor& operator=(const SubReactor&) = delete;
  ~SubReactor();

  void Start();
  void Stop();

  void EnqueueNewConn(int fd);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dkv_server

