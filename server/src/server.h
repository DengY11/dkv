#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include <dkv/db.h>

namespace dkv_server {

class ThreadPool;
class SubReactor;
class Acceptor;

struct ServerConfig {
  std::string bind{"0.0.0.0"};
  int port{6379};
  std::size_t subreactors{0};
  std::size_t workers{0};
  dkv::Options dkv_options{};
};

class DkvServer {
 public:
  explicit DkvServer(ServerConfig cfg);
  DkvServer(const DkvServer&) = delete;
  DkvServer& operator=(const DkvServer&) = delete;
  ~DkvServer();

  void Start();
  void Stop();

 private:
  ServerConfig cfg_;
  std::unique_ptr<dkv::DB> db_;

  std::unique_ptr<ThreadPool> workers_;
  std::vector<std::unique_ptr<SubReactor>> subreactors_;
  std::unique_ptr<Acceptor> acceptor_;
  std::atomic_bool started_{false};
};

}  // namespace dkv_server
