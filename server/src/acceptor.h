#pragma once

#include <memory>
#include <string>
#include <vector>

namespace dkv_server {

class SubReactor;

class Acceptor {
 public:
  Acceptor(std::string bind, int port, std::vector<SubReactor*> subs);
  Acceptor(const Acceptor&) = delete;
  Acceptor& operator=(const Acceptor&) = delete;
  ~Acceptor();

  void Start();
  void Stop();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dkv_server

