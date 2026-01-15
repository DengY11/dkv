#include "acceptor.h"

#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <iostream>
#include <system_error>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "subreactor.h"
#include "util.h"

namespace dkv_server {

struct Acceptor::Impl {
  Impl(std::string bind, int port, std::vector<SubReactor*> subs) : bind_(std::move(bind)), port_(port), subs_(std::move(subs)) {}

  void Start() {
    Setup();
    thread_ = std::thread([this] { Loop(); });
  }

  void Stop() {
    stopping_.store(true, std::memory_order_relaxed);
    if (thread_.joinable()) thread_.join();
  }

 private:
  void Setup() {
    listen_fd_.reset(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!listen_fd_.valid()) ThrowSys("socket");
    SetReuseAddr(listen_fd_.get());
    SetKeepAlive(listen_fd_.get());

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port_));
    if (::inet_pton(AF_INET, bind_.c_str(), &addr.sin_addr) != 1) {
      throw std::runtime_error("invalid bind address: " + bind_);
    }
    if (::bind(listen_fd_.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
      int err = errno;
      throw std::system_error(err, std::generic_category(), "bind(" + bind_ + ":" + std::to_string(port_) + ")");
    }
    if (::listen(listen_fd_.get(), SOMAXCONN) < 0) {
      int err = errno;
      throw std::system_error(err, std::generic_category(), "listen(" + bind_ + ":" + std::to_string(port_) + ")");
    }

    epoll_fd_.reset(::epoll_create1(EPOLL_CLOEXEC));
    if (!epoll_fd_.valid()) ThrowSys("epoll_create1");
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd_.get();
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, listen_fd_.get(), &ev) < 0) ThrowSys("epoll_ctl(ADD listen)");
  }

  void Loop() {
    try {
      std::vector<epoll_event> events(16);
      while (!stopping_.load(std::memory_order_relaxed)) {
        int n = ::epoll_wait(epoll_fd_.get(), events.data(), static_cast<int>(events.size()), 1000);
        if (n < 0) {
          if (errno == EINTR) continue;
          ThrowSys("epoll_wait");
        }
        for (int i = 0; i < n; ++i) {
          if (events[i].data.fd != listen_fd_.get()) continue;
          AcceptLoop();
        }
      }
    } catch (const std::exception& ex) {
      std::cerr << "[acceptor] fatal: " << ex.what() << "\n";
    }
  }

  void AcceptLoop() {
    for (;;) {
      int fd = ::accept4(listen_fd_.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (fd >= 0) {
        try {
          SetTcpNoDelay(fd);
          SetKeepAlive(fd);
        } catch (...) {
          ::close(fd);
          continue;
        }
        if (subs_.empty()) {
          ::close(fd);
          continue;
        }
        std::size_t idx = rr_.fetch_add(1, std::memory_order_relaxed) % subs_.size();
        subs_[idx]->EnqueueNewConn(fd);
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) return;
      if (errno == EINTR) continue;
      return;
    }
  }

  std::string bind_;
  int port_{0};
  std::vector<SubReactor*> subs_;
  std::atomic_bool stopping_{false};
  std::atomic_size_t rr_{0};
  std::thread thread_;
  UniqueFd listen_fd_;
  UniqueFd epoll_fd_;
};

Acceptor::Acceptor(std::string bind, int port, std::vector<SubReactor*> subs)
    : impl_(std::make_unique<Impl>(std::move(bind), port, std::move(subs))) {}

Acceptor::~Acceptor() { Stop(); }

void Acceptor::Start() { impl_->Start(); }

void Acceptor::Stop() {
  if (!impl_) return;
  impl_->Stop();
}

}  // namespace dkv_server
