#include "subreactor.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <deque>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "commands.h"
#include "mpmc_queue.h"
#include "resp.h"
#include "thread_pool.h"
#include "util.h"

namespace dkv_server {
namespace {

struct ConnToken {
  int fd{-1};
  std::uint64_t id{0};
};

struct ResponseMsg {
  ConnToken token;
  std::string payload;
  bool close_after{false};
};

}  // namespace

struct SubReactor::Impl {
  Impl(std::size_t index, dkv::DB* db, const ServerConfig* cfg, ThreadPool* workers)
      : index_(index),
        db_(db),
        cfg_(cfg),
        workers_(workers),
        pending_new_(kPendingNewCap),
        pending_resp_(kPendingRespCap) {}

  void Start() { thread_ = std::thread([this] { Loop(); }); }

  void Stop() {
    stopping_.store(true, std::memory_order_relaxed);
    Notify();
    if (thread_.joinable()) thread_.join();
  }

  void EnqueueNewConn(int fd) {
    while (!pending_new_.Enqueue(fd)) {
      if (stopping_.load(std::memory_order_relaxed)) return;
      std::this_thread::yield();
    }
    Notify();
  }

 private:
  struct Connection {
    ConnToken token;
    std::string in;
    std::size_t in_pos{0};
    std::string out;
    std::size_t out_pos{0};
    bool close_after_write{false};
    bool in_flight{false};
    std::deque<std::vector<std::string>> queue;
  };

  void EnqueueResponse(ResponseMsg msg) {
    while (!pending_resp_.Enqueue(std::move(msg))) {
      if (stopping_.load(std::memory_order_relaxed)) return;
      std::this_thread::yield();
    }
    Notify();
  }

  void Notify() {
    if (!event_fd_.valid()) return;
    std::uint64_t one = 1;
    ssize_t n = ::write(event_fd_.get(), &one, sizeof(one));
    (void)n;
  }

  void Loop() {
    try {
      event_fd_.reset(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
      if (!event_fd_.valid()) ThrowSys("eventfd");

      epoll_fd_.reset(::epoll_create1(EPOLL_CLOEXEC));
      if (!epoll_fd_.valid()) ThrowSys("epoll_create1");

      epoll_event ev{};
      ev.events = EPOLLIN;
      ev.data.fd = event_fd_.get();
      if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, event_fd_.get(), &ev) < 0) ThrowSys("epoll_ctl(ADD eventfd)");

      std::vector<epoll_event> events(256);
      while (!stopping_.load(std::memory_order_relaxed)) {
        int n = ::epoll_wait(epoll_fd_.get(), events.data(), static_cast<int>(events.size()), 1000);
        if (n < 0) {
          if (errno == EINTR) continue;
          ThrowSys("epoll_wait");
        }
        for (int i = 0; i < n; ++i) {
          int fd = events[i].data.fd;
          std::uint32_t e = events[i].events;
          if (fd == event_fd_.get()) {
            DrainEventfd();
            ProcessPending();
            continue;
          }
          auto it = conns_.find(fd);
          if (it == conns_.end()) continue;
          if (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
            CloseConn(it->second);
            conns_.erase(it);
            continue;
          }
          if (e & EPOLLIN) {
            if (!HandleRead(it->second)) {
              CloseConn(it->second);
              conns_.erase(it);
              continue;
            }
          }
          if (e & EPOLLOUT) {
            if (!HandleWrite(it->second)) {
              CloseConn(it->second);
              conns_.erase(it);
              continue;
            }
          }
        }
        ProcessPending();
      }

      for (auto& [fd, c] : conns_) {
        (void)fd;
        CloseConn(c);
      }
      conns_.clear();
    } catch (const std::exception& ex) {
      std::cerr << "[subreactor " << index_ << "] fatal: " << ex.what() << "\n";
    }
  }

  void DrainEventfd() {
    std::uint64_t v = 0;
    for (;;) {
      ssize_t n = ::read(event_fd_.get(), &v, sizeof(v));
      if (n == sizeof(v)) continue;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
      return;
    }
  }

  void ProcessPending() {
    int fd = -1;
    while (pending_new_.TryDequeue(fd)) {
      AddConn(fd);
    }
    ResponseMsg msg;
    while (pending_resp_.TryDequeue(msg)) {
      ApplyResponse(msg);
    }
  }

  void AddConn(int fd) {
    Connection c;
    c.token.fd = fd;
    c.token.id = ++next_conn_id_;
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLRDHUP;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, fd, &ev) < 0) {
      ::close(fd);
      return;
    }
    conns_.emplace(fd, std::move(c));
  }

  void CloseConn(Connection& c) {
    if (c.token.fd >= 0) {
      ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, c.token.fd, nullptr);
      ::close(c.token.fd);
      c.token.fd = -1;
    }
  }

  void ApplyResponse(const ResponseMsg& msg) {
    auto it = conns_.find(msg.token.fd);
    if (it == conns_.end()) return;
    Connection& c = it->second;
    if (c.token.id != msg.token.id) return;

    c.out.append(msg.payload);
    if (msg.close_after) c.close_after_write = true;
    c.in_flight = false;
    UpdateEpollInterest(c);
    MaybeDispatch(c);
  }

  void UpdateEpollInterest(const Connection& c) {
    if (!epoll_fd_.valid() || c.token.fd < 0) return;
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLRDHUP;
    if (c.out_pos < c.out.size()) ev.events |= EPOLLOUT;
    ev.data.fd = c.token.fd;
    ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, c.token.fd, &ev);
  }

  bool HandleRead(Connection& c) {
    char buf[16 * 1024];
    for (;;) {
      ssize_t n = ::read(c.token.fd, buf, sizeof(buf));
      if (n > 0) {
        c.in.append(buf, static_cast<std::size_t>(n));
        continue;
      }
      if (n == 0) return false;
      if (errno == EAGAIN || errno == EWOULDBLOCK) break;
      if (errno == EINTR) continue;
      return false;
    }

    for (;;) {
      std::string_view view(c.in);
      view.remove_prefix(c.in_pos);
      std::size_t consumed = 0;
      std::vector<std::string> args;
      std::string perr;
      resp::ParseResult pr = resp::ParseCommand(view, &consumed, &args, &perr);
      if (pr == resp::ParseResult::kNeedMore) break;
      if (pr == resp::ParseResult::kError) {
        resp::AppendError(c.out, perr.empty() ? "ERR protocol error" : perr);
        c.close_after_write = true;
        UpdateEpollInterest(c);
        return true;
      }
      c.in_pos += consumed;
      if (c.in_pos > 4096 && c.in_pos * 2 > c.in.size()) {
        c.in.erase(0, c.in_pos);
        c.in_pos = 0;
      }
      c.queue.push_back(std::move(args));
    }

    MaybeDispatch(c);
    return true;
  }

  bool HandleWrite(Connection& c) {
    while (c.out_pos < c.out.size()) {
      ssize_t n = ::write(c.token.fd, c.out.data() + c.out_pos, c.out.size() - c.out_pos);
      if (n > 0) {
        c.out_pos += static_cast<std::size_t>(n);
        continue;
      }
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
      if (n < 0 && errno == EINTR) continue;
      return false;
    }

    if (c.out_pos == c.out.size()) {
      c.out.clear();
      c.out_pos = 0;
      UpdateEpollInterest(c);
      if (c.close_after_write) return false;
    } else {
      UpdateEpollInterest(c);
    }
    return true;
  }

  void MaybeDispatch(Connection& c) {
    if (c.in_flight) return;
    if (c.queue.empty()) return;
    if (!workers_) return;

    c.in_flight = true;
    constexpr std::size_t kMaxBatch = 8;
    const std::size_t batch_n = std::min<std::size_t>(kMaxBatch, c.queue.size());
    std::vector<std::vector<std::string>> batch;
    batch.reserve(batch_n);
    for (std::size_t i = 0; i < batch_n; ++i) {
      batch.push_back(std::move(c.queue.front()));
      c.queue.pop_front();
    }
    ConnToken token = c.token;
    dkv::DB* db = db_;
    const ServerConfig* cfg = cfg_;
    Impl* self = this;

    workers_->Submit([self, token, db, cfg, batch = std::move(batch)]() mutable {
      ResponseMsg msg;
      msg.token = token;
      for (auto& cmd : batch) {
        CommandResult cr = ExecuteCommand(cmd, db, cfg);
        msg.payload.append(cr.payload);
        if (cr.close_after) {
          msg.close_after = true;
          break;
        }
      }
      self->EnqueueResponse(std::move(msg));
    });
  }

  std::size_t index_{0};
  dkv::DB* db_{nullptr};
  const ServerConfig* cfg_{nullptr};
  ThreadPool* workers_{nullptr};

  std::atomic_bool stopping_{false};
  std::thread thread_;

  UniqueFd event_fd_;
  UniqueFd epoll_fd_;

  static constexpr std::size_t kPendingNewCap = 4096;
  static constexpr std::size_t kPendingRespCap = 16384;
  MpmcQueue<int> pending_new_;
  MpmcQueue<ResponseMsg> pending_resp_;

  std::unordered_map<int, Connection> conns_;
  std::uint64_t next_conn_id_{0};
};

SubReactor::SubReactor(std::size_t index, dkv::DB* db, const ServerConfig* cfg, ThreadPool* workers)
    : impl_(std::make_unique<Impl>(index, db, cfg, workers)) {}

SubReactor::~SubReactor() { Stop(); }

void SubReactor::Start() { impl_->Start(); }

void SubReactor::Stop() {
  if (!impl_) return;
  impl_->Stop();
}

void SubReactor::EnqueueNewConn(int fd) { impl_->EnqueueNewConn(fd); }

}  // namespace dkv_server
