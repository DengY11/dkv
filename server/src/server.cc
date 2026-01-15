#include "server.h"

#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <optional>

#include <dkv/filename.h>

#include "resp.h"
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

struct CommandResult {
  std::string payload;
  bool close_after{false};
};

std::string UpperAscii(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (unsigned char c : s) out.push_back(static_cast<char>(std::toupper(c)));
  return out;
}

std::string LowerAscii(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (unsigned char c : s) out.push_back(static_cast<char>(std::tolower(c)));
  return out;
}

bool GlobMatch(std::string_view pattern, std::string_view text) {
  std::size_t p = 0;
  std::size_t t = 0;
  std::size_t star = std::string_view::npos;
  std::size_t match = 0;
  while (t < text.size()) {
    if (p < pattern.size() && (pattern[p] == '?' || pattern[p] == text[t])) {
      ++p;
      ++t;
      continue;
    }
    if (p < pattern.size() && pattern[p] == '*') {
      star = p++;
      match = t;
      continue;
    }
    if (star != std::string_view::npos) {
      p = star + 1;
      t = ++match;
      continue;
    }
    return false;
  }
  while (p < pattern.size() && pattern[p] == '*') ++p;
  return p == pattern.size();
}

bool ParseInt(std::string_view s, std::int64_t* out) {
  if (s.empty()) return false;
  std::int64_t v = 0;
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
  if (ec != std::errc() || ptr != s.data() + s.size()) return false;
  *out = v;
  return true;
}

CommandResult HandleCommand(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg) {
  CommandResult r;
  if (args.empty()) {
    resp::AppendError(r.payload, "ERR empty command");
    return r;
  }

  const std::string cmd = UpperAscii(args[0]);
  if (cmd == "PING") {
    if (args.size() == 1) {
      resp::AppendSimpleString(r.payload, "PONG");
    } else {
      resp::AppendBulkString(r.payload, args[1]);
    }
    return r;
  }
  if (cmd == "ECHO") {
    if (args.size() != 2) {
      resp::AppendError(r.payload, "ERR wrong number of arguments for 'echo' command");
      return r;
    }
    resp::AppendBulkString(r.payload, args[1]);
    return r;
  }
  if (cmd == "QUIT") {
    resp::AppendSimpleString(r.payload, "OK");
    r.close_after = true;
    return r;
  }
  if (cmd == "COMMAND") {
    resp::AppendArrayHeader(r.payload, 0);
    return r;
  }
  if (cmd == "CLIENT") {
    // Accept common client metadata commands like: CLIENT SETINFO LIB-NAME xxx
    resp::AppendSimpleString(r.payload, "OK");
    return r;
  }
  if (cmd == "HELLO") {
    // RESP2 reply: array of key/value pairs, keep it minimal.
    resp::AppendArrayHeader(r.payload, 14);
    resp::AppendBulkString(r.payload, "server");
    resp::AppendBulkString(r.payload, "dkv");
    resp::AppendBulkString(r.payload, "version");
    resp::AppendBulkString(r.payload, "0.1");
    resp::AppendBulkString(r.payload, "proto");
    resp::AppendBulkString(r.payload, "2");
    resp::AppendBulkString(r.payload, "id");
    resp::AppendBulkString(r.payload, "0");
    resp::AppendBulkString(r.payload, "mode");
    resp::AppendBulkString(r.payload, "standalone");
    resp::AppendBulkString(r.payload, "role");
    resp::AppendBulkString(r.payload, "master");
    resp::AppendBulkString(r.payload, "modules");
    resp::AppendArrayHeader(r.payload, 0);
    return r;
  }
  if (cmd == "INFO") {
    std::string info;
    info += "# Server\n";
    info += "redis_version:0.1\n";
    info += "dkv_server:dkv-server\n";
    resp::AppendBulkString(r.payload, info);
    return r;
  }
  if (cmd == "CONFIG") {
    if (!cfg) {
      resp::AppendError(r.payload, "ERR config not available");
      return r;
    }
    if (args.size() < 2) {
      resp::AppendError(r.payload, "ERR wrong number of arguments for 'config' command");
      return r;
    }
    const std::string sub = UpperAscii(args[1]);
    if (sub == "GET") {
      if (args.size() != 3) {
        resp::AppendError(r.payload, "ERR wrong number of arguments for 'config get' command");
        return r;
      }
      const std::string pat = LowerAscii(args[2]);
      const auto& o = cfg->dkv_options;
      auto yesno = [](bool b) { return b ? "yes" : "no"; };

      std::vector<std::pair<std::string, std::string>> entries;
      entries.emplace_back("bind", cfg->bind);
      entries.emplace_back("port", std::to_string(cfg->port));
      entries.emplace_back("subreactors", std::to_string(cfg->subreactors));
      entries.emplace_back("workers", std::to_string(cfg->workers));

      entries.emplace_back("data-dir", o.data_dir.string());
      entries.emplace_back("wal-path", (o.data_dir / std::string(dkv::kWalActiveName)).string());
      entries.emplace_back("sst-dir", (o.data_dir / "sst").string());
      entries.emplace_back("manifest-path", (o.data_dir / std::string(dkv::kManifestName)).string());
      // Minimal Redis-compatible config keys (best-effort, for tools like redis-benchmark).
      entries.emplace_back("dir", o.data_dir.string());
      entries.emplace_back("dbfilename", "");
      entries.emplace_back("appendonly", "no");
      entries.emplace_back("save", "");

      entries.emplace_back("memtable-soft-limit-bytes", std::to_string(o.memtable_soft_limit_bytes));
      entries.emplace_back("sync-wal", yesno(o.sync_wal));
      entries.emplace_back("sstable-target-size-bytes", std::to_string(o.sstable_target_size_bytes));
      entries.emplace_back("sstable-block-size-bytes", std::to_string(o.sstable_block_size_bytes));
      entries.emplace_back("bloom-bits-per-key", std::to_string(o.bloom_bits_per_key));
      entries.emplace_back("bloom-cache-capacity-bytes", std::to_string(o.bloom_cache_capacity_bytes));
      entries.emplace_back("raw-block-cache-capacity-bytes", std::to_string(o.raw_block_cache_capacity_bytes));
      entries.emplace_back("block-cache-capacity-bytes", std::to_string(o.block_cache_capacity_bytes));
      entries.emplace_back("level0-file-limit", std::to_string(o.level0_file_limit));
      entries.emplace_back("level-base-bytes", std::to_string(o.level_base_bytes));
      entries.emplace_back("level-size-multiplier", std::to_string(o.level_size_multiplier));
      entries.emplace_back("max-levels", std::to_string(o.max_levels));
      entries.emplace_back("flush-thread-count", std::to_string(o.flush_thread_count));
      entries.emplace_back("max-immutable-memtables", std::to_string(o.max_immutable_memtables));
      entries.emplace_back("compaction-thread-count", std::to_string(o.compaction_thread_count));
      entries.emplace_back("wal-sync-interval-ms", std::to_string(o.wal_sync_interval_ms));
      entries.emplace_back("enable-crc", yesno(o.enable_crc));
      entries.emplace_back("verify-sstable-crc", yesno(o.verify_sstable_crc));
      entries.emplace_back("enable-compress", yesno(o.enable_compress));

      std::vector<std::pair<std::string, std::string>> matched;
      matched.reserve(entries.size());
      for (auto& kv : entries) {
        if (GlobMatch(pat, LowerAscii(kv.first))) matched.push_back(kv);
      }

      resp::AppendArrayHeader(r.payload, matched.size() * 2);
      for (auto& kv : matched) {
        resp::AppendBulkString(r.payload, kv.first);
        resp::AppendBulkString(r.payload, kv.second);
      }
      return r;
    }
    if (sub == "RESETSTAT" || sub == "REWRITE") {
      resp::AppendSimpleString(r.payload, "OK");
      return r;
    }
    resp::AppendError(r.payload, "ERR unsupported CONFIG subcommand");
    return r;
  }

  if (!db) {
    resp::AppendError(r.payload, "ERR db not ready");
    return r;
  }

  if (cmd == "GET") {
    if (args.size() != 2) {
      resp::AppendError(r.payload, "ERR wrong number of arguments for 'get' command");
      return r;
    }
    std::string value;
    dkv::Status s = db->Get(dkv::ReadOptions{}, args[1], value);
    if (s.ok()) {
      resp::AppendBulkString(r.payload, value);
    } else if (s.code() == dkv::Status::Code::kNotFound) {
      resp::AppendNullBulkString(r.payload);
    } else {
      resp::AppendError(r.payload, std::string("ERR ") + s.ToString());
    }
    return r;
  }

  if (cmd == "SET") {
    if (args.size() != 3) {
      resp::AppendError(r.payload, "ERR wrong number of arguments for 'set' command");
      return r;
    }
    dkv::Status s = db->Put(dkv::WriteOptions{}, args[1], args[2]);
    if (s.ok()) {
      resp::AppendSimpleString(r.payload, "OK");
    } else {
      resp::AppendError(r.payload, std::string("ERR ") + s.ToString());
    }
    return r;
  }

  if (cmd == "DEL") {
    if (args.size() < 2) {
      resp::AppendError(r.payload, "ERR wrong number of arguments for 'del' command");
      return r;
    }
    std::int64_t removed = 0;
    for (std::size_t i = 1; i < args.size(); ++i) {
      std::string tmp;
      dkv::Status gs = db->Get(dkv::ReadOptions{.fill_cache = false}, args[i], tmp);
      if (gs.ok()) {
        dkv::Status ds = db->Delete(dkv::WriteOptions{}, args[i]);
        if (!ds.ok()) {
          resp::AppendError(r.payload, std::string("ERR ") + ds.ToString());
          return r;
        }
        ++removed;
      } else if (gs.code() == dkv::Status::Code::kNotFound) {
        // noop
      } else {
        resp::AppendError(r.payload, std::string("ERR ") + gs.ToString());
        return r;
      }
    }
    resp::AppendInteger(r.payload, removed);
    return r;
  }

  if (cmd == "EXISTS") {
    if (args.size() < 2) {
      resp::AppendError(r.payload, "ERR wrong number of arguments for 'exists' command");
      return r;
    }
    std::int64_t cnt = 0;
    for (std::size_t i = 1; i < args.size(); ++i) {
      std::string tmp;
      dkv::Status s = db->Get(dkv::ReadOptions{.fill_cache = false}, args[i], tmp);
      if (s.ok()) {
        ++cnt;
      } else if (s.code() == dkv::Status::Code::kNotFound) {
        // noop
      } else {
        resp::AppendError(r.payload, std::string("ERR ") + s.ToString());
        return r;
      }
    }
    resp::AppendInteger(r.payload, cnt);
    return r;
  }

  if (cmd == "MGET") {
    if (args.size() < 2) {
      resp::AppendError(r.payload, "ERR wrong number of arguments for 'mget' command");
      return r;
    }
    resp::AppendArrayHeader(r.payload, args.size() - 1);
    for (std::size_t i = 1; i < args.size(); ++i) {
      std::string value;
      dkv::Status s = db->Get(dkv::ReadOptions{.fill_cache = false}, args[i], value);
      if (s.ok()) {
        resp::AppendBulkString(r.payload, value);
      } else if (s.code() == dkv::Status::Code::kNotFound) {
        resp::AppendNullBulkString(r.payload);
      } else {
        r.payload.clear();
        resp::AppendError(r.payload, std::string("ERR ") + s.ToString());
        return r;
      }
    }
    return r;
  }

  if (cmd == "MSET") {
    if (args.size() < 3 || ((args.size() - 1) % 2) != 0) {
      resp::AppendError(r.payload, "ERR wrong number of arguments for 'mset' command");
      return r;
    }
    dkv::WriteBatch batch;
    for (std::size_t i = 1; i < args.size(); i += 2) {
      batch.Put(args[i], args[i + 1]);
    }
    dkv::Status s = db->Write(dkv::WriteOptions{}, batch);
    if (s.ok()) {
      resp::AppendSimpleString(r.payload, "OK");
    } else {
      resp::AppendError(r.payload, std::string("ERR ") + s.ToString());
    }
    return r;
  }

  auto do_incr = [&](const std::string& key, std::int64_t by) -> CommandResult {
    std::string cur;
    dkv::Status gs = db->Get(dkv::ReadOptions{.fill_cache = false}, key, cur);
    std::int64_t val = 0;
    if (gs.ok()) {
      if (!ParseInt(cur, &val)) {
        resp::AppendError(r.payload, "ERR value is not an integer or out of range");
        return r;
      }
    } else if (gs.code() == dkv::Status::Code::kNotFound) {
      val = 0;
    } else {
      resp::AppendError(r.payload, std::string("ERR ") + gs.ToString());
      return r;
    }
    if ((by > 0 && val > std::numeric_limits<std::int64_t>::max() - by) ||
        (by < 0 && val < std::numeric_limits<std::int64_t>::min() - by)) {
      resp::AppendError(r.payload, "ERR increment or decrement would overflow");
      return r;
    }
    val += by;
    dkv::Status ps = db->Put(dkv::WriteOptions{}, key, std::to_string(val));
    if (!ps.ok()) {
      resp::AppendError(r.payload, std::string("ERR ") + ps.ToString());
      return r;
    }
    resp::AppendInteger(r.payload, val);
    return r;
  };

  if (cmd == "INCR") {
    if (args.size() != 2) {
      resp::AppendError(r.payload, "ERR wrong number of arguments for 'incr' command");
      return r;
    }
    return do_incr(args[1], 1);
  }
  if (cmd == "DECR") {
    if (args.size() != 2) {
      resp::AppendError(r.payload, "ERR wrong number of arguments for 'decr' command");
      return r;
    }
    return do_incr(args[1], -1);
  }
  if (cmd == "INCRBY") {
    if (args.size() != 3) {
      resp::AppendError(r.payload, "ERR wrong number of arguments for 'incrby' command");
      return r;
    }
    std::int64_t by = 0;
    if (!ParseInt(args[2], &by)) {
      resp::AppendError(r.payload, "ERR value is not an integer or out of range");
      return r;
    }
    return do_incr(args[1], by);
  }
  if (cmd == "DECRBY") {
    if (args.size() != 3) {
      resp::AppendError(r.payload, "ERR wrong number of arguments for 'decrby' command");
      return r;
    }
    std::int64_t by = 0;
    if (!ParseInt(args[2], &by)) {
      resp::AppendError(r.payload, "ERR value is not an integer or out of range");
      return r;
    }
    return do_incr(args[1], -by);
  }

  resp::AppendError(r.payload, "ERR unknown command");
  return r;
}

}  // namespace

class DkvServer::SubReactor {
 public:
  SubReactor(std::size_t index, dkv::DB* db, const ServerConfig* cfg, ThreadPool* workers)
      : index_(index), db_(db), cfg_(cfg), workers_(workers) {}
  SubReactor(const SubReactor&) = delete;
  SubReactor& operator=(const SubReactor&) = delete;

  void Start() { thread_ = std::thread([this] { Loop(); }); }

  void Stop() {
    stopping_.store(true, std::memory_order_relaxed);
    Notify();
    if (thread_.joinable()) thread_.join();
  }

  void EnqueueNewConn(int fd) {
    {
      std::lock_guard lk(mu_);
      pending_new_.push_back(fd);
    }
    Notify();
  }

  void EnqueueResponse(ResponseMsg msg) {
    {
      std::lock_guard lk(mu_);
      pending_resp_.push_back(std::move(msg));
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
    std::vector<int> new_fds;
    std::vector<ResponseMsg> resps;
    {
      std::lock_guard lk(mu_);
      if (!pending_new_.empty()) new_fds.swap(pending_new_);
      if (!pending_resp_.empty()) resps.swap(pending_resp_);
    }
    for (int fd : new_fds) AddConn(fd);
    for (auto& msg : resps) ApplyResponse(msg);
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
    std::vector<std::string> cmd = std::move(c.queue.front());
    c.queue.pop_front();
    ConnToken token = c.token;
    dkv::DB* db = db_;
    const ServerConfig* cfg = cfg_;
    SubReactor* self = this;

    workers_->Submit([self, token, db, cfg, cmd = std::move(cmd)]() mutable {
      CommandResult cr = HandleCommand(cmd, db, cfg);
      ResponseMsg msg;
      msg.token = token;
      msg.payload = std::move(cr.payload);
      msg.close_after = cr.close_after;
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

  std::mutex mu_;
  std::vector<int> pending_new_;
  std::vector<ResponseMsg> pending_resp_;

  std::unordered_map<int, Connection> conns_;
  std::uint64_t next_conn_id_{0};
};

class DkvServer::Acceptor {
 public:
  Acceptor(std::string bind, int port, std::vector<SubReactor*> subs) : bind_(std::move(bind)), port_(port), subs_(std::move(subs)) {}

  void Start() { thread_ = std::thread([this] { Loop(); }); }

  void Stop() {
    stopping_.store(true, std::memory_order_relaxed);
    if (thread_.joinable()) thread_.join();
  }

 private:
  void Loop() {
    try {
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
      if (::bind(listen_fd_.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) ThrowSys("bind");
      if (::listen(listen_fd_.get(), SOMAXCONN) < 0) ThrowSys("listen");

      epoll_fd_.reset(::epoll_create1(EPOLL_CLOEXEC));
      if (!epoll_fd_.valid()) ThrowSys("epoll_create1");
      epoll_event ev{};
      ev.events = EPOLLIN;
      ev.data.fd = listen_fd_.get();
      if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, listen_fd_.get(), &ev) < 0) ThrowSys("epoll_ctl(ADD listen)");

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

DkvServer::DkvServer(ServerConfig cfg) : cfg_(std::move(cfg)) {}

DkvServer::~DkvServer() { Stop(); }

void DkvServer::Start() {
  if (started_.exchange(true)) return;

  dkv::Options opt = cfg_.dkv_options;
  std::unique_ptr<dkv::DB> db;
  dkv::Status s = dkv::DB::Open(opt, db);
  if (!s.ok()) throw std::runtime_error("dkv::DB::Open failed: " + s.ToString());
  db_ = std::move(db);

  std::size_t sub_n = cfg_.subreactors;
  if (sub_n == 0) sub_n = std::max<std::size_t>(1, std::thread::hardware_concurrency());
  std::size_t worker_n = cfg_.workers;
  if (worker_n == 0) worker_n = std::max<std::size_t>(1, std::thread::hardware_concurrency());
  cfg_.subreactors = sub_n;
  cfg_.workers = worker_n;

  workers_ = std::make_unique<ThreadPool>(worker_n);
  subreactors_.reserve(sub_n);
  std::vector<SubReactor*> subs_raw;
  subs_raw.reserve(sub_n);
  for (std::size_t i = 0; i < sub_n; ++i) {
    subreactors_.push_back(std::make_unique<SubReactor>(i, db_.get(), &cfg_, workers_.get()));
    subs_raw.push_back(subreactors_.back().get());
  }
  for (auto& sr : subreactors_) sr->Start();

  acceptor_ = std::make_unique<Acceptor>(cfg_.bind, cfg_.port, std::move(subs_raw));
  acceptor_->Start();

  std::cout << "dkv-server listening on " << cfg_.bind << ":" << cfg_.port << " (subreactors=" << sub_n
            << ", workers=" << worker_n << ", data_dir=" << cfg_.dkv_options.data_dir.string() << ")\n";
}

void DkvServer::Stop() {
  if (!started_.exchange(false)) return;
  if (acceptor_) acceptor_->Stop();
  for (auto& sr : subreactors_) {
    if (sr) sr->Stop();
  }
  if (workers_) workers_->Stop();
}

}  // namespace dkv_server
