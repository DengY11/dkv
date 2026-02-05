#include "server.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <utility>

#include "acceptor.h"
#include "log.h"
#include "subreactor.h"
#include "thread_pool.h"

namespace dkv_server {

DkvServer::DkvServer(ServerConfig cfg) : cfg_(std::move(cfg)) {}

DkvServer::~DkvServer() { Stop(); }

void DkvServer::Start() {
  if (started_.exchange(true)) return;

  try {
    dkv::Options opt = cfg_.dkv_options;
    std::unique_ptr<dkv::DB> db;
    dkv::Status s = dkv::DB::Open(opt, db);
    if (!s.ok()) throw std::runtime_error("dkv::DB::Open failed: " + s.ToString());
    db_ = std::move(db);

    const std::size_t hc = std::max<std::size_t>(1, std::thread::hardware_concurrency());
    std::size_t sub_n = cfg_.subreactors;
    if (sub_n == 0) sub_n = hc;
    std::size_t worker_n = cfg_.workers;
    if (worker_n == 0) worker_n = hc;

    if (sub_n > hc) sub_n = hc;
    if (worker_n > hc) worker_n = hc;
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

    acceptor_ = std::make_unique<Acceptor>(cfg_.bind, cfg_.port, std::move(subs_raw), cfg_.log_new_conn);
    acceptor_->Start();

    std::ostringstream oss;
    oss << "dkv-server listening on " << cfg_.bind << ":" << cfg_.port << " (subreactors=" << sub_n
        << ", workers=" << worker_n << ", data_dir=" << cfg_.dkv_options.data_dir.string() << ")";
    LogInfo(oss.str());
  } catch (...) {
    Stop();
    throw;
  }
}

void DkvServer::Stop() {
  if (!started_.exchange(false)) return;
  if (acceptor_) acceptor_->Stop();
  for (auto& sr : subreactors_) {
    if (sr) sr->Stop();
  }
  if (workers_) workers_->Stop();

  acceptor_.reset();
  subreactors_.clear();
  workers_.reset();
  db_.reset();
}

}  // namespace dkv_server
