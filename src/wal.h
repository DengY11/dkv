#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>

#include "dkv/status.h"

namespace dkv {

enum class WalRecordType : std::uint8_t { kPut = 0, kDelete = 1 };

class WAL {
 public:
  WAL(std::filesystem::path path, bool sync_by_default);
  ~WAL();

  Status Open();
  Status AppendPut(std::uint64_t seq, std::string_view key, std::string_view value, bool sync);
  Status AppendDelete(std::uint64_t seq, std::string_view key, bool sync);
  Status Replay(const std::function<void(std::uint64_t seq, bool deleted, std::string&& key,
                                         std::string&& value)>& apply);
  Status Reset();
  Status Sync(bool force_sync);  // flush + optional fsync depending on force_sync/sync_by_default_

 private:
  Status AppendRecord(WalRecordType type, std::uint64_t seq, std::string_view key,
                      std::string_view value, bool sync);

  std::filesystem::path path_;
  bool sync_by_default_{false};
  std::ofstream out_;
  std::mutex mu_;
  bool dirty_{false};
};

}  // namespace dkv
