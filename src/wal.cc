#include "wal.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <utility>

#include "util.h"

namespace dkv {

namespace {

inline void AppendU8(std::string& buf, std::uint8_t v) { buf.push_back(static_cast<char>(v)); }

inline void AppendU32(std::string& buf, std::uint32_t v) {
  buf.push_back(static_cast<char>(v & 0xFFu));
  buf.push_back(static_cast<char>((v >> 8u) & 0xFFu));
  buf.push_back(static_cast<char>((v >> 16u) & 0xFFu));
  buf.push_back(static_cast<char>((v >> 24u) & 0xFFu));
}

inline void AppendU64(std::string& buf, std::uint64_t v) {
  for (int i = 0; i < 8; ++i) buf.push_back(static_cast<char>((v >> (i * 8)) & 0xFFu));
}

}  // namespace

WAL::WAL(std::filesystem::path path, bool sync_by_default)
    : path_(std::move(path)), sync_by_default_(sync_by_default) {}

WAL::~WAL() { out_.close(); }

Status WAL::Open() {
  std::error_code ec;
  std::filesystem::create_directories(path_.parent_path(), ec);
  out_.open(path_, std::ios::binary | std::ios::app);
  if (!out_.is_open()) {
    return Status::IOError("failed to open WAL: " + path_.string());
  }
  return Status::OK();
}

Status WAL::AppendPut(std::uint64_t seq, std::string_view key, std::string_view value, bool sync) {
  return AppendRecord(WalRecordType::kPut, seq, key, value, sync);
}

Status WAL::AppendDelete(std::uint64_t seq, std::string_view key, bool sync) {
  return AppendRecord(WalRecordType::kDelete, seq, key, std::string_view(), sync);
}

Status WAL::Replay(const std::function<void(std::uint64_t, bool, std::string&&, std::string&&)>& apply) {
  std::ifstream in(path_, std::ios::binary);
  if (!in.is_open()) return Status::OK();

  while (true) {
    std::uint8_t type = 0;
    if (!ReadU8(in, type)) break;

    std::uint64_t seq = 0;
    std::uint32_t key_size = 0;
    std::uint32_t value_size = 0;
    if (!ReadU64(in, seq) || !ReadU32(in, key_size) || !ReadU32(in, value_size)) break;
    std::string key(key_size, '\0');
    if (!in.read(key.data(), static_cast<std::streamsize>(key_size))) break;
    std::string value;
    if (type == static_cast<std::uint8_t>(WalRecordType::kPut)) {
      value.assign(value_size, '\0');
      if (!in.read(value.data(), static_cast<std::streamsize>(value_size))) break;
    } else {
      if (value_size != 0) {
        // Corrupt entry; stop replay to avoid applying garbage.
        break;
      }
    }
    std::uint32_t stored_crc = 0;
    if (!ReadU32(in, stored_crc)) break;

    std::string buf;
    buf.reserve(1 + sizeof(std::uint64_t) + sizeof(std::uint32_t) * 2 + key.size() + value.size());
    AppendU8(buf, type);
    AppendU64(buf, seq);
    AppendU32(buf, static_cast<std::uint32_t>(key.size()));
    AppendU32(buf, static_cast<std::uint32_t>(value.size()));
    buf.append(key);
    buf.append(value);
    const auto computed = CRC32(buf);
    if (computed != stored_crc) {
      break;  // stop at first bad record
    }

    if (type == static_cast<std::uint8_t>(WalRecordType::kPut)) {
      apply(seq, false, std::move(key), std::move(value));
    } else {
      apply(seq, true, std::move(key), std::move(value));
    }
  }

  return Status::OK();
}

Status WAL::Reset() {
  std::lock_guard lock(mu_);
  out_.close();
  std::ofstream trunc(path_, std::ios::binary | std::ios::trunc);
  trunc.close();
  out_.open(path_, std::ios::binary | std::ios::app);
  if (!out_) return Status::IOError("failed to reset WAL: " + path_.string());
  dirty_ = false;
  auto s = SyncParentDir(path_);
  if (!s.ok()) return s;
  return Status::OK();
}

Status WAL::AppendRecord(WalRecordType type, std::uint64_t seq, std::string_view key,
                         std::string_view value, bool sync) {
  std::lock_guard lock(mu_);
  if (!out_.is_open()) return Status::IOError("wal not open");

  std::string buf;
  buf.reserve(1 + sizeof(std::uint64_t) + sizeof(std::uint32_t) * 2 + key.size() + value.size());
  AppendU8(buf, static_cast<std::uint8_t>(type));
  AppendU64(buf, seq);
  AppendU32(buf, static_cast<std::uint32_t>(key.size()));
  AppendU32(buf, static_cast<std::uint32_t>(value.size()));
  buf.append(key);
  buf.append(value);
  const auto crc = CRC32(buf);

  out_.write(buf.data(), static_cast<std::streamsize>(buf.size()));
  WriteU32(out_, crc);
  if (!out_) return Status::IOError("failed to append WAL");
  // on disk: [type:1][seq:8][klen:4][vlen:4][key][value][crc32]

  dirty_ = true;
  if (sync) {
    out_.flush();
    Status s = SyncFileToDisk(path_);
    if (!s.ok()) return s;
    dirty_ = false;
  }
  return Status::OK();
}

Status WAL::Sync(bool force_sync) {
  std::lock_guard lock(mu_);
  if (!dirty_) return Status::OK();
  out_.flush();
  if (force_sync || sync_by_default_) {
    auto s = SyncFileToDisk(path_);
    if (!s.ok()) return s;
  }
  dirty_ = false;
  return Status::OK();
}

}  // namespace dkv
