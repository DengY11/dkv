#include "sstable.h"

#include <algorithm>
#include <utility>

namespace dkv {

namespace {

struct ParsedFooter {
  std::uint64_t index_start{0};
  std::uint64_t bloom_start{0};
  std::uint64_t max_seq{0};
  std::uint32_t block_count{0};
};

template <typename Entry>
Status WriteImpl(const std::filesystem::path& path, const std::vector<Entry>& entries, std::size_t block_size,
                 std::size_t bloom_bits_per_key) {
  if (entries.empty()) return Status::InvalidArgument("no entries to write");
  std::error_code ec;
  std::filesystem::create_directories(path.parent_path(), ec);

  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    return Status::IOError("failed to create sstable: " + path.string());
  }

  struct LocalIdx {
    std::string key;
    std::uint64_t offset{0};
  };
  std::vector<LocalIdx> blocks;
  blocks.reserve(entries.size());
  std::uint64_t max_seq = 0;
  std::vector<std::string_view> bloom_keys;
  bloom_keys.reserve(entries.size());

  std::uint64_t block_start = static_cast<std::uint64_t>(out.tellp());
  std::string block_first_key(entries.front().key);
  blocks.push_back(LocalIdx{block_first_key, block_start});
  std::uint64_t current_block_bytes = 0;

  for (std::size_t i = 0; i < entries.size(); ++i) {
    const auto& e = entries[i];
    const std::string_view key_view = e.key;
    const std::string_view value_view = e.value;
    bloom_keys.push_back(key_view);
    max_seq = std::max(max_seq, e.seq);

    const auto before = static_cast<std::uint64_t>(out.tellp());
    WriteU8(out, static_cast<std::uint8_t>(e.deleted ? 1 : 0));
    WriteU64(out, e.seq);
    WriteU32(out, static_cast<std::uint32_t>(key_view.size()));
    WriteU32(out, static_cast<std::uint32_t>(value_view.size()));
    out.write(key_view.data(), static_cast<std::streamsize>(key_view.size()));
    out.write(value_view.data(), static_cast<std::streamsize>(value_view.size()));
    current_block_bytes += static_cast<std::uint64_t>(out.tellp()) - before;

    if (current_block_bytes >= block_size && i + 1 < entries.size()) {
      block_start = static_cast<std::uint64_t>(out.tellp());
      block_first_key.assign(entries[i + 1].key);
      blocks.push_back(LocalIdx{block_first_key, block_start});
      current_block_bytes = 0;
    }
  }

  BloomFilter bloom = BloomFilter::Build(bloom_keys, static_cast<std::uint32_t>(bloom_bits_per_key));
  const auto bloom_start = static_cast<std::uint64_t>(out.tellp());
  WriteU32(out, static_cast<std::uint32_t>(bloom.bits_per_key()));
  WriteU32(out, static_cast<std::uint32_t>(bloom.data().size()));
  out.write(reinterpret_cast<const char*>(bloom.data().data()),
            static_cast<std::streamsize>(bloom.data().size()));

  const auto index_start = static_cast<std::uint64_t>(out.tellp());
  for (const auto& idx : blocks) {
    WriteU32(out, static_cast<std::uint32_t>(idx.key.size()));
    out.write(idx.key.data(), static_cast<std::streamsize>(idx.key.size()));
    WriteU64(out, idx.offset);
  }

  WriteU64(out, index_start);
  WriteU64(out, bloom_start);
  WriteU64(out, max_seq);
  WriteU32(out, static_cast<std::uint32_t>(blocks.size()));
  WriteU32(out, kSSTableMagic);
  out.flush();
  if (!out) return Status::IOError("failed to finish sstable: " + path.string());
  return Status::OK();
}

bool ReadFooter(std::ifstream& in, std::uint64_t file_size, ParsedFooter& footer) {
  if (file_size < kSSTableFooterSize) return false;
  in.seekg(static_cast<std::streamoff>(file_size - kSSTableFooterSize));
  std::uint32_t magic = 0;
  if (!ReadU64(in, footer.index_start) || !ReadU64(in, footer.bloom_start) || !ReadU64(in, footer.max_seq) ||
      !ReadU32(in, footer.block_count) || !ReadU32(in, magic)) {
    return false;
  }
  return magic == kSSTableMagic;
}

}  // namespace

SSTable::SSTable(std::filesystem::path path, std::vector<BlockIndexEntry> index, BloomFilter bloom,
                 std::string min_key, std::string max_key, std::uint64_t max_seq, std::uint64_t file_size,
                 std::uint64_t bloom_start)
    : path_(std::move(path)),
      blocks_(std::move(index)),
      bloom_(std::move(bloom)),
      min_key_(std::move(min_key)),
      max_key_(std::move(max_key)),
      max_seq_(max_seq),
      file_size_(file_size),
      bloom_start_(bloom_start),
      file_(path_, std::ios::binary) {}

Status SSTable::Write(const std::filesystem::path& path, const std::vector<MemEntry>& entries,
                     std::size_t block_size, std::size_t bloom_bits_per_key) {
  return WriteImpl(path, entries, block_size, bloom_bits_per_key);
}

Status SSTable::Write(const std::filesystem::path& path, const std::vector<MemEntryView>& entries,
                     std::size_t block_size, std::size_t bloom_bits_per_key) {
  return WriteImpl(path, entries, block_size, bloom_bits_per_key);
}

Status SSTable::Open(const std::filesystem::path& path, const std::shared_ptr<BlockCache>& cache,
                    std::shared_ptr<SSTable>& out) {
  auto size_opt = FileSize(path);
  if (!size_opt) return Status::IOError("unable to stat sstable: " + path.string());

  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) return Status::IOError("failed to open sstable: " + path.string());

  ParsedFooter footer;
  if (!ReadFooter(in, *size_opt, footer)) {
    return Status::Corruption("failed to read sstable footer: " + path.string());
  }
  if (footer.index_start > *size_opt || footer.bloom_start > *size_opt) {
    return Status::Corruption("sstable index/bloom out of range: " + path.string());
  }

  in.seekg(static_cast<std::streamoff>(footer.bloom_start));
  std::uint32_t bloom_bits = 0;
  std::uint32_t bloom_bytes = 0;
  if (!ReadU32(in, bloom_bits) || !ReadU32(in, bloom_bytes)) {
    return Status::Corruption("bad bloom header: " + path.string());
  }
  std::vector<std::uint8_t> bloom_data(bloom_bytes, 0);
  if (!in.read(reinterpret_cast<char*>(bloom_data.data()), static_cast<std::streamsize>(bloom_bytes))) {
    return Status::Corruption("bad bloom data: " + path.string());
  }
  BloomFilter bloom;
  bloom.SetData(std::move(bloom_data), bloom_bits, std::max<std::uint32_t>(1, static_cast<std::uint32_t>(bloom_bits * 0.69)));

  in.seekg(static_cast<std::streamoff>(footer.index_start));
  std::vector<BlockIndexEntry> index;
  index.reserve(footer.block_count);
  for (std::uint32_t i = 0; i < footer.block_count; ++i) {
    std::uint32_t key_size = 0;
    std::uint64_t offset = 0;
    if (!ReadU32(in, key_size)) return Status::Corruption("bad index in sstable: " + path.string());
    std::string key(key_size, '\0');
    if (!in.read(key.data(), static_cast<std::streamsize>(key_size))) {
      return Status::Corruption("bad index key in sstable: " + path.string());
    }
    if (!ReadU64(in, offset)) return Status::Corruption("bad index offset in sstable: " + path.string());
    index.push_back(BlockIndexEntry{std::move(key), offset});
  }

  if (index.empty()) return Status::Corruption("empty index in sstable: " + path.string());

  // Derive max_key by reading the last block's last entry.
  in.seekg(static_cast<std::streamoff>(index.back().offset));
  std::string max_key;
  while (true) {
    std::uint8_t type = 0;
    std::uint64_t seq = 0;
    std::uint32_t key_size = 0, value_size = 0;
    const auto pos = static_cast<std::uint64_t>(in.tellg());
    if (pos >= footer.bloom_start) break;
    if (!ReadU8(in, type) || !ReadU64(in, seq) || !ReadU32(in, key_size) || !ReadU32(in, value_size)) break;
    std::string key(key_size, '\0');
    if (!in.read(key.data(), static_cast<std::streamsize>(key_size))) break;
    in.seekg(static_cast<std::streamoff>(value_size), std::ios::cur);
    max_key = key;
  }

  auto sstable = std::shared_ptr<SSTable>(
      new SSTable(path, std::move(index), std::move(bloom), index.front().key, max_key, footer.max_seq,
                  *size_opt, footer.bloom_start));
  sstable->cache_ = cache;
  if (!sstable->file_.is_open()) return Status::IOError("failed to open sstable reader: " + path.string());
  out = std::move(sstable);
  return Status::OK();
}

bool SSTable::Get(std::string_view key, MemEntry& entry) const {
  if (!bloom_.MayContain(key)) return false;
  // Find block whose first key is <= target.
  auto it = std::upper_bound(
      blocks_.begin(), blocks_.end(), key,
      [](std::string_view k, const BlockIndexEntry& b) { return k < b.key; });
  if (it == blocks_.begin()) return false;
  --it;
  std::uint64_t start = it->offset;
  std::uint64_t end = (it + 1 == blocks_.end()) ? bloom_start_ : (it + 1)->offset;
  return ReadEntryRange(start, end, key, entry);
}

Status SSTable::LoadAll(std::vector<MemEntry>& out) const {
  std::lock_guard lock(io_mu_);
  if (!file_.is_open()) return Status::IOError("sstable file not open");
  file_.clear();
  file_.seekg(0);
  while (static_cast<std::uint64_t>(file_.tellg()) < bloom_start_) {
    std::uint8_t type = 0;
    std::uint64_t seq = 0;
    std::uint32_t key_size = 0;
    std::uint32_t value_size = 0;
    if (!ReadU8(file_, type) || !ReadU64(file_, seq) || !ReadU32(file_, key_size) ||
        !ReadU32(file_, value_size)) {
      break;
    }
    std::string key(key_size, '\0');
    std::string value(value_size, '\0');
    if (!file_.read(key.data(), static_cast<std::streamsize>(key_size))) {
      return Status::Corruption("bad entry key in sstable: " + path_.string());
    }
    if (!file_.read(value.data(), static_cast<std::streamsize>(value_size))) {
      return Status::Corruption("bad entry value in sstable: " + path_.string());
    }
    MemEntry e;
    e.key = std::move(key);
    e.value = std::move(value);
    e.seq = seq;
    e.deleted = type != 0;
    out.push_back(std::move(e));
  }
  return Status::OK();
}

Status SSTable::Scan(std::string_view from, std::size_t limit,
                     std::vector<std::pair<std::string, std::string>>& out) const {
  if (limit == 0) return Status::OK();
  auto it = std::upper_bound(
      blocks_.begin(), blocks_.end(), from,
      [](std::string_view key, const BlockIndexEntry& e) { return key < e.key; });
  if (it != blocks_.begin()) --it;
  std::size_t added = 0;
  for (; it != blocks_.end() && added < limit; ++it) {
    std::uint64_t start = it->offset;
    std::uint64_t end = (it + 1 == blocks_.end()) ? bloom_start_ : (it + 1)->offset;
    if (!ReadBlockRange(start, end, out, limit - added)) {
      return Status::Corruption("failed scan read: " + path_.string());
    }
    added = out.size();
  }
  return Status::OK();
}

bool SSTable::ReadEntryRange(std::uint64_t start, std::uint64_t end, std::string_view key,
                             MemEntry& entry) const {
  if (cache_) {
    std::vector<MemEntry> cached;
    if (cache_->Get(path_.string(), start, cached)) {
      auto it = std::find_if(cached.begin(), cached.end(), [&](const MemEntry& e) { return e.key == key; });
      if (it != cached.end()) {
        entry = *it;
        return true;
      }
    }
  }
  std::vector<MemEntry> block;
  if (!ReadBlock(start, end, block)) return false;
  if (cache_) cache_->Put(path_.string(), start, block);
  auto it = std::find_if(block.begin(), block.end(), [&](const MemEntry& e) { return e.key == key; });
  if (it == block.end()) return false;
  entry = *it;
  return true;
}

bool SSTable::ReadBlockRange(std::uint64_t start, std::uint64_t end,
                             std::vector<std::pair<std::string, std::string>>& out,
                             std::size_t limit) const {
  if (cache_) {
    std::vector<MemEntry> cached;
    if (cache_->Get(path_.string(), start, cached)) {
      for (const auto& e : cached) {
        if (!e.deleted && out.size() < limit) out.emplace_back(e.key, e.value);
      }
      return true;
    }
  }
  std::vector<MemEntry> block;
  if (!ReadBlock(start, end, block)) return false;
  if (cache_) cache_->Put(path_.string(), start, block);
  for (const auto& e : block) {
    if (!e.deleted && out.size() < limit) out.emplace_back(e.key, e.value);
  }
  return true;
}

bool SSTable::ReadBlock(std::uint64_t start, std::uint64_t end, std::vector<MemEntry>& out) const {
  std::lock_guard lock(io_mu_);
  if (!file_.is_open()) return false;
  file_.clear();
  file_.seekg(static_cast<std::streamoff>(start));
  while (static_cast<std::uint64_t>(file_.tellg()) < end) {
    std::uint8_t type = 0;
    std::uint64_t seq = 0;
    std::uint32_t key_size = 0;
    std::uint32_t value_size = 0;
    if (!ReadU8(file_, type) || !ReadU64(file_, seq) || !ReadU32(file_, key_size) ||
        !ReadU32(file_, value_size)) {
      break;
    }
    std::string key(key_size, '\0');
    if (!file_.read(key.data(), static_cast<std::streamsize>(key_size))) return false;
    std::string value(value_size, '\0');
    if (!file_.read(value.data(), static_cast<std::streamsize>(value_size))) return false;
    MemEntry e;
    e.key = std::move(key);
    e.value = std::move(value);
    e.seq = seq;
    e.deleted = type != 0;
    out.push_back(std::move(e));
  }
  return true;
}

}  // namespace dkv
