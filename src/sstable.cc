#include "sstable.h"

#include <algorithm>
#include <string>
#include <string_view>
#include <utility>

namespace dkv {

namespace {

struct ParsedFooter {
  std::uint64_t index_start{0};
  std::uint64_t bloom_start{0};
  std::uint64_t max_seq{0};
  std::uint32_t block_count{0};
};

struct BlockHeader {
  std::uint32_t raw_size{0};
  std::uint32_t stored_size{0};
  std::uint8_t compression{0};  // 0 = none, 1 = snappy
};

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

// Stub Snappy hooks: if compression fails/unavailable, return false and we store raw.
bool SnappyCompress(std::string_view input, std::string& output) {
  (void)input;
  output.clear();
  return false;
}

bool SnappyUncompress(std::string_view input, std::string& output) {
  output.assign(input);
  return true;
}

template <typename Entry>
Status WriteImpl(const std::filesystem::path& path, const std::vector<Entry>& entries, std::size_t block_size,
                 std::size_t bloom_bits_per_key, CompressionType compression) {
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
    std::uint32_t size{0};
  };
  std::vector<LocalIdx> blocks;
  blocks.reserve(entries.size());
  std::uint64_t max_seq = 0;
  std::vector<std::string_view> bloom_keys;
  bloom_keys.reserve(entries.size());

  std::uint64_t current_block_bytes = 0;
  std::string block_first_key(entries.front().key);
  std::uint64_t block_start = static_cast<std::uint64_t>(out.tellp());
  std::string block_buf;
  block_buf.reserve(block_size * 2);
  auto flush_block = [&]() -> Status {
    if (block_buf.empty()) return Status::OK();
    // Compress block if enabled.
    std::string compressed;
    bool use_compression = false;
    if (compression == CompressionType::kSnappy) {
      if (SnappyCompress(block_buf, compressed)) {
        // require at least 12.5% savings.
        if (compressed.size() + compressed.size() / 8 < block_buf.size()) {
          use_compression = true;
        }
      }
    }
    const std::string_view payload = use_compression ? std::string_view(compressed) : std::string_view(block_buf);
    BlockHeader hdr;
    hdr.raw_size = static_cast<std::uint32_t>(block_buf.size());
    hdr.stored_size = static_cast<std::uint32_t>(payload.size());
    hdr.compression = use_compression ? 1 : 0;

    WriteU32(out, hdr.raw_size);
    WriteU32(out, hdr.stored_size);
    WriteU8(out, hdr.compression);
    out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
    if (!out) return Status::IOError("failed to write block");
    std::uint64_t end = static_cast<std::uint64_t>(out.tellp());
    blocks.push_back(LocalIdx{block_first_key, block_start,
                              static_cast<std::uint32_t>(end - block_start)});
    block_buf.clear();
    current_block_bytes = 0;
    block_start = end;
    return Status::OK();
  };

  for (std::size_t i = 0; i < entries.size(); ++i) {
    const auto& e = entries[i];
    const std::string_view key_view = e.key;
    const std::string_view value_view = e.value;
    bloom_keys.push_back(key_view);
    max_seq = std::max(max_seq, e.seq);

    const auto before = block_buf.size();
    block_buf.push_back(static_cast<char>(e.deleted ? 1 : 0));
    AppendU64(block_buf, e.seq);
    AppendU32(block_buf, static_cast<std::uint32_t>(key_view.size()));
    AppendU32(block_buf, static_cast<std::uint32_t>(value_view.size()));
    block_buf.append(key_view.data(), key_view.size());
    block_buf.append(value_view.data(), value_view.size());
    current_block_bytes += block_buf.size() - before;

    if (current_block_bytes >= block_size && i + 1 < entries.size()) {
      Status fs = flush_block();
      if (!fs.ok()) return fs;
      block_first_key.assign(entries[i + 1].key);
    }
  }
  Status fs = flush_block();
  if (!fs.ok()) return fs;

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
    WriteU32(out, idx.size);
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

SSTable::SSTable(std::filesystem::path path, std::vector<BlockIndexEntry> index, std::string min_key,
                 std::string max_key, std::uint64_t max_seq, std::uint64_t file_size, std::uint64_t bloom_start,
                 std::uint32_t bloom_bytes, std::uint32_t bloom_bits_per_key, bool pin_bloom,
                 std::shared_ptr<BlockCache> cache, std::shared_ptr<BloomCache> bloom_cache)
    : path_(std::move(path)),
      blocks_(std::move(index)),
      min_key_(std::move(min_key)),
      max_key_(std::move(max_key)),
      max_seq_(max_seq),
      file_size_(file_size),
      bloom_start_(bloom_start),
      cache_(std::move(cache)),
      bloom_cache_(std::move(bloom_cache)),
      bloom_bytes_(bloom_bytes),
      bloom_bits_per_key_(bloom_bits_per_key),
      pin_bloom_(pin_bloom),
      file_(path_, std::ios::binary) {}

Status SSTable::Write(const std::filesystem::path& path, const std::vector<MemEntry>& entries,
                     std::size_t block_size, std::size_t bloom_bits_per_key, CompressionType compression) {
  return WriteImpl(path, entries, block_size, bloom_bits_per_key, compression);
}

Status SSTable::Write(const std::filesystem::path& path, const std::vector<MemEntryView>& entries,
                     std::size_t block_size, std::size_t bloom_bits_per_key, CompressionType compression) {
  return WriteImpl(path, entries, block_size, bloom_bits_per_key, compression);
}

std::shared_ptr<BloomCache::Data> SSTable::LoadBloom() const {
  if (pin_bloom_ && pinned_bloom_) return pinned_bloom_;
  if (auto cached = bloom_ref_.lock()) return cached;

  std::shared_ptr<BloomCache::Data> data;
  if (bloom_cache_) {
    data = bloom_cache_->Get(path_.string());
  }

  if (!data) {
    // Load from disk
    std::vector<std::uint8_t> bits(bloom_bytes_, 0);
    {
      std::lock_guard lock(io_mu_);
      if (!file_.is_open()) return nullptr;
      file_.clear();
      file_.seekg(static_cast<std::streamoff>(bloom_start_));
      std::uint32_t bits_per_key = 0;
      std::uint32_t bytes = 0;
      if (!ReadU32(file_, bits_per_key) || !ReadU32(file_, bytes)) return nullptr;
      if (bytes != bloom_bytes_) return nullptr;
      if (!file_.read(reinterpret_cast<char*>(bits.data()), static_cast<std::streamsize>(bloom_bytes_))) return nullptr;
    }
    const std::uint32_t k = std::max<std::uint32_t>(1, static_cast<std::uint32_t>(bloom_bits_per_key_ * 0.69));
    if (bloom_cache_) {
      data = bloom_cache_->Put(path_.string(), std::move(bits), static_cast<std::uint32_t>(bloom_bits_per_key_), k);
    }
    if (!data) {
      BloomFilter bloom;
      bloom.SetData(std::move(bits), static_cast<std::uint32_t>(bloom_bits_per_key_), k);
      data = std::make_shared<BloomCache::Data>(BloomCache::Data{std::move(bloom), static_cast<std::size_t>(bloom_bytes_)});
    }
  }

  if (pin_bloom_) {
    pinned_bloom_ = data;
  } else {
    bloom_ref_ = data;
  }
  return data;
}

Status SSTable::Open(const std::filesystem::path& path, const std::shared_ptr<BlockCache>& cache,
                    const std::shared_ptr<BloomCache>& bloom_cache, bool pin_bloom,
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

  in.seekg(static_cast<std::streamoff>(footer.index_start));
  std::vector<BlockIndexEntry> index;
  index.reserve(footer.block_count);
  for (std::uint32_t i = 0; i < footer.block_count; ++i) {
    std::uint32_t key_size = 0;
    std::uint64_t offset = 0;
    std::uint32_t size = 0;
    if (!ReadU32(in, key_size)) return Status::Corruption("bad index in sstable: " + path.string());
    std::string key(key_size, '\0');
    if (!in.read(key.data(), static_cast<std::streamsize>(key_size))) {
      return Status::Corruption("bad index key in sstable: " + path.string());
    }
    if (!ReadU64(in, offset) || !ReadU32(in, size)) {
      return Status::Corruption("bad index offset/size in sstable: " + path.string());
    }
    index.push_back(BlockIndexEntry{std::move(key), offset, size});
  }

  if (index.empty()) return Status::Corruption("empty index in sstable: " + path.string());

  // Derive max_key by reading the last block's last entry.
  in.seekg(static_cast<std::streamoff>(index.back().offset));
  BlockHeader hdr;
  if (!ReadU32(in, hdr.raw_size) || !ReadU32(in, hdr.stored_size) || !ReadU8(in, hdr.compression)) {
    return Status::Corruption("failed to read block header: " + path.string());
  }
  if (index.back().size < sizeof(hdr.raw_size) + sizeof(hdr.stored_size) + sizeof(hdr.compression) + hdr.stored_size) {
    return Status::Corruption("block size too small: " + path.string());
  }
  std::string payload(hdr.stored_size, '\0');
  if (!in.read(payload.data(), static_cast<std::streamsize>(hdr.stored_size))) {
    return Status::Corruption("failed to read block payload: " + path.string());
  }
  std::string raw;
  std::string_view data_view;
  if (hdr.compression == 1) {
    if (!SnappyUncompress(payload, raw)) return Status::Corruption("failed to uncompress block");
    data_view = raw;
  } else {
    data_view = payload;
  }
  if (data_view.size() != hdr.raw_size) {
    return Status::Corruption("block raw size mismatch: " + path.string());
  }
  std::string max_key;
  std::size_t offset = 0;
  while (offset < data_view.size()) {
    if (offset + 1 + 8 + 4 + 4 > data_view.size()) break;
    offset += 1 + 8;  // type + seq
    auto read32 = [&](std::size_t pos) -> std::uint32_t {
      return static_cast<std::uint32_t>(static_cast<unsigned char>(data_view[pos])) |
             (static_cast<std::uint32_t>(static_cast<unsigned char>(data_view[pos + 1])) << 8) |
             (static_cast<std::uint32_t>(static_cast<unsigned char>(data_view[pos + 2])) << 16) |
             (static_cast<std::uint32_t>(static_cast<unsigned char>(data_view[pos + 3])) << 24);
    };
    const std::uint32_t key_size = read32(offset);
    offset += 4;
    const std::uint32_t value_size = read32(offset);
    offset += 4;
    if (offset + key_size + value_size > data_view.size()) break;
    max_key.assign(data_view.substr(offset, key_size));
    offset += key_size + value_size;
  }

  // Minimal bloom info (lazy load on demand)
  in.seekg(static_cast<std::streamoff>(footer.bloom_start));
  std::uint32_t bloom_bits = 0;
  std::uint32_t bloom_bytes = 0;
  if (!ReadU32(in, bloom_bits) || !ReadU32(in, bloom_bytes)) {
    return Status::Corruption("bad bloom header: " + path.string());
  }

  auto sstable = std::shared_ptr<SSTable>(
      new SSTable(path, std::move(index), index.front().key, max_key, footer.max_seq, *size_opt, footer.bloom_start,
                  bloom_bytes, bloom_bits, pin_bloom, cache, bloom_cache));
  if (!sstable->file_.is_open()) return Status::IOError("failed to open sstable reader: " + path.string());
  out = std::move(sstable);
  return Status::OK();
}

bool SSTable::Get(std::string_view key, MemEntry& entry) const {
  auto bloom_data = LoadBloom();
  if (!bloom_data) return false;
  if (!bloom_data->bloom.MayContain(key)) return false;
  // Find block whose first key is <= target.
  auto it = std::upper_bound(
      blocks_.begin(), blocks_.end(), key,
      [](std::string_view k, const BlockIndexEntry& b) { return k < b.key; });
  if (it == blocks_.begin()) return false;
  --it;
  std::uint64_t start = it->offset;
  std::uint64_t size = it->size;
  return ReadEntryRange(start, start + size, key, entry);
}

Status SSTable::LoadAll(std::vector<MemEntry>& out) const {
  for (const auto& b : blocks_) {
    std::vector<MemEntry> block;
    if (!ReadBlock(b.offset, b.size, block)) {
      return Status::Corruption("failed to read block: " + path_.string());
    }
    out.insert(out.end(), std::make_move_iterator(block.begin()), std::make_move_iterator(block.end()));
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
    std::uint64_t end = start + it->size;
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
  if (!ReadBlock(start, end - start, block)) return false;
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
  if (!ReadBlock(start, end - start, block)) return false;
  if (cache_) cache_->Put(path_.string(), start, block);
  for (const auto& e : block) {
    if (!e.deleted && out.size() < limit) out.emplace_back(e.key, e.value);
  }
  return true;
}

bool SSTable::ReadBlock(std::uint64_t start, std::uint64_t size, std::vector<MemEntry>& out) const {
  std::lock_guard lock(io_mu_);
  if (!file_.is_open()) return false;
  file_.clear();
  file_.seekg(static_cast<std::streamoff>(start));

  BlockHeader hdr;
  if (!ReadU32(file_, hdr.raw_size) || !ReadU32(file_, hdr.stored_size)) return false;
  if (!ReadU8(file_, hdr.compression)) return false;
  const std::uint64_t header_bytes = sizeof(hdr.raw_size) + sizeof(hdr.stored_size) + sizeof(hdr.compression);
  if (header_bytes + hdr.stored_size > size) return false;
  std::string payload(hdr.stored_size, '\0');
  if (!file_.read(payload.data(), static_cast<std::streamsize>(hdr.stored_size))) return false;

  std::string raw;
  std::string_view data_view;
  if (hdr.compression == 1) {
    if (!SnappyUncompress(payload, raw)) return false;
    data_view = raw;
  } else {
    data_view = payload;
  }
  if (data_view.size() != hdr.raw_size) return false;

  std::size_t offset = 0;
  while (offset < data_view.size()) {
    if (offset + 1 + 8 + 4 + 4 > data_view.size()) break;
    const std::uint8_t type = static_cast<std::uint8_t>(data_view[offset]);
    offset += 1;
    std::uint64_t seq = 0;
    for (int i = 0; i < 8; ++i) seq |= static_cast<std::uint64_t>(static_cast<unsigned char>(data_view[offset + i])) << (8 * i);
    offset += 8;
    auto read32 = [&](std::size_t pos) -> std::uint32_t {
      return static_cast<std::uint32_t>(static_cast<unsigned char>(data_view[pos])) |
             (static_cast<std::uint32_t>(static_cast<unsigned char>(data_view[pos + 1])) << 8) |
             (static_cast<std::uint32_t>(static_cast<unsigned char>(data_view[pos + 2])) << 16) |
             (static_cast<std::uint32_t>(static_cast<unsigned char>(data_view[pos + 3])) << 24);
    };
    const std::uint32_t key_size = read32(offset);
    offset += 4;
    const std::uint32_t value_size = read32(offset);
    offset += 4;
    if (offset + key_size + value_size > data_view.size()) break;
    std::string key(data_view.substr(offset, key_size));
    offset += key_size;
    std::string value(data_view.substr(offset, value_size));
    offset += value_size;
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
