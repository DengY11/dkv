#include "sstable.h"

#include <algorithm>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>

#if DKV_USE_SNAPPY
#include <snappy.h>
#endif
#if DKV_USE_ZSTD
#include <zstd.h>
#endif
#if DKV_USE_LZ4
#include <lz4.h>
#endif

namespace dkv {

namespace {

struct ParsedFooter {
  std::uint64_t index_start{0};
  std::uint64_t bloom_start{0};
  std::uint64_t block_filter_start{0};
  std::uint64_t block_filter_index_start{0};
  std::uint64_t max_seq{0};
  std::uint32_t block_count{0};
  bool has_block_filters{false};
};

struct BlockHeader {
  std::uint32_t raw_size{0};
  std::uint32_t stored_size{0};
  std::uint8_t compression{0};  // 0 = none, 1 = snappy
  std::uint32_t crc{0};         // CRC32 of raw payload (uncompressed)
};

static bool DecompressData(std::uint8_t code, std::string_view input, std::string& output, std::size_t raw_size);

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

#if DKV_USE_SNAPPY
constexpr std::uint8_t kCompressionCode = 1;
#elif DKV_USE_ZSTD
constexpr std::uint8_t kCompressionCode = 2;
#elif DKV_USE_LZ4
constexpr std::uint8_t kCompressionCode = 3;
#else
  constexpr std::uint8_t kCompressionCode = 0;
  #endif

  bool CompressData(std::string_view input, std::string& output, std::uint8_t& code) {
  output.clear();
  code = 0;
#if DKV_USE_SNAPPY
  size_t max_sz = snappy::MaxCompressedLength(input.size());
  output.resize(max_sz);
  size_t out_sz = 0;
  snappy::RawCompress(input.data(), input.size(), output.data(), &out_sz);
  output.resize(out_sz);
  code = 1;
  return true;
#elif DKV_USE_ZSTD
  size_t bound = ZSTD_compressBound(input.size());
  output.resize(bound);
  size_t res = ZSTD_compress(output.data(), bound, input.data(), input.size(), ZSTD_CLEVEL_DEFAULT);
  if (ZSTD_isError(res)) return false;
  output.resize(res);
  code = 2;
  return true;
#elif DKV_USE_LZ4
  int bound = LZ4_compressBound(static_cast<int>(input.size()));
  output.resize(static_cast<std::size_t>(bound));
  int res = LZ4_compress_default(input.data(), output.data(), static_cast<int>(input.size()), bound);
  if (res <= 0) return false;
  output.resize(static_cast<std::size_t>(res));
  code = 3;
  return true;
#else
  (void)input;
  (void)output;
  (void)code;
  return false;
#endif
}

static bool DecompressData(std::uint8_t code, std::string_view input, std::string& output, std::size_t raw_size) {
  output.clear();
  if (code == 0) {
    output.assign(input);
    return true;
  } else if (code == 1) {
#if DKV_USE_SNAPPY
    size_t uncompressed = 0;
    if (!snappy::GetUncompressedLength(input.data(), input.size(), &uncompressed)) return false;
    output.resize(uncompressed);
    return snappy::RawUncompress(input.data(), input.size(), output.data());
#else
    (void)raw_size;
    return false;
#endif
  } else if (code == 2) {
#if DKV_USE_ZSTD
    output.resize(raw_size);
    size_t res = ZSTD_decompress(output.data(), raw_size, input.data(), input.size());
    if (ZSTD_isError(res)) return false;
    output.resize(res);
    return true;
#else
    return false;
#endif
  } else if (code == 3) {
#if DKV_USE_LZ4
    output.resize(raw_size);
    int res = LZ4_decompress_safe(input.data(), output.data(), static_cast<int>(input.size()),
                                  static_cast<int>(raw_size));
    if (res < 0) return false;
    output.resize(static_cast<std::size_t>(res));
    return true;
#else
    (void)raw_size;
    return false;
#endif
  }
  return false;
}

template <typename Entry>
Status WriteImpl(const std::filesystem::path& path, const std::vector<Entry>& entries, std::size_t block_size,
                 std::size_t bloom_bits_per_key, bool enable_compress, std::size_t block_bloom_bits_per_key) {
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
  std::vector<std::string_view> current_block_keys;
  current_block_keys.reserve(256);
  std::vector<std::vector<std::uint8_t>> block_bloom_bits;
  std::uint32_t block_filter_k = 0;
  auto flush_block = [&]() -> Status {
    if (block_buf.empty()) return Status::OK();
    if (block_bloom_bits_per_key > 0 && !current_block_keys.empty()) {
      auto bf = BloomFilter::Build(current_block_keys, static_cast<std::uint32_t>(block_bloom_bits_per_key));
      block_filter_k = bf.hash_count();
      block_bloom_bits.push_back(bf.data());
    } else if (block_bloom_bits_per_key > 0) {
      block_bloom_bits.push_back({});
    }
    current_block_keys.clear();
    // Compress block if enabled.
    std::string compressed;
    bool use_compression = enable_compress && kCompressionCode != 0;
    std::uint8_t comp_code = 0;
    if (use_compression) {
      if (CompressData(block_buf, compressed, comp_code)) {
        // require at least 12.5% savings
        if (!(compressed.size() + compressed.size() / 8 < block_buf.size())) {
          use_compression = false;
        }
      } else {
        use_compression = false;
      }
    }
    const std::string_view payload = use_compression ? std::string_view(compressed) : std::string_view(block_buf);
    BlockHeader hdr;
    hdr.raw_size = static_cast<std::uint32_t>(block_buf.size());
    hdr.stored_size = static_cast<std::uint32_t>(payload.size());
    hdr.compression = use_compression ? comp_code : 0;
    hdr.crc = CRC32(block_buf);

    WriteU32(out, hdr.raw_size);
    WriteU32(out, hdr.stored_size);
    WriteU8(out, hdr.compression);
    WriteU32(out, hdr.crc);
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
    if (block_bloom_bits_per_key > 0) current_block_keys.push_back(key_view);
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

  std::uint64_t block_filter_start = 0;
  std::uint64_t block_filter_index_start = 0;
  std::vector<std::uint64_t> block_filter_offsets;
  if (block_bloom_bits_per_key > 0 && !block_bloom_bits.empty()) {
    block_filter_start = static_cast<std::uint64_t>(out.tellp());
    WriteU32(out, static_cast<std::uint32_t>(block_bloom_bits_per_key));
    WriteU32(out, block_filter_k);
    block_filter_offsets.resize(block_bloom_bits.size(), 0);
    for (std::size_t i = 0; i < block_bloom_bits.size(); ++i) {
      if (block_bloom_bits[i].empty()) continue;
      block_filter_offsets[i] = static_cast<std::uint64_t>(out.tellp());
      WriteU32(out, static_cast<std::uint32_t>(block_bloom_bits[i].size()));
      out.write(reinterpret_cast<const char*>(block_bloom_bits[i].data()),
                static_cast<std::streamsize>(block_bloom_bits[i].size()));
    }
    block_filter_index_start = static_cast<std::uint64_t>(out.tellp());
    for (auto off : block_filter_offsets) {
      WriteU64(out, off);
    }
  }

  const auto index_start = static_cast<std::uint64_t>(out.tellp());
  for (const auto& idx : blocks) {
    WriteU32(out, static_cast<std::uint32_t>(idx.key.size()));
    out.write(idx.key.data(), static_cast<std::streamsize>(idx.key.size()));
    WriteU64(out, idx.offset);
    WriteU32(out, idx.size);
  }

  WriteU64(out, index_start);
  WriteU64(out, bloom_start);
  WriteU64(out, block_filter_start);
  WriteU64(out, block_filter_index_start);
  WriteU64(out, max_seq);
  WriteU32(out, static_cast<std::uint32_t>(blocks.size()));
  WriteU32(out, kSSTableMagic);
  out.flush();
  if (!out) return Status::IOError("failed to finish sstable: " + path.string());
  return Status::OK();
}

bool ReadFooter(std::ifstream& in, std::uint64_t file_size, ParsedFooter& footer) {
  if (file_size < kSSTableFooterSizeV1) return false;

  auto read_v2 = [&]() -> bool {
    if (file_size < kSSTableFooterSizeV2) return false;
    in.seekg(static_cast<std::streamoff>(file_size - kSSTableFooterSizeV2));
    std::uint32_t magic = 0;
    if (!ReadU64(in, footer.index_start) || !ReadU64(in, footer.bloom_start) ||
        !ReadU64(in, footer.block_filter_start) || !ReadU64(in, footer.block_filter_index_start) ||
        !ReadU64(in, footer.max_seq) || !ReadU32(in, footer.block_count) || !ReadU32(in, magic)) {
      return false;
    }
    if (magic != kSSTableMagic) return false;
    footer.has_block_filters = footer.block_filter_start != 0 && footer.block_filter_index_start != 0;
    return true;
  };

  if (read_v2()) return true;

  // Fallback to V1 (no block filters).
  in.seekg(static_cast<std::streamoff>(file_size - kSSTableFooterSizeV1));
  std::uint32_t magic = 0;
  if (!ReadU64(in, footer.index_start) || !ReadU64(in, footer.bloom_start) || !ReadU64(in, footer.max_seq) ||
      !ReadU32(in, footer.block_count) || !ReadU32(in, magic)) {
    return false;
  }
  if (magic != kSSTableMagic) return false;
  footer.block_filter_start = 0;
  footer.block_filter_index_start = 0;
  footer.has_block_filters = false;
  return true;
}

}  // namespace

SSTable::SSTable(std::filesystem::path path, std::vector<BlockIndexEntry> index, std::string min_key,
                 std::string max_key, std::uint64_t max_seq, std::uint64_t file_size, std::uint64_t bloom_start,
                 std::uint32_t bloom_bytes, std::uint32_t bloom_bits_per_key, bool pin_bloom,
                 std::shared_ptr<BlockCache> cache, std::shared_ptr<RawBlockCache> raw_cache,
                 std::shared_ptr<BloomCache> bloom_cache, std::shared_ptr<BloomCache> block_bloom_cache,
                 std::shared_ptr<const Options> options, std::atomic<std::uint64_t>* crc_errors,
                 std::atomic<std::uint64_t>* read_errors, std::vector<std::uint64_t> block_filter_offsets,
                 std::uint32_t block_filter_bits_per_key, std::uint32_t block_filter_hash_count,
                 std::uint64_t block_filter_start)
    : path_(std::move(path)),
      path_ref_(std::make_shared<std::string>(path_.string())),
      blocks_(std::move(index)),
      min_key_(std::move(min_key)),
      max_key_(std::move(max_key)),
      max_seq_(max_seq),
      file_size_(file_size),
      bloom_start_(bloom_start),
      cache_(std::move(cache)),
      raw_cache_(std::move(raw_cache)),
      bloom_cache_(std::move(bloom_cache)),
      block_bloom_cache_(std::move(block_bloom_cache)),
      options_(std::move(options)),
      crc_error_counter_(crc_errors),
      read_error_counter_(read_errors),
      bloom_bytes_(bloom_bytes),
      bloom_bits_per_key_(bloom_bits_per_key),
      block_filter_offsets_(std::move(block_filter_offsets)),
      block_filter_bits_per_key_(block_filter_bits_per_key),
      block_filter_hash_count_(block_filter_hash_count),
      block_filter_start_(block_filter_start),
      has_block_filters_(!block_filter_offsets_.empty() && block_filter_bits_per_key_ > 0 &&
                         block_filter_hash_count_ > 0),
      pin_bloom_(pin_bloom),
      file_(path_, std::ios::binary) {}

Status SSTable::Write(const std::filesystem::path& path, const std::vector<MemEntry>& entries,
                     std::size_t block_size, std::size_t bloom_bits_per_key, bool enable_compress,
                     std::size_t block_bloom_bits_per_key) {
  return WriteImpl(path, entries, block_size, bloom_bits_per_key, enable_compress, block_bloom_bits_per_key);
}

Status SSTable::Write(const std::filesystem::path& path, const std::vector<MemEntryView>& entries,
                     std::size_t block_size, std::size_t bloom_bits_per_key, bool enable_compress,
                     std::size_t block_bloom_bits_per_key) {
  return WriteImpl(path, entries, block_size, bloom_bits_per_key, enable_compress, block_bloom_bits_per_key);
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
      if (!file_.is_open())[[unlikely]] return nullptr;
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

  // Keep a strong ref so we don't reload from disk every Get when no shared cache is configured.
  if (pin_bloom_ || !bloom_cache_) {
    pinned_bloom_ = data;
  }
  bloom_ref_ = data;
  return data;
}

std::shared_ptr<BloomCache::Data> SSTable::LoadBlockBloom(std::size_t block_index) const {
  if (!has_block_filters_ || block_index >= block_filter_offsets_.size()) return nullptr;
  const auto offset = block_filter_offsets_[block_index];
  if (offset == 0) return nullptr;
  const std::string cache_key = *path_ref_ + "#blk#" + std::to_string(block_index);
  if (block_bloom_cache_) {
    if (auto cached = block_bloom_cache_->Get(cache_key)) return cached;
  }

  std::vector<std::uint8_t> bits;
  {
    std::lock_guard lk(io_mu_);
    if (!file_.is_open()) return nullptr;
    file_.clear();
    file_.seekg(static_cast<std::streamoff>(offset));
    std::uint32_t bytes = 0;
    if (!ReadU32(file_, bytes)) return nullptr;
    if (bytes == 0 || offset + sizeof(std::uint32_t) + bytes > file_size_) return nullptr;
    bits.resize(bytes);
    if (!file_.read(reinterpret_cast<char*>(bits.data()), static_cast<std::streamsize>(bytes))) {
      return nullptr;
    }
  }

  if (block_bloom_cache_) {
    return block_bloom_cache_->Put(cache_key, std::move(bits), block_filter_bits_per_key_, block_filter_hash_count_);
  }
  BloomFilter bloom;
  bloom.SetData(std::move(bits), block_filter_bits_per_key_, block_filter_hash_count_);
  return std::make_shared<BloomCache::Data>(BloomCache::Data{std::move(bloom), bloom.bit_size() / 8});
}

Status SSTable::Open(const std::filesystem::path& path, const std::shared_ptr<BlockCache>& cache,
                    const std::shared_ptr<RawBlockCache>& raw_cache, const std::shared_ptr<BloomCache>& bloom_cache,
                    const std::shared_ptr<BloomCache>& block_bloom_cache, bool pin_bloom,
                    std::shared_ptr<SSTable>& out, std::shared_ptr<const Options> options,
                    std::atomic<std::uint64_t>* crc_errors, std::atomic<std::uint64_t>* read_errors) {
  auto size_opt = FileSize(path);
  if (!size_opt)[[unlikely]] return Status::IOError("unable to stat sstable: " + path.string());

  std::ifstream in(path, std::ios::binary);
  if (!in.is_open())[[unlikely]] return Status::IOError("failed to open sstable: " + path.string());

  ParsedFooter footer;
  if (!ReadFooter(in, *size_opt, footer))[[unlikely]] {
    return Status::Corruption("failed to read sstable footer: " + path.string());
  }
  if (footer.index_start > *size_opt || footer.bloom_start > *size_opt)[[unlikely]] {
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

  if (index.empty())[[unlikely]] return Status::Corruption("empty index in sstable: " + path.string());

  // Derive max_key by reading the last block's last entry.
  in.seekg(static_cast<std::streamoff>(index.back().offset));
  BlockHeader hdr;
  if (!ReadU32(in, hdr.raw_size) || !ReadU32(in, hdr.stored_size) || !ReadU8(in, hdr.compression) ||
      !ReadU32(in, hdr.crc)) {
    return Status::Corruption("failed to read block header: " + path.string());
  }
  if (index.back().size <
      sizeof(hdr.raw_size) + sizeof(hdr.stored_size) + sizeof(hdr.compression) + sizeof(hdr.crc) + hdr.stored_size) {
    return Status::Corruption("block size too small: " + path.string());
  }
  std::string payload(hdr.stored_size, '\0');
  if (!in.read(payload.data(), static_cast<std::streamsize>(hdr.stored_size))) {
    return Status::Corruption("failed to read block payload: " + path.string());
  }
  std::string raw;
  std::string_view data_view;
  if (hdr.compression != 0) {
    if (!DecompressData(hdr.compression, payload, raw, hdr.raw_size)) {
      return Status::Corruption("failed to uncompress block");
    }
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

  std::vector<std::uint64_t> block_filter_offsets;
  std::uint32_t block_filter_bits_per_key = 0;
  std::uint32_t block_filter_k = 0;
  if (footer.has_block_filters) {
    if (footer.block_filter_start > *size_opt || footer.block_filter_index_start > *size_opt) {
      return Status::Corruption("block filter offsets out of range: " + path.string());
    }
    in.seekg(static_cast<std::streamoff>(footer.block_filter_start));
    if (!ReadU32(in, block_filter_bits_per_key) || !ReadU32(in, block_filter_k)) {
      return Status::Corruption("bad block filter header: " + path.string());
    }
    block_filter_offsets.resize(footer.block_count, 0);
    in.seekg(static_cast<std::streamoff>(footer.block_filter_index_start));
    for (std::uint32_t i = 0; i < footer.block_count; ++i) {
      if (!ReadU64(in, block_filter_offsets[i])) {
        return Status::Corruption("bad block filter index: " + path.string());
      }
    }
  }

  std::string min_key = index.front().key;
  auto sstable = std::shared_ptr<SSTable>(
      new SSTable(path, std::move(index), std::move(min_key), max_key, footer.max_seq, *size_opt, footer.bloom_start,
                  bloom_bytes, bloom_bits, pin_bloom, cache, raw_cache, bloom_cache, block_bloom_cache,
                  std::move(options), crc_errors, read_errors, std::move(block_filter_offsets),
                  block_filter_bits_per_key, block_filter_k, footer.block_filter_start));
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
  std::size_t block_index = static_cast<std::size_t>(it - blocks_.begin());
  if (has_block_filters_) {
    if (auto block_filter = LoadBlockBloom(block_index)) {
      if (!block_filter->bloom.MayContain(key)) return false;
    }
  }
  std::uint64_t start = it->offset;
  std::uint64_t size = it->size;
  return ReadEntryRange(start, start + size, key, entry);
}

Status SSTable::LoadAll(std::vector<MemEntry>& out) const {
  for (const auto& b : blocks_) {
    std::shared_ptr<const std::vector<MemEntry>> raw_block;
    if (!ReadBlock(b.offset, b.size, raw_block)) {
      return Status::Corruption("failed to read block: " + path_.string());
    }
    out.insert(out.end(), raw_block->begin(), raw_block->end());
  }
  return Status::OK();
}

Status SSTable::ReadBlockByIndex(std::size_t index, std::vector<MemEntry>& out) const {
  if (index >= blocks_.size()) return Status::InvalidArgument("block index out of range");
  out.clear();
  std::shared_ptr<const std::vector<MemEntry>> raw_block;
  if (!ReadBlock(blocks_[index].offset, blocks_[index].size, raw_block)) {
    return Status::Corruption("failed to read block: " + path_.string());
  }
  out.assign(raw_block->begin(), raw_block->end());
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
  std::shared_ptr<const std::vector<MemEntry>> block;
  if (!ReadBlock(start, end - start, block)) return false;
  for (const auto& e : *block) {
    if (e.key == key) {
      entry = e;
      return true;
    }
  }
  return false;
}

bool SSTable::ReadBlockRange(std::uint64_t start, std::uint64_t end,
                             std::vector<std::pair<std::string, std::string>>& out,
                             std::size_t limit) const {
  std::shared_ptr<const std::vector<MemEntry>> block;
  if (!ReadBlock(start, end - start, block)) return false;
  for (const auto& e : *block) {
    if (!e.deleted && out.size() < limit) out.emplace_back(e.key, e.value);
  }
  return true;
}

bool SSTable::ReadBlock(std::uint64_t start, std::uint64_t size,
                        std::shared_ptr<const std::vector<MemEntry>>& out) const {
  if (cache_) {
    if (cache_->Get(path_ref_, start, out)) {
      return true;
    }
  }
  std::shared_ptr<const std::string> raw_cached;
  if (raw_cache_) {
    if (raw_cache_->Get(path_ref_, start, raw_cached)) {
      // Decode from raw cache.
      auto record_read_error = [&]() {
        if (read_error_counter_) read_error_counter_->fetch_add(1, std::memory_order_relaxed);
      };
      const char* buf = raw_cached->data();
      const std::size_t buf_size = raw_cached->size();
      if (buf_size < sizeof(std::uint32_t) * 2 + sizeof(std::uint8_t) + sizeof(std::uint32_t))[[unlikely]] {
        record_read_error();
        return false;
      }
      std::uint32_t raw_size = 0;
      std::uint32_t stored_size = 0;
      std::uint8_t compression = 0;
      std::uint32_t hdr_crc = 0;
      std::memcpy(&raw_size, buf, sizeof(raw_size));
      std::memcpy(&stored_size, buf + sizeof(raw_size), sizeof(stored_size));
      std::memcpy(&compression, buf + sizeof(raw_size) + sizeof(stored_size), sizeof(compression));
      std::memcpy(&hdr_crc, buf + sizeof(raw_size) + sizeof(stored_size) + sizeof(compression), sizeof(hdr_crc));
      const std::size_t header_bytes =
          sizeof(raw_size) + sizeof(stored_size) + sizeof(compression) + sizeof(hdr_crc);
      if (header_bytes + stored_size > buf_size)[[unlikely]] {
        record_read_error();
        return false;
      }

      std::string raw;
      std::string_view data_view;
      if (compression != 0) {
        if (!DecompressData(compression, raw_cached->substr(header_bytes, stored_size), raw, raw_size)) {
          record_read_error();
          return false;
        }
        data_view = raw;
      } else {
        data_view = std::string_view(raw_cached->data() + header_bytes, raw_size);
      }
      if (data_view.size() != raw_size) {
        record_read_error();
        return false;
      }
      if (options_ && options_->verify_sstable_crc) {
        const auto crc = CRC32(data_view);
        if (crc != hdr_crc) {
          if (crc_error_counter_) crc_error_counter_->fetch_add(1, std::memory_order_relaxed);
          record_read_error();
          return false;
        }
      }

      auto vec = std::make_shared<std::vector<MemEntry>>();
      vec->reserve(data_view.size() / 16 + 1);
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
        vec->push_back(std::move(e));
      }
      if (cache_) cache_->Put(path_ref_, start, *vec);
      out = std::move(vec);
      return true;
    }
  }

  auto record_read_error = [&]() {
    if (read_error_counter_) read_error_counter_->fetch_add(1, std::memory_order_relaxed);
  };

  std::lock_guard lock(io_mu_);
  if (!file_.is_open()) {
    record_read_error();
    return false;
  }
  file_.clear();
  file_.seekg(static_cast<std::streamoff>(start));

  BlockHeader hdr;
  if (!ReadU32(file_, hdr.raw_size) || !ReadU32(file_, hdr.stored_size)) {
    record_read_error();
    return false;
  }
  if (!ReadU8(file_, hdr.compression)) {
    record_read_error();
    return false;
  }
  if (!ReadU32(file_, hdr.crc)) {
    record_read_error();
    return false;
  }
  const std::uint64_t header_bytes =
      sizeof(hdr.raw_size) + sizeof(hdr.stored_size) + sizeof(hdr.compression) + sizeof(hdr.crc);
  if (header_bytes + hdr.stored_size > size)[[unlikely]] {
    record_read_error();
    return false;
  }

  std::string payload(hdr.stored_size, '\0');
  if (!file_.read(payload.data(), static_cast<std::streamsize>(hdr.stored_size))) {
    record_read_error();
    return false;
  }

  std::string raw;
  std::string_view data_view;
  if (hdr.compression != 0) {
    if (!DecompressData(hdr.compression, payload, raw, hdr.raw_size)) {
      record_read_error();
      return false;
    }
    data_view = raw;
  } else {
    data_view = payload;
  }
  if (data_view.size() != hdr.raw_size) {
    record_read_error();
    return false;
  }
  if (options_ && options_->verify_sstable_crc) {
    const auto crc = CRC32(data_view);
    if (crc != hdr.crc) {
      if (crc_error_counter_) crc_error_counter_->fetch_add(1, std::memory_order_relaxed);
      record_read_error();
      return false;
    }
  }

  auto vec = std::make_shared<std::vector<MemEntry>>();
  vec->reserve(data_view.size() / 16 + 1);
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
    vec->push_back(std::move(e));
  }
  if (raw_cache_) {
    std::string raw_block;
    raw_block.resize(static_cast<std::size_t>(header_bytes + hdr.stored_size));
    std::memcpy(raw_block.data(), &hdr.raw_size, sizeof(hdr.raw_size));
    std::memcpy(raw_block.data() + sizeof(hdr.raw_size), &hdr.stored_size, sizeof(hdr.stored_size));
    std::memcpy(raw_block.data() + sizeof(hdr.raw_size) + sizeof(hdr.stored_size), &hdr.compression,
                sizeof(hdr.compression));
    std::memcpy(raw_block.data() + sizeof(hdr.raw_size) + sizeof(hdr.stored_size) + sizeof(hdr.compression),
                &hdr.crc, sizeof(hdr.crc));
    std::memcpy(raw_block.data() + header_bytes, payload.data(), hdr.stored_size);
    raw_cache_->Put(path_ref_, start, std::move(raw_block));
  }
  if (cache_) cache_->Put(path_ref_, start, *vec);
  out = std::move(vec);
  return true;
}

}  // namespace dkv
