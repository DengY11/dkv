#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <dkv/filename.h>
#include <dkv/options.h>
#include <dkv/status.h>

#include "sstable.h"
#include "util.h"

namespace fs = std::filesystem;

namespace {

struct WalDumpOptions {
  bool ignore_crc{false};  // don't stop on CRC mismatch
  std::size_t limit{0};    // 0 = unlimited
};

struct SstDumpOptions {
  bool verify_crc{false};
  bool blocks{false};
  std::size_t limit{0};  // 0 = unlimited
};

[[nodiscard]] std::string EscapeBytes(std::string_view s, std::size_t max_bytes = 0) {
  const std::size_t n = (max_bytes != 0 && s.size() > max_bytes) ? max_bytes : s.size();
  std::string out;
  out.reserve(n + 16);
  auto hex = [](unsigned char v) -> char { return v < 10 ? static_cast<char>('0' + v) : static_cast<char>('a' + v - 10); };
  for (std::size_t i = 0; i < n; ++i) {
    unsigned char c = static_cast<unsigned char>(s[i]);
    switch (c) {
      case '\\':
        out += "\\\\";
        break;
      case '"':
        out += "\\\"";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        if (std::isprint(c)) {
          out.push_back(static_cast<char>(c));
        } else {
          out += "\\x";
          out.push_back(hex((c >> 4) & 0xF));
          out.push_back(hex(c & 0xF));
        }
        break;
    }
  }
  if (n != s.size()) out += "...";
  return out;
}

[[nodiscard]] std::string Quote(std::string_view s) {
  std::string out;
  out.reserve(s.size() + 2);
  out.push_back('"');
  out.append(EscapeBytes(s));
  out.push_back('"');
  return out;
}

void PrintUsage(std::ostream& os, const char* prog) {
  os << "Usage:\n";
  os << "  " << prog << " wal <wal_file_or_dir> [--ignore-crc] [--limit N] [--out FILE]\n";
  os << "  " << prog << " sst <sst_file_or_dir> [--verify-crc] [--blocks] [--limit N] [--out FILE]\n";
  os << "\nNotes:\n";
  os << "  - For 'wal', if input is a directory, this tool dumps wal.log + wal-*.log in sequence order.\n";
  os << "  - For 'sst', if input is a directory, this tool dumps all *.sst files under it.\n";
}

[[nodiscard]] bool StartsWith(std::string_view s, std::string_view prefix) { return s.rfind(prefix, 0) == 0; }

[[nodiscard]] std::optional<std::size_t> ParseSizeT(std::string_view s) {
  if (s.empty()) return std::nullopt;
  std::size_t v = 0;
  for (char c : s) {
    if (c < '0' || c > '9') return std::nullopt;
    if (v > (std::numeric_limits<std::size_t>::max() / 10)) return std::nullopt;
    v = v * 10 + static_cast<std::size_t>(c - '0');
  }
  return v;
}

struct Out {
  std::ostream* os{&std::cout};
  std::ofstream file;
};

Out OpenOut(const std::string& path, std::ostream& err) {
  Out out;
  if (path.empty() || path == "-") return out;
  out.file.open(path, std::ios::binary | std::ios::trunc);
  if (!out.file.is_open()) {
    err << "error: failed to open output file: " << path << "\n";
    return out;
  }
  out.os = &out.file;
  return out;
}

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

struct WalSegment {
  std::uint64_t order{0};
  fs::path path;
};

std::vector<WalSegment> CollectWalSegments(const fs::path& input) {
  std::vector<WalSegment> segs;
  std::error_code ec;
  if (fs::is_regular_file(input, ec)) {
    segs.push_back(WalSegment{0, input});
    return segs;
  }
  if (!fs::is_directory(input, ec)) return segs;

  fs::path active = input / std::string(dkv::kWalActiveName);
  if (fs::exists(active, ec)) segs.push_back(WalSegment{std::numeric_limits<std::uint64_t>::max(), active});

  for (const auto& entry : fs::directory_iterator(input, ec)) {
    if (ec) break;
    if (!entry.is_regular_file()) continue;
    const fs::path p = entry.path();
    const auto name = p.filename().string();
    if (!StartsWith(name, dkv::kWalPrefix)) continue;
    if (p.extension() != dkv::kWalExtension) continue;

    const std::size_t prefix_len = dkv::kWalPrefix.size();
    const std::size_t ext_len = dkv::kWalExtension.size();
    if (name.size() <= prefix_len + ext_len) continue;

    const std::string num_str = name.substr(prefix_len, name.size() - prefix_len - ext_len);
    try {
      const auto seq = static_cast<std::uint64_t>(std::stoull(num_str));
      segs.push_back(WalSegment{seq, p});
    } catch (...) {
      continue;
    }
  }

  std::sort(segs.begin(), segs.end(), [](const WalSegment& a, const WalSegment& b) {
    if (a.order != b.order) return a.order < b.order;
    return a.path < b.path;
  });
  return segs;
}

int DumpWalFile(const fs::path& path, const WalDumpOptions& opt, std::ostream& os, std::ostream& err) {
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    err << "error: failed to open WAL: " << path << "\n";
    return 1;
  }

  os << "# WAL " << path.string() << "\n";
  std::size_t dumped = 0;
  std::size_t idx = 0;
  std::string crc_buf;
  for (;;) {
    if (opt.limit != 0 && dumped >= opt.limit) break;

    std::uint8_t type = 0;
    if (!dkv::ReadU8(in, type)) break;

    std::uint64_t seq = 0;
    std::uint32_t key_size = 0;
    std::uint32_t value_size = 0;
    if (!dkv::ReadU64(in, seq) || !dkv::ReadU32(in, key_size) || !dkv::ReadU32(in, value_size)) {
      err << "error: truncated WAL header at record " << idx << " in " << path << "\n";
      return 1;
    }

    std::string key(key_size, '\0');
    if (!in.read(key.data(), static_cast<std::streamsize>(key_size))) {
      err << "error: truncated WAL key at record " << idx << " in " << path << "\n";
      return 1;
    }

    std::string value;
    const bool is_put = (type == 0);
    const bool is_del = (type == 1);
    if (is_put) {
      value.assign(value_size, '\0');
      if (!in.read(value.data(), static_cast<std::streamsize>(value_size))) {
        err << "error: truncated WAL value at record " << idx << " in " << path << "\n";
        return 1;
      }
    } else if (is_del) {
      if (value_size != 0) {
        err << "error: corrupt WAL delete record (non-zero value_size) at record " << idx << " in " << path << "\n";
        return 1;
      }
    } else {
      err << "error: unknown WAL record type=" << static_cast<int>(type) << " at record " << idx << " in " << path
          << "\n";
      return 1;
    }

    std::uint32_t stored_crc = 0;
    if (!dkv::ReadU32(in, stored_crc)) {
      err << "error: truncated WAL crc32 at record " << idx << " in " << path << "\n";
      return 1;
    }

    // dkv's replay computes CRC32 over: [type:1][seq:8][klen:4][vlen:4][key][value]
    bool crc_checked = false;
    bool crc_ok = true;
    std::uint32_t computed_crc = 0;
    if (stored_crc != 0) {
      crc_buf.clear();
      crc_buf.reserve(1 + 8 + 4 + 4 + key.size() + value.size());
      AppendU8(crc_buf, type);
      AppendU64(crc_buf, seq);
      AppendU32(crc_buf, static_cast<std::uint32_t>(key.size()));
      AppendU32(crc_buf, static_cast<std::uint32_t>(value.size()));
      crc_buf.append(key);
      crc_buf.append(value);
      computed_crc = dkv::CRC32(crc_buf);
      crc_checked = true;
      crc_ok = (computed_crc == stored_crc);
    }

    const char* opname = is_put ? "PUT" : "DEL";
    os << "seq=" << seq << " op=" << opname << " klen=" << key.size() << " key=" << Quote(key);
    if (is_put) {
      os << " vlen=" << value.size() << " value=" << Quote(value);
    }
    if (crc_checked) {
      os << " crc=" << (crc_ok ? "OK" : "BAD") << " stored=" << stored_crc << " computed=" << computed_crc;
    } else {
      os << " crc=OFF stored=" << stored_crc;
    }
    os << "\n";

    if (crc_checked && !crc_ok && !opt.ignore_crc) {
      err << "error: CRC mismatch at record " << idx << " in " << path << "\n";
      return 1;
    }

    ++idx;
    ++dumped;
  }

  return 0;
}

std::vector<fs::path> CollectSstFiles(const fs::path& input) {
  std::vector<fs::path> files;
  std::error_code ec;
  if (fs::is_regular_file(input, ec)) {
    files.push_back(input);
    return files;
  }
  if (!fs::is_directory(input, ec)) return files;
  for (const auto& entry : fs::recursive_directory_iterator(input, ec)) {
    if (ec) break;
    if (!entry.is_regular_file()) continue;
    if (entry.path().extension() == ".sst") files.push_back(entry.path());
  }
  std::sort(files.begin(), files.end());
  return files;
}

int DumpSstFile(const fs::path& path, const SstDumpOptions& opt, std::ostream& os, std::ostream& err) {
  dkv::Options options;
  options.verify_sstable_crc = opt.verify_crc;
  auto options_ptr = std::make_shared<const dkv::Options>(options);

  std::shared_ptr<dkv::SSTable> table;
  std::atomic<std::uint64_t> crc_errors{0};
  std::atomic<std::uint64_t> read_errors{0};
  dkv::Status s = dkv::SSTable::Open(path, /*cache=*/{}, /*raw_cache=*/{}, /*bloom_cache=*/{}, /*pin_bloom=*/false,
                                    table, std::move(options_ptr), &crc_errors, &read_errors);
  if (!s.ok()) {
    err << "error: failed to open sstable: " << path << ": " << s.ToString() << "\n";
    return 1;
  }

  os << "# SST " << path.string() << "\n";
  os << "file_size=" << table->file_size() << " blocks=" << table->block_count()
     << " max_seq=" << table->max_sequence() << " min_key=" << Quote(table->min_key())
     << " max_key=" << Quote(table->max_key()) << "\n";

  std::size_t dumped = 0;
  if (opt.blocks) {
    for (std::size_t bi = 0; bi < table->block_count(); ++bi) {
      if (opt.limit != 0 && dumped >= opt.limit) break;
      std::vector<dkv::MemEntry> entries;
      dkv::Status bs = table->ReadBlockByIndex(bi, entries);
      if (!bs.ok()) {
        err << "error: failed to read block " << bi << " from " << path << ": " << bs.ToString() << "\n";
        return 1;
      }
      os << "## block=" << bi << " entries=" << entries.size() << "\n";
      for (const auto& e : entries) {
        if (opt.limit != 0 && dumped >= opt.limit) break;
        os << "seq=" << e.seq << " op=" << (e.deleted ? "DEL" : "PUT") << " klen=" << e.key.size()
           << " key=" << Quote(e.key);
        if (!e.deleted) os << " vlen=" << e.value.size() << " value=" << Quote(e.value);
        os << "\n";
        ++dumped;
      }
    }
  } else {
    auto it = table->NewIterator();
    dkv::MemEntry e;
    while (it.Next(e)) {
      if (opt.limit != 0 && dumped >= opt.limit) break;
      os << "seq=" << e.seq << " op=" << (e.deleted ? "DEL" : "PUT") << " klen=" << e.key.size()
         << " key=" << Quote(e.key);
      if (!e.deleted) os << " vlen=" << e.value.size() << " value=" << Quote(e.value);
      os << "\n";
      ++dumped;
    }
  }

  if (crc_errors.load() != 0 || read_errors.load() != 0) {
    os << "# read_errors=" << read_errors.load() << " crc_errors=" << crc_errors.load() << "\n";
  }
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 3) {
    PrintUsage(std::cerr, argv[0]);
    return 2;
  }

  std::string sub = argv[1];
  std::string input = argv[2];
  std::string out_path;

  WalDumpOptions wal_opt;
  SstDumpOptions sst_opt;

  for (int i = 3; i < argc; ++i) {
    std::string_view a(argv[i]);
    auto take_value = [&](std::string* dst) -> bool {
      if (i + 1 >= argc) return false;
      *dst = argv[++i];
      return true;
    };
    if (a == "--help" || a == "-h") {
      PrintUsage(std::cout, argv[0]);
      return 0;
    }
    if (a == "--out") {
      if (!take_value(&out_path)) {
        std::cerr << "error: --out requires a value\n";
        return 2;
      }
      continue;
    }
    if (a == "--limit") {
      std::string v;
      if (!take_value(&v)) {
        std::cerr << "error: --limit requires a value\n";
        return 2;
      }
      auto n = ParseSizeT(v);
      if (!n) {
        std::cerr << "error: invalid --limit: " << v << "\n";
        return 2;
      }
      wal_opt.limit = *n;
      sst_opt.limit = *n;
      continue;
    }
    if (a == "--ignore-crc") {
      wal_opt.ignore_crc = true;
      continue;
    }
    if (a == "--verify-crc") {
      sst_opt.verify_crc = true;
      continue;
    }
    if (a == "--blocks") {
      sst_opt.blocks = true;
      continue;
    }
    std::cerr << "error: unknown option: " << a << "\n";
    return 2;
  }

  Out out = OpenOut(out_path, std::cerr);
  if (out_path.size() > 0 && out.os == &std::cout && out_path != "-") return 2;

  if (sub == "wal") {
    auto segs = CollectWalSegments(input);
    if (segs.empty()) {
      std::cerr << "error: no WAL files found at: " << input << "\n";
      return 1;
    }
    for (const auto& seg : segs) {
      int rc = DumpWalFile(seg.path, wal_opt, *out.os, std::cerr);
      if (rc != 0) return rc;
    }
    return 0;
  }

  if (sub == "sst") {
    auto files = CollectSstFiles(input);
    if (files.empty()) {
      std::cerr << "error: no SST files found at: " << input << "\n";
      return 1;
    }
    for (const auto& p : files) {
      int rc = DumpSstFile(p, sst_opt, *out.os, std::cerr);
      if (rc != 0) return rc;
    }
    return 0;
  }

  std::cerr << "error: unknown subcommand: " << sub << "\n";
  PrintUsage(std::cerr, argv[0]);
  return 2;
}

