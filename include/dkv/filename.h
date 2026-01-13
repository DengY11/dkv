#pragma once

#include <string_view>

namespace dkv {

// Common filenames and prefixes used on disk.
constexpr std::string_view kWalActiveName = "wal.log";
constexpr std::string_view kWalPrefix = "wal-";
constexpr std::string_view kWalExtension = ".log";

constexpr std::string_view kManifestName = "MANIFEST";
constexpr std::string_view kManifestTmpPrefix = "MANIFEST.tmp.";

}  // namespace dkv

