#pragma once

#include <string>
#include <vector>

namespace dkv {
class DB;
}  // namespace dkv

namespace dkv_server {

struct ServerConfig;

struct CommandResult {
  std::string payload;
  bool close_after{false};
};

[[nodiscard]] CommandResult ExecuteCommand(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg);

}  // namespace dkv_server

