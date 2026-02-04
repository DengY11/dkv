// Example: RESP client usage against dkv-server.
// Build with DKV_BUILD_EXAMPLES=ON and run:
//   ./build/dkv-resp-client-ops 127.0.0.1 6379
#include <dkv/resp_client.h>

#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

void PrintTitle(std::string_view title) {
  std::cout << "\n== " << title << " ==\n";
}

void PrintKv(const std::vector<std::pair<std::string, std::string>>& kvs) {
  for (const auto& [k, v] : kvs) {
    std::cout << k << " = " << v << "\n";
  }
}

void PrintOptionalStrings(const std::vector<std::optional<std::string>>& vals) {
  for (std::size_t i = 0; i < vals.size(); ++i) {
    if (vals[i]) {
      std::cout << "[" << i << "] " << *vals[i] << "\n";
    } else {
      std::cout << "[" << i << "] (nil)\n";
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  // Connection target (default: localhost:6379).
  const char* host = argc > 1 ? argv[1] : "127.0.0.1";
  int port = argc > 2 ? std::atoi(argv[2]) : 6379;

  try {
    // Connect once and reuse a single client.
    dkv::RespClient client(host, static_cast<std::uint16_t>(port));

    // Basic commands: PING / ECHO / INFO / CLIENT SETINFO.
    auto basic = client.basic();
    PrintTitle("basic");
    std::cout << "PING: " << basic.PingText() << "\n";
    std::cout << "ECHO: " << basic.EchoText("hello") << "\n";
    basic.ClientSetInfoOk("lib-name", "dkv-example");

    // KV commands: SET/GET/MGET/MSET/EXISTS/DEL.
    auto kv = client.kv();
    PrintTitle("kv");
    kv.SetOk("k1", "v1");
    if (auto v = kv.GetString("k1")) {
      std::cout << "GET k1 = " << *v << "\n";
    }
    kv.MsetOk({{"a", "1"}, {"b", "2"}, {"c", "3"}});
    auto mvals = kv.MgetStrings({"a", "b", "missing"});
    PrintOptionalStrings(mvals);
    std::cout << "EXISTS a missing = " << kv.ExistsCount({"a", "missing"}) << "\n";
    std::cout << "DEL a b = " << kv.DelCount({"a", "b"}) << "\n";

    // Counter commands: INCR/DECR and *BY variants.
    auto counter = client.counter();
    PrintTitle("counter");
    std::cout << "INCR ctr = " << counter.IncrValue("ctr") << "\n";
    std::cout << "INCRBY ctr 5 = " << counter.IncrByValue("ctr", 5) << "\n";
    std::cout << "DECR ctr = " << counter.DecrValue("ctr") << "\n";
    std::cout << "DECRBY ctr 2 = " << counter.DecrByValue("ctr", 2) << "\n";

    // Config: CONFIG GET/RESETSTAT/REWRITE (GET returns key-value pairs).
    auto config = client.config();
    PrintTitle("config");
    auto cfg = config.GetMap("*");
    PrintKv(cfg);

    // Metrics: METRICS (key-value array).
    auto admin = client.admin();
    PrintTitle("metrics");
    auto metrics = admin.MetricsMap();
    PrintKv(metrics);

    // Iterator-style commands: SCAN/NEXT/ITERDEL.
    auto iter = client.iter();
    PrintTitle("iter");
    auto scan = iter.ScanState("k");
    std::cout << "SCAN id=" << scan.id << " valid=" << (scan.state.valid ? "1" : "0") << "\n";
    if (scan.state.key && scan.state.value) {
      std::cout << "key=" << *scan.state.key << " value=" << *scan.state.value << "\n";
    }
    if (scan.id != 0) {
      auto next = iter.NextState(scan.id);
      std::cout << "NEXT valid=" << (next.valid ? "1" : "0") << "\n";
      if (next.key && next.value) {
        std::cout << "key=" << *next.key << " value=" << *next.value << "\n";
      }
      iter.IterDelOk(scan.id);
    }

    // Custom / demo command.
    auto custom = client.custom();
    PrintTitle("custom");
    std::cout << custom.HiDylanText() << "\n";

    // Raw pipeline example: send multiple commands, then read replies in order.
    auto raw = client.raw();
    PrintTitle("raw");
    raw.Send({"PING"});
    raw.Send({"GET", "k1"});
    auto r1 = raw.Read();
    auto r2 = raw.Read();
    std::cout << "RAW PING = " << r1.str << "\n";
    if (r2.IsString()) {
      std::cout << "RAW GET k1 = " << r2.str << "\n";
    }

    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "error: " << ex.what() << "\n";
    return 1;
  }
}
