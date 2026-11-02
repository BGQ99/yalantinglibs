/*
 * Copyright (c) 2026, Alibaba Group Holding Limited;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "doctest.h"

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif
#include <cstdio>

using namespace async_simple::coro;
using namespace std::chrono_literals;
namespace profile = coro_io::urma_benchmark_profile;

namespace profile_wire {
struct slow_result { int value; };
std::size_t sp_get_needed_size(const slow_result& result) {
  return struct_pack::get_write_size(result.value);
}
template <typename Writer>
void sp_serialize_to(Writer& writer, const slow_result& result) {
  std::this_thread::sleep_for(20ms);
  struct_pack::write(writer, result.value);
}
template <typename Reader>
struct_pack::err_code sp_deserialize_to(Reader& reader, slow_result& result) {
  return struct_pack::read(reader, result.value);
}
constexpr std::string_view sp_set_type_name(slow_result*) {
  return "profile_slow_result";
}
}

namespace {
int profile_sync(int value) {
  std::this_thread::sleep_for(2ms);
  return value;
}
Lazy<int> profile_coro(int value) {
  co_await coro_io::sleep_for(2ms);
  co_return value;
}
void profile_callback(coro_rpc::context<int> ctx, int value) {
  std::thread([ctx, value]() mutable {
    std::this_thread::sleep_for(2ms);
    ctx.response_msg(value);
  }).detach();
}
std::mutex inline_trace_mutex;
std::shared_ptr<profile::request_trace> inline_trace;
void profile_inline_callback(coro_rpc::context<profile_wire::slow_result> ctx) {
  {
    std::lock_guard lock(inline_trace_mutex);
    inline_trace = ctx.get_context_info()->profile();
  }
  ctx.response_msg(profile_wire::slow_result{29});
}
void profile_void() {}
Lazy<void> profile_coro_void() { co_return; }
int profile_throw() { throw std::runtime_error("profile test"); }
struct profile_service {
  int sync() { return 17; }
  Lazy<int> coro() { co_return 19; }
  void sync_void(int) {}
  Lazy<void> coro_void(int) { co_return; }
};

using stage_counts = std::array<uint64_t, static_cast<std::size_t>(profile::stage::count)>;
stage_counts snapshot_counts() {
  stage_counts result{};
  std::lock_guard registry_lock(profile::registry_mutex());
  for (std::size_t s = 0; s < result.size(); ++s)
    result[s] = profile::merged_counters()[s];
  for (auto* thread : profile::registry()) {
    std::lock_guard snapshot_lock(thread->snapshot_mutex);
    for (std::size_t s = 0; s < result.size(); ++s)
      result[s] += thread->counters[s];
  }
  return result;
}

// Used only after all request work has stopped. Restore stderr before checking
// output so a failed assertion cannot hide the test runner's diagnostics.
class stderr_capture {
  struct close_file {
    void operator()(FILE* file) const { std::fclose(file); }
  };
  std::unique_ptr<FILE, close_file> file_{std::tmpfile()};
  int saved_ = -1;
#ifdef _WIN32
  static int duplicate(int fd) { return _dup(fd); }
  static int redirect(int from, int to) { return _dup2(from, to); }
  static int descriptor(FILE* file) { return _fileno(file); }
  static void close_fd(int fd) { _close(fd); }
#else
  static int duplicate(int fd) { return dup(fd); }
  static int redirect(int from, int to) { return dup2(from, to); }
  static int descriptor(FILE* file) { return fileno(file); }
  static void close_fd(int fd) { close(fd); }
#endif
  void restore() {
    if (saved_ < 0) return;
    std::fflush(stderr);
    redirect(saved_, descriptor(stderr));
    close_fd(saved_);
    saved_ = -1;
  }
 public:
  stderr_capture() {
    if (!file_) throw std::runtime_error("tmpfile failed");
    std::fflush(stderr);
    saved_ = duplicate(descriptor(stderr));
    if (saved_ < 0) throw std::runtime_error("dup failed");
    if (redirect(descriptor(file_.get()), descriptor(stderr)) < 0) {
      restore();
      throw std::runtime_error("dup2 failed");
    }
  }
  ~stderr_capture() { restore(); }
  std::string finish() {
    restore();
    std::rewind(file_.get());
    std::string result;
    char buffer[4096];
    while (auto size = std::fread(buffer, 1, sizeof(buffer), file_.get()))
      result.append(buffer, size);
    return result;
  }
};

struct profiling_guard {
  bool was_enabled = profile::enabled();
  uint32_t rate = profile::sample_rate_value().load();
  profiling_guard() { profile::configure(true, 1); }
  ~profiling_guard() { profile::configure(was_enabled, rate); }
};
}

TEST_CASE("RPC profiling covers sync coroutine and callback paths") {
  profiling_guard guard;
  coro_rpc::g_action = {};
  coro_rpc::coro_rpc_server server(2, 0);
  profile_service service;
  server.register_handler<profile_sync, profile_coro, profile_callback,
                          profile_void, profile_coro_void, profile_throw,
                          profile_inline_callback>();
  server.register_handler<&profile_service::sync, &profile_service::coro,
                          &profile_service::sync_void,
                          &profile_service::coro_void>(&service);
  auto running = server.async_start();
  REQUIRE(!running.hasResult());
  coro_rpc::coro_rpc_client client;
  REQUIRE(!syncAwait(client.connect("127.0.0.1", std::to_string(server.port()))));
  auto check_call = [&](auto call, bool serialized = true) {
    auto before = snapshot_counts();
    auto result = syncAwait(std::move(call));
    auto dispatch = static_cast<std::size_t>(profile::stage::server_dispatch);
    auto deadline = std::chrono::steady_clock::now() + 2s;
    auto after = snapshot_counts();
    while (after[dispatch] == before[dispatch] &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(1ms);
      after = snapshot_counts();
    }
    for (auto id : {profile::stage::server_deserialize_request,
                    profile::stage::server_handler_execute,
                    profile::stage::server_dispatch,
                    profile::stage::client_deserialize_response}) {
      auto stage = static_cast<std::size_t>(id);
      CHECK_MESSAGE(after[stage] - before[stage] == 1, profile::stage_names[stage]);
    }
    auto serialize = static_cast<std::size_t>(profile::stage::server_serialize_result);
    CHECK(after[serialize] - before[serialize] == (serialized ? 1 : 0));
    return result;
  };
  auto sync = check_call(client.call<profile_sync>(11));
  REQUIRE(sync);
  CHECK(sync.value() == 11);
  auto coro = check_call(client.call<profile_coro>(13));
  REQUIRE(coro);
  CHECK(coro.value() == 13);
  auto callback = check_call(client.call<profile_callback>(23));
  REQUIRE(callback);
  CHECK(callback.value() == 23);
  CHECK(check_call(client.call<profile_void>()));
  CHECK(check_call(client.call<profile_coro_void>()));
  auto member = check_call(client.call<&profile_service::sync>());
  REQUIRE(member);
  CHECK(member.value() == 17);
  auto member_coro = check_call(client.call<&profile_service::coro>());
  REQUIRE(member_coro);
  CHECK(member_coro.value() == 19);
  CHECK(check_call(client.call<&profile_service::sync_void>(1)));
  CHECK(check_call(client.call<&profile_service::coro_void>(1)));
  auto inline_result = check_call(client.call<profile_inline_callback>());
  REQUIRE(inline_result);
  CHECK(inline_result.value().value == 29);
  std::shared_ptr<profile::request_trace> trace;
  {
    std::lock_guard lock(inline_trace_mutex);
    trace = inline_trace;
  }
  REQUIRE(trace);
  auto duration = [&](profile::stage id) {
    return trace->durations[static_cast<std::size_t>(id)].load();
  };
  auto serialization = duration(profile::stage::server_serialize_result);
  CHECK(serialization >= 15000000);
  CHECK(trace->inline_serialization_ns.load() == serialization);
  CHECK(duration(profile::stage::server_handler_execute) + serialization <=
        duration(profile::stage::server_dispatch));
  CHECK(!check_call(client.call<profile_throw>(), false));
  client.close();
  server.stop();
  std::ostringstream output;
  profile::print(output);
  for (auto label : {"prepare_request", "send_request", "recv_header",
                     "recv_payload", "deserialize_response", "read_header",
                     "read_payload", "deserialize_request", "handler_execute",
                     "serialize_result", "serialize_response", "response_queue",
                     "send_response", "client.response_resume"}) {
    CHECK_MESSAGE(output.str().find(label) != std::string::npos, label);
  }
}

TEST_CASE("RPC profiling snapshot is safe during worker recording and exit") {
  profiling_guard guard;
  std::atomic<bool> started = false;
  std::atomic<bool> stop = false;
  std::thread worker([&] {
    started = true;
    while (!stop.load()) profile::record(profile::stage::urma_poll_completions, 3);
  });
  while (!started.load()) std::this_thread::yield();
  for (int i = 0; i < 10; ++i) {
    std::ostringstream output;
    profile::print(output);
  }
  stop = true;
  worker.join();
  std::ostringstream output;
  profile::print(output);
  CHECK(output.str().find("unit=count urma.poll_completions") != std::string::npos);
}


TEST_CASE("RPC profiling bounds polling samples and weights merged threads") {
  profiling_guard guard;
  constexpr auto id = profile::stage::urma_poll_completions;
  const auto stage = static_cast<std::size_t>(id);
  std::size_t retained = 0;
  bool retained_late_values = false;
  std::thread worker([&] {
    auto& local = profile::local_samples();
    local.polling_rng.seed(1);
    for (std::size_t i = 0; i < 50000; ++i)
      profile::record(id, i < 25000 ? 1 : 9);
    retained = local.samples[stage][0].size();
    retained_late_values = std::find(local.samples[stage][0].begin(),
                                    local.samples[stage][0].end(), 9) !=
                           local.samples[stage][0].end();
  });
  worker.join();
  CHECK(retained == profile::polling_sample_limit);
  CHECK(retained_late_values);
  {
    std::lock_guard lock(profile::registry_mutex());
    CHECK(profile::merged_samples()[stage][0].size() <= profile::polling_sample_limit);
  }
  std::vector<uint64_t> busy(profile::polling_sample_limit, 1), idle(1000, 9);
  uint64_t busy_seen = 100000;
  std::mt19937_64 rng(1);
  profile::merge_polling_samples(busy, busy_seen, idle, 1000, rng);
  CHECK(busy_seen == 101000);
  CHECK(busy.size() == profile::polling_sample_limit);
  auto idle_values = std::count(busy.begin(), busy.end(), 9);
  CHECK(idle_values > 0);
  CHECK(idle_values < 100); // Equal weighting of reservoirs would give about 800.
  std::ostringstream output;
  profile::print(output);
  CHECK(output.str().find("sampling=reservoir eligible=") != std::string::npos);
}

TEST_CASE("RPC profiling logs slow payload EOF with the protocol sequence") {
  profiling_guard guard;
  coro_rpc::g_action = {};
  stderr_capture capture;
  coro_rpc::coro_rpc_server server(1, 0, "127.0.0.1");
  server.register_handler<profile_sync>();
  auto running = server.async_start();
  REQUIRE(!running.hasResult());
  asio::io_context io;
  asio::ip::tcp::socket socket(io);
  socket.connect({asio::ip::address_v4::loopback(), server.port()});
  using protocol = coro_rpc::protocol::coro_rpc_protocol;
  protocol::req_header header{};
  header.magic = protocol::magic_number;
  header.seq_num = 73001;
  header.function_id = coro_rpc::func_id<profile_sync>();
  header.length = 100;
  auto before = snapshot_counts();
  auto bytes = struct_pack::serialize<struct_pack::sp_config::DISABLE_ALL_META_INFO>(header);
  asio::write(socket, asio::buffer(bytes));
  auto header_stage = static_cast<std::size_t>(profile::stage::server_read_header);
  auto deadline = std::chrono::steady_clock::now() + 2s;
  while (snapshot_counts()[header_stage] == before[header_stage] &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(1ms);
  REQUIRE(snapshot_counts()[header_stage] == before[header_stage] + 1);
  std::this_thread::sleep_for(5ms);
  socket.shutdown(asio::ip::tcp::socket::shutdown_send);
  char ignored;
  std::error_code ec;
  socket.read_some(asio::buffer(&ignored, 1), ec);
  CHECK(ec == asio::error::eof);
  server.stop();
  auto after = snapshot_counts();
  auto payload = static_cast<std::size_t>(profile::stage::server_read_payload);
  auto handler = static_cast<std::size_t>(profile::stage::server_handler_execute);
  CHECK(after[payload] == before[payload] + 1);
  CHECK(after[handler] == before[handler]);
  auto log = capture.finish();
  if (profile::slow_threshold_ns() == 0) {
    CHECK(log.find("ylt_rpc_slow ") == std::string::npos);
  }
  else if (profile::slow_threshold_ns() <= 5000000) {
    CHECK(log.find("ylt_rpc_slow side=server conn_id=") != std::string::npos);
    CHECK(log.find("request_id=73001 seq_num=73001") != std::string::npos);
    CHECK(log.find("status=read_error") != std::string::npos);
    CHECK(log.find("profile_sync") != std::string::npos);
    auto first = log.find("ylt_rpc_slow ");
    REQUIRE(first != std::string::npos);
    CHECK(log.find("ylt_rpc_slow ", first + 1) == std::string::npos);
  }
}
