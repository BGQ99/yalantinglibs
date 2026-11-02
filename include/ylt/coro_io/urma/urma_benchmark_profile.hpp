/*
 * Copyright (c) 2025, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <memory>
#include <sstream>
#include <limits>
#include <numeric>
#include <random>
#include <thread>
#include <string_view>
#include <vector>

namespace coro_io::urma_benchmark_profile {

// Payload size buckets in bytes.
//   0: [0, 100)
//   1: [100, 500)
//   2: [500, 1K)
//   3..N: [1K, 2K), [2K, 3K), ... each 1K wide
inline constexpr std::size_t bucket_count = 16;
inline constexpr std::array<uint32_t, bucket_count> bucket_upper = {
    100,  500,  1000, 2000,  3000,  4000,  5000,  6000,
    7000, 8000, 9000, 10000, 12000, 16000, 32000, UINT32_MAX,
};
inline constexpr std::array<std::string_view, bucket_count> bucket_names = {
    "0-100",   "100-500", "500-1K",  "1K-2K", "2K-3K", "3K-4K",
    "4K-5K",   "5K-6K",   "6K-7K",   "7K-8K", "8K-9K", "9K-10K",
    "10K-12K", "12K-16K", "16K-32K", "32K+",
};

inline std::size_t size_to_bucket(std::size_t payload_size) {
  for (std::size_t i = 0; i < bucket_count; ++i) {
    if (payload_size < bucket_upper[i])
      return i;
  }
  return bucket_count - 1;
}

enum class stage : uint8_t {
  benchmark_rpc_call = 0,
  client_prepare_request,
  client_send_request,
  client_recv_header,
  client_recv_payload,
  client_deserialize_response,
  server_read_header,
  server_read_payload,
  server_deserialize_request,
  server_handler_execute,
  server_dispatch,
  server_serialize_response,
  server_serialize_result,
  server_response_queue,
  server_send_response,
  client_connect_total,
  client_connect_tcp,
  client_connect_handshake,
  urma_write_total,
  urma_write_copy,
  urma_post_send,
  urma_wait_send_completion,
  urma_read_wait_completion,
  urma_read_copy,
  urma_read_view,
  raw_client_write,
  client_write_mutex_wait,
  client_wait_send_slot,
  client_response_resume,
  urma_timer_wakeup_delay,
  urma_post_queue_delay,
  urma_poll_call,
  urma_poll_round,
  urma_completion_callback,
  urma_completion_detect_to_callback,
  urma_poll_completions,
  urma_completion_resume,
  count
};

inline constexpr std::array<std::string_view,
                            static_cast<std::size_t>(stage::count)>
    stage_names = {
        "benchmark.rpc_call",
        "client.prepare_request",
        "client.send_request",
        "client.recv_header",
        "client.recv_payload",
        "client.deserialize_response",
        "server.read_header",
        "server.read_payload",
        "server.deserialize_request",
        "server.handler_execute",
        "server.dispatch",
        "server.serialize_response",
        "server.serialize_result",
        "server.response_queue",
        "server.send_response",
        "client.connect_total",
        "client.connect_tcp",
        "client.connect_handshake",
        "urma.write_total",
        "urma.write_copy",
        "urma.post_send",
        "urma.wait_send_completion",
        "urma.read_wait_completion",
        "urma.read_copy",
        "urma.read_view",
        "raw.client_write",
        "client.write_mutex_wait",
        "client.wait_send_slot",
        "client.response_resume",
        "urma.timer_wakeup_delay",
        "urma.post_queue_delay",
        "urma.poll_call",
        "urma.poll_round",
        "urma.completion_callback",
        "urma.completion_detect_to_callback",
        "urma.poll_completions",
        "urma.completion_resume",

};

inline std::atomic<bool>& enabled_flag() {
  static std::atomic<bool> value{false};
  return value;
}

inline std::atomic<uint32_t>& sample_rate_value() {
  static std::atomic<uint32_t> value{1};
  return value;
}

inline void print(std::ostream& os);

inline void register_exit_print() {
  static std::once_flag flag;
  std::call_once(flag, [] {
    std::atexit([] {
      if (enabled_flag().load(std::memory_order_relaxed)) {
        std::fprintf(stderr, "[rpc_profile] atexit: printing profile\n");
        print(std::cerr);
      }
    });
  });
}

inline void init_from_env() {
  static std::once_flag flag;
  std::call_once(flag, [] {
    const char* env = std::getenv("YLT_RPC_PROFILE_ENABLE");
    if (env && (std::string_view(env) == "1" || std::string_view(env) == "on" ||
                std::string_view(env) == "true")) {
      enabled_flag().store(true, std::memory_order_relaxed);
    }
    const char* rate = std::getenv("YLT_RPC_PROFILE_SAMPLE_RATE");
    if (rate && *rate) {
      uint32_t r = 0;
      for (const char* p = rate; *p >= '0' && *p <= '9'; ++p)
        r = r * 10 + (*p - '0');
      if (r > 0)
        sample_rate_value().store(r, std::memory_order_relaxed);
    }
    if (enabled_flag().load(std::memory_order_relaxed)) {
      register_exit_print();
    }
  });
}

inline bool enabled() noexcept {
  if (enabled_flag().load(std::memory_order_relaxed))
    return true;
  init_from_env();
  return enabled_flag().load(std::memory_order_relaxed);
}

inline void configure(bool enabled, uint32_t sample_rate) noexcept {
  init_from_env();
  if (enabled)
    register_exit_print();
  enabled_flag().store(enabled, std::memory_order_relaxed);
  sample_rate_value().store(std::max<uint32_t>(sample_rate, 1),
                            std::memory_order_relaxed);
}

inline uint64_t now_ns() noexcept {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

// Only the newly added, high-frequency polling stages use bounded reservoirs.
inline constexpr std::size_t polling_sample_limit = 4096;
inline bool is_polling_stage(std::size_t id) {
  return id >= static_cast<std::size_t>(stage::urma_timer_wakeup_delay) &&
         id < static_cast<std::size_t>(stage::count);
}

// Merge two uniform reservoirs by drawing population counts without replacement,
// then drawing that many retained values from each. Busy and idle threads must
// contribute in proportion to their eligible event counts, not reservoir sizes.
inline void merge_polling_samples(std::vector<uint64_t>& dst, uint64_t& dst_seen,
                                  const std::vector<uint64_t>& src,
                                  uint64_t src_seen, std::mt19937_64& rng) {
  if (!src_seen)
    return;
  if (!dst_seen) {
    dst = src;
    dst_seen = src_seen;
    return;
  }
  auto left = dst;
  auto right = src;
  uint64_t left_count = dst_seen, right_count = src_seen;
  std::size_t left_used = 0, right_used = 0;
  const auto size = std::min<uint64_t>(polling_sample_limit, dst_seen + src_seen);
  dst.clear();
  dst.reserve(size);
  for (std::size_t i = 0; i < size; ++i) {
    const bool take_left =
        std::uniform_int_distribution<uint64_t>(
            0, left_count + right_count - 1)(rng) < left_count;
    auto& values = take_left ? left : right;
    auto& used = take_left ? left_used : right_used;
    auto index = std::uniform_int_distribution<std::size_t>(
        used, values.size() - 1)(rng);
    std::swap(values[used], values[index]);
    dst.push_back(values[used++]);
    --(take_left ? left_count : right_count);
  }
  dst_seen += src_seen;
}

// Per-thread, per-stage, per-bucket samples.
struct thread_samples {
  std::array<std::array<std::vector<uint64_t>, bucket_count>,
             static_cast<std::size_t>(stage::count)>
      samples;
  std::array<uint32_t, static_cast<std::size_t>(stage::count)> counters{};
  std::array<uint64_t, static_cast<std::size_t>(stage::count)> polling_seen{};
  std::mt19937_64 polling_rng{now_ns()};
  // Only the owner thread and the snapshot reader take this lock.
  std::mutex snapshot_mutex;
  ~thread_samples();
};

inline std::mutex& registry_mutex() {
  static auto* value = new std::mutex;
  return *value;
}

inline std::array<std::array<std::vector<uint64_t>, bucket_count>,
                  static_cast<std::size_t>(stage::count)>&
merged_samples() {
  // Deliberately process-lived: atexit and TLS destruction order can differ.
  static auto* value =
      new std::array<std::array<std::vector<uint64_t>, bucket_count>,
                     static_cast<std::size_t>(stage::count)>;
  return *value;
}

inline std::array<uint32_t, static_cast<std::size_t>(stage::count)>&
merged_counters() {
  static auto* value = new std::array<uint32_t, static_cast<std::size_t>(stage::count)>{};
  return *value;
}

inline auto& merged_polling_seen() {
  static auto* value = new std::array<uint64_t, static_cast<std::size_t>(stage::count)>{};
  return *value;
}

inline std::vector<thread_samples*>& registry() {
  static auto* value = new std::vector<thread_samples*>;
  return *value;
}

inline thread_samples& local_samples() {
  thread_local thread_samples value;
  thread_local bool registered = [] {
    std::lock_guard<std::mutex> lock(registry_mutex());
    registry().push_back(&value);
    return true;
  }();
  (void)registered;
  return value;
}

inline thread_samples::~thread_samples() {
  std::lock_guard<std::mutex> lock(registry_mutex());
  auto& dst = merged_samples();
  auto& dst_cnt = merged_counters();
  for (std::size_t s = 0; s < samples.size(); ++s) {
    dst_cnt[s] += counters[s];
    if (is_polling_stage(s)) {
      merge_polling_samples(dst[s][0], merged_polling_seen()[s], samples[s][0],
                            polling_seen[s], polling_rng);
      continue;
    }
    for (std::size_t b = 0; b < bucket_count; ++b) {
      dst[s][b].insert(dst[s][b].end(), samples[s][b].begin(),
                       samples[s][b].end());
      samples[s][b].clear();
    }
  }
  auto& reg = registry();
  reg.erase(std::remove(reg.begin(), reg.end(), this), reg.end());
}

inline void record_with_size(stage stage_id, uint64_t duration_ns,
                             std::size_t payload_size) {
  if (!enabled())
    return;
  auto& local = local_samples();
  std::lock_guard snapshot_lock(local.snapshot_mutex);
  auto s = static_cast<std::size_t>(stage_id);
  auto b = is_polling_stage(s) ? 0 : size_to_bucket(payload_size);
  auto rate = sample_rate_value().load(std::memory_order_relaxed);
  auto counter = ++local.counters[s];
  if ((counter % rate) != 0)
    return;
  auto& values = local.samples[s][b];
  if (is_polling_stage(s)) {
    auto seen = ++local.polling_seen[s];
    if (values.size() < polling_sample_limit) {
      values.push_back(duration_ns);
    }
    else {
      auto index = std::uniform_int_distribution<uint64_t>(0, seen - 1)(
          local.polling_rng);
      if (index < polling_sample_limit)
        values[index] = duration_ns;
    }
    return;
  }
  values.push_back(duration_ns);
}

inline void record(stage stage_id, uint64_t duration_ns) {
  record_with_size(stage_id, duration_ns, 0);
}

inline void record_since(stage stage_id, uint64_t begin_ns) {
  if (!enabled())
    return;
  auto end_ns = now_ns();
  if (end_ns >= begin_ns)
    record(stage_id, end_ns - begin_ns);
}

inline void record_since_with_size(stage stage_id, uint64_t begin_ns,
                                   std::size_t payload_size) {
  if (!enabled())
    return;
  auto end_ns = now_ns();
  if (end_ns >= begin_ns)
    record_with_size(stage_id, end_ns - begin_ns, payload_size);
}

inline void reserve_per_stage(std::size_t count) {
  if (!enabled())
    return;
  auto& local = local_samples();
  std::lock_guard snapshot_lock(local.snapshot_mutex);
  for (std::size_t s = 0; s < local.samples.size(); ++s) {
    if (is_polling_stage(s)) {
      local.samples[s][0].reserve(std::min(count, polling_sample_limit));
    }
    else {
      for (auto& bucket : local.samples[s]) bucket.reserve(count);
    }
  }
}

inline void print(std::ostream& os) {
  // merged[stage][bucket] -> samples
  std::array<std::array<std::vector<uint64_t>, bucket_count>,
             static_cast<std::size_t>(stage::count)>
      merged;
  std::array<uint32_t, static_cast<std::size_t>(stage::count)> total_counters{};
  std::array<uint64_t, static_cast<std::size_t>(stage::count)> polling_seen{};
  std::mt19937_64 polling_rng{now_ns()};
  {
    std::lock_guard<std::mutex> lock(registry_mutex());
    for (std::size_t s = 0; s < merged.size(); ++s) {
      total_counters[s] += merged_counters()[s];
      polling_seen[s] = merged_polling_seen()[s];
      for (std::size_t b = 0; b < bucket_count; ++b) {
        auto& src = merged_samples()[s][b];
        merged[s][b].insert(merged[s][b].end(), src.begin(), src.end());
      }
    }
    for (auto* thread : registry()) {
      if (thread == nullptr)
        continue;
      std::lock_guard snapshot_lock(thread->snapshot_mutex);
      for (std::size_t s = 0; s < merged.size(); ++s) {
        total_counters[s] += thread->counters[s];
        if (is_polling_stage(s)) {
          merge_polling_samples(merged[s][0], polling_seen[s], thread->samples[s][0],
                                thread->polling_seen[s], polling_rng);
          continue;
        }
        for (std::size_t b = 0; b < bucket_count; ++b) {
          auto& src = thread->samples[s][b];
          merged[s][b].insert(merged[s][b].end(), src.begin(), src.end());
        }
      }
    }
  }

  os << "rpc_profile sample_rate="
     << sample_rate_value().load(std::memory_order_relaxed) << " unit=us\n";
  os << std::fixed << std::setprecision(2);

  // stats for one stage (merge all buckets)
  struct stats {
    std::size_t calls = 0, sampled = 0;
    double avg = 0, p50 = 0, p90 = 0, p99 = 0, p999 = 0, max = 0;
  };
  auto compute = [&](std::size_t s) -> stats {
    stats st;
    st.calls = total_counters[s];
    std::vector<uint64_t> all;
    for (std::size_t b = 0; b < bucket_count; ++b) {
      auto& v = merged[s][b];
      all.insert(all.end(), v.begin(), v.end());
    }
    if (all.empty())
      return st;
    st.sampled = all.size();
    std::sort(all.begin(), all.end());
    auto pct = [&](double p) {
      return static_cast<double>(
                 all[static_cast<std::size_t>((all.size() - 1) * p)]) /
             1000.0;
    };
    auto sum = std::accumulate(all.begin(), all.end(), 0LL);
    st.avg = static_cast<double>(sum) / all.size() / 1000.0;
    st.p50 = pct(0.50);
    st.p90 = pct(0.90);
    st.p99 = pct(0.99);
    st.p999 = pct(0.999);
    st.max = static_cast<double>(all.back()) / 1000.0;
    return st;
  };
  // stats for one bucket of one stage
  auto compute_b = [&](std::size_t s, std::size_t b) -> stats {
    stats st;
    st.calls = total_counters[s];
    auto& v = merged[s][b];
    if (v.empty())
      return st;
    st.sampled = v.size();
    std::sort(v.begin(), v.end());
    auto pct = [&](double p) {
      return static_cast<double>(
                 v[static_cast<std::size_t>((v.size() - 1) * p)]) /
             1000.0;
    };
    auto sum = std::accumulate(v.begin(), v.end(), 0LL);
    st.avg = static_cast<double>(sum) / v.size() / 1000.0;
    st.p50 = pct(0.50);
    st.p90 = pct(0.90);
    st.p99 = pct(0.99);
    st.p999 = pct(0.999);
    st.max = static_cast<double>(v.back()) / 1000.0;
    return st;
  };
  (void)compute_b;  // bucket detail available if needed later

  // Print a tree node: indent + branch char + label + stats
  auto node = [&](int indent, bool last, std::string_view label,
                  const stats& st) {
    for (int i = 0; i < indent; ++i) os << (i < indent - 1 ? "│  " : "   ");
    if (indent > 0)
      os << (last ? "└─ " : "├─ ");
    os << std::left << std::setw(28) << label << " n=" << std::right
       << std::setw(8) << st.sampled << " avg=" << std::setw(10) << st.avg
       << " p50=" << std::setw(10) << st.p50 << " p90=" << std::setw(10)
       << st.p90 << " p99=" << std::setw(10) << st.p99
       << " p999=" << std::setw(10) << st.p999 << " max=" << std::setw(10)
       << st.max << "\n";
  };
  // Print a parent node (with children)
  auto parent = [&](int indent, bool last, std::string_view label,
                    const stats& st) {
    for (int i = 0; i < indent; ++i) os << (i < indent - 1 ? "│  " : "   ");
    if (indent > 0)
      os << (last ? "└─ " : "├─ ");
    os << std::left << std::setw(28) << label << " n=" << std::right
       << std::setw(8) << st.sampled << " avg=" << std::setw(10) << st.avg
       << " p50=" << std::setw(10) << st.p50 << " p90=" << std::setw(10)
       << st.p90 << " p99=" << std::setw(10) << st.p99
       << " p999=" << std::setw(10) << st.p999 << " max=" << std::setw(10) << st.max << "\n";
  };
  auto has = [&](std::size_t s) {
    return total_counters[s] > 0;
  };
  auto st = [&](stage s) {
    return compute(static_cast<std::size_t>(s));
  };

  // Print bucket breakdown for a stage under the current parent
  auto buckets = [&](int indent, stage s) {
    for (std::size_t b = 0; b < bucket_count; ++b) {
      auto& v = merged[static_cast<std::size_t>(s)][b];
      if (v.empty())
        continue;
      auto bst = compute_b(static_cast<std::size_t>(s), b);
      for (int i = 0; i < indent; ++i) os << "  ";
      os << "  [" << bucket_names[b] << "]"
         << " n=" << std::right << std::setw(8) << bst.sampled
         << " avg=" << std::setw(10) << bst.avg << " p50=" << std::setw(10)
         << bst.p50 << " p99=" << std::setw(10) << bst.p99
         << " max=" << std::setw(10) << bst.max << "\n";
    }
  };

  // Additional scheduling and poller stages are independent measurements.
  os << "\n[scheduling_and_polling]\n";
  for (auto id = static_cast<std::size_t>(stage::client_write_mutex_wait);
       id < static_cast<std::size_t>(stage::count); ++id) {
    if (!has(id))
      continue;
    auto value = compute(id);
    if (id == static_cast<std::size_t>(stage::urma_poll_completions)) {
      // Stored unscaled; unlike duration stages, these samples count CQEs.
      value.avg *= 1000;
      value.p50 *= 1000;
      value.p90 *= 1000;
      value.p99 *= 1000;
      value.p999 *= 1000;
      value.max *= 1000;
      os << "unit=count ";
    }
    node(0, true, stage_names[id], value);
    if (is_polling_stage(id)) {
      os << "  sampling=reservoir eligible=" << polling_seen[id]
         << " retained=" << value.sampled << " limit=" << polling_sample_limit
         << " dropped=" << (polling_seen[id] - value.sampled) << "\n";
    }
  }

  // ── Benchmark ──
  if (has(static_cast<std::size_t>(stage::benchmark_rpc_call))) {
    os << "\n[benchmark]\n";
    parent(0, true, "rpc_call (total)", st(stage::benchmark_rpc_call));
    buckets(1, stage::benchmark_rpc_call);
  }

  // ── Client ──
  os << "\n[client]\n";
  if (has(static_cast<std::size_t>(stage::client_send_request))) {
    parent(0, false, "send_request", st(stage::client_send_request));
    buckets(1, stage::client_send_request);
    if (has(static_cast<std::size_t>(stage::client_prepare_request)))
      node(1, !has(static_cast<std::size_t>(stage::urma_write_total)),
           "prepare_request", st(stage::client_prepare_request));
    if (has(static_cast<std::size_t>(stage::urma_write_total))) {
      parent(1, true, "urma.write_total", st(stage::urma_write_total));
      bool has_wc = has(static_cast<std::size_t>(stage::urma_write_copy));
      bool has_ps = has(static_cast<std::size_t>(stage::urma_post_send));
      bool has_wsc =
          has(static_cast<std::size_t>(stage::urma_wait_send_completion));
      int cnt = has_wc + has_ps + has_wsc;
      if (has_wc)
        node(2, --cnt == 0, "write_copy", st(stage::urma_write_copy));
      if (has_ps)
        node(2, --cnt == 0, "post_send", st(stage::urma_post_send));
      if (has_wsc)
        node(2, --cnt == 0, "wait_send_completion",
             st(stage::urma_wait_send_completion));
    }
  }
  if (has(static_cast<std::size_t>(stage::client_recv_header))) {
    parent(0, false, "recv_header", st(stage::client_recv_header));
    buckets(1, stage::client_recv_header);
    bool has_rwc =
        has(static_cast<std::size_t>(stage::urma_read_wait_completion));
    bool has_rc = has(static_cast<std::size_t>(stage::urma_read_copy));
    int cnt = has_rwc + has_rc;
    if (has_rwc)
      node(1, --cnt == 0, "urma.read_wait",
           st(stage::urma_read_wait_completion));
    if (has_rc)
      node(1, --cnt == 0, "urma.read_copy", st(stage::urma_read_copy));
  }
  if (has(static_cast<std::size_t>(stage::client_recv_payload))) {
    parent(0, false, "recv_payload", st(stage::client_recv_payload));
    buckets(1, stage::client_recv_payload);
  }
  if (has(static_cast<std::size_t>(stage::client_deserialize_response)))
    parent(0, false, "deserialize_response",
           st(stage::client_deserialize_response));
  if (has(static_cast<std::size_t>(stage::client_connect_total))) {
    parent(0, true, "connect_total", st(stage::client_connect_total));
    bool has_tcp = has(static_cast<std::size_t>(stage::client_connect_tcp));
    bool has_hs =
        has(static_cast<std::size_t>(stage::client_connect_handshake));
    int cnt = has_tcp + has_hs;
    if (has_tcp)
      node(1, --cnt == 0, "connect_tcp", st(stage::client_connect_tcp));
    if (has_hs)
      node(1, --cnt == 0, "connect_handshake",
           st(stage::client_connect_handshake));
  }

  // ── Server ──
  bool has_server = has(static_cast<std::size_t>(stage::server_read_header)) ||
                    has(static_cast<std::size_t>(stage::server_dispatch));
  if (has_server) {
    os << "\n[server]\n";
    if (has(static_cast<std::size_t>(stage::server_read_header))) {
      parent(0, false, "read_header", st(stage::server_read_header));
      buckets(1, stage::server_read_header);
    }
    if (has(static_cast<std::size_t>(stage::server_read_payload))) {
      parent(0, false, "read_payload", st(stage::server_read_payload));
      buckets(1, stage::server_read_payload);
      bool has_rwc =
          has(static_cast<std::size_t>(stage::urma_read_wait_completion));
      bool has_rc = has(static_cast<std::size_t>(stage::urma_read_copy));
      int cnt = has_rwc + has_rc;
      if (has_rwc)
        node(1, --cnt == 0, "urma.read_wait",
             st(stage::urma_read_wait_completion));
      if (has_rc)
        node(1, --cnt == 0, "urma.read_copy", st(stage::urma_read_copy));
    }
    if (has(static_cast<std::size_t>(stage::server_dispatch))) {
      parent(0, false, "dispatch", st(stage::server_dispatch));
      buckets(1, stage::server_dispatch);
      bool has_deser =
          has(static_cast<std::size_t>(stage::server_deserialize_request));
      bool has_exec =
          has(static_cast<std::size_t>(stage::server_handler_execute));
      bool has_sres =
          has(static_cast<std::size_t>(stage::server_serialize_result));
      int cnt = has_deser + has_exec + has_sres;
      if (has_deser)
        node(1, --cnt == 0, "deserialize_request",
             st(stage::server_deserialize_request));
      if (has_exec)
        node(1, --cnt == 0, "handler_execute",
             st(stage::server_handler_execute));
      if (has_sres)
        node(1, --cnt == 0, "serialize_result",
             st(stage::server_serialize_result));
    }
    if (has(static_cast<std::size_t>(stage::server_serialize_response)))
      parent(0, false, "serialize_response",
             st(stage::server_serialize_response));
    if (has(static_cast<std::size_t>(stage::server_response_queue)))
      parent(0, false, "response_queue", st(stage::server_response_queue));
    if (has(static_cast<std::size_t>(stage::server_send_response))) {
      parent(0, true, "send_response", st(stage::server_send_response));
      if (has(static_cast<std::size_t>(stage::urma_write_total))) {
        parent(1, true, "urma.write_total", st(stage::urma_write_total));
        bool has_wc = has(static_cast<std::size_t>(stage::urma_write_copy));
        bool has_ps = has(static_cast<std::size_t>(stage::urma_post_send));
        bool has_wsc =
            has(static_cast<std::size_t>(stage::urma_wait_send_completion));
        int cnt = has_wc + has_ps + has_wsc;
        if (has_wc)
          node(2, --cnt == 0, "write_copy", st(stage::urma_write_copy));
        if (has_ps)
          node(2, --cnt == 0, "post_send", st(stage::urma_post_send));
        if (has_wsc)
          node(2, --cnt == 0, "wait_send_completion",
               st(stage::urma_wait_send_completion));
      }
    }
  }

  if (has(static_cast<std::size_t>(stage::urma_read_view))) {
    os << "\n[urma]\n";
    parent(0, true, "read_view", st(stage::urma_read_view));
  }
}


// Process configuration is read once. An invalid value retains the default.
inline uint64_t slow_threshold_ns() {
  static const uint64_t value = [] {
    const char* env = std::getenv("YLT_RPC_TRACE_SLOW_US");
    uint64_t us = 1000;
    if (env && *env) {
      uint64_t parsed = 0;
      bool valid = true;
      for (const char* p = env; *p; ++p) {
        if (*p < '0' || *p > '9' ||
            parsed > (std::numeric_limits<uint64_t>::max() / 1000 -
                      static_cast<unsigned>(*p - '0')) / 10) {
          valid = false;
          break;
        }
        parsed = parsed * 10 + (*p - '0');
      }
      if (valid) us = parsed;
    }
    return us * 1000;
  }();
  return value;
}

// Shared only by continuations for one request, never by unrelated requests.
struct request_trace {
  std::array<std::atomic<uint64_t>, static_cast<std::size_t>(stage::count)>
      durations{};
  std::atomic<uint64_t> response_ready_ns{0};
  std::atomic<bool> reported{false};
  std::atomic<bool> dispatch_done{false};
  std::atomic<uint64_t> response_end_ns{0};
  enum class server_result { ok, send_error, read_error };
  std::atomic<server_result> response_status{server_result::ok};
  // Set before a synchronous context handler publishes its context.
  std::thread::id synchronous_handler_thread;
  std::atomic<uint64_t> inline_serialization_ns{0};
  const uint64_t begin_ns = now_ns();
  uint64_t owner_id;
  uint32_t seq_num;
  std::string rpc;
  bool server;

  request_trace(uint64_t owner, uint32_t seq, std::string_view name,
                bool is_server)
      : owner_id(owner), seq_num(seq), rpc(name), server(is_server) {}

  void add(stage id, uint64_t duration) {
    durations[static_cast<std::size_t>(id)].fetch_add(
        duration, std::memory_order_relaxed);
  }

  void finish_read_error() {
    dispatch_done.store(true, std::memory_order_release);
    finish("read_error");
  }

  void finish(const char* status = "ok") {
    auto end = now_ns();
    if (server) {
      if (!response_end_ns.load(std::memory_order_acquire)) {
        auto result = std::string_view(status) == "ok" ? server_result::ok :
            std::string_view(status) == "read_error" ? server_result::read_error :
                                                     server_result::send_error;
        response_status.store(result);
        response_end_ns.store(end, std::memory_order_release);
      }
      if (!dispatch_done.load(std::memory_order_acquire))
        return;
      end = response_end_ns.load();
      auto result = response_status.load();
      status = result == server_result::ok ? "ok" :
          result == server_result::read_error ? "read_error" : "send_error";
    }
    auto threshold = slow_threshold_ns();
    if (!threshold || end - begin_ns < threshold || reported.exchange(true))
      return;
    std::ostringstream out;
    out << "ylt_rpc_slow side=" << (server ? "server" : "client")
        << (server ? " conn_id=" : " client_id=") << owner_id
        << " request_id=" << seq_num << " seq_num=" << seq_num
        << " rpc=" << std::quoted(rpc) << " total_us="
        << std::fixed << std::setprecision(3) << (end - begin_ns) / 1000.0
        << " status=" << status;
    if (server)
      out << " header_wait_in_total=0";
    auto first = server ? stage::server_read_header : stage::client_prepare_request;
    auto last = server ? stage::server_send_response : stage::client_deserialize_response;
    for (auto i = static_cast<std::size_t>(first);
         i <= static_cast<std::size_t>(last); ++i) {
      out << ' ' << stage_names[i] << "_us=" << durations[i].load() / 1000.0;
    }
    if (!server) {
      for (auto id : {stage::client_write_mutex_wait, stage::client_wait_send_slot,
                      stage::client_response_resume}) {
        out << ' ' << stage_names[static_cast<std::size_t>(id)] << "_us="
            << durations[static_cast<std::size_t>(id)].load() / 1000.0;
      }
    }
    auto line = out.str();
    std::fprintf(stderr, "%s\n", line.c_str());
  }
};

inline std::shared_ptr<request_trace> make_trace(uint64_t owner, uint32_t seq,
                                                std::string_view name,
                                                bool server = false) {
  if (!enabled() && !slow_threshold_ns())
    return {};
  return std::make_shared<request_trace>(owner, seq, name, server);
}

// Explicit finish separates adjacent phases; destruction records error paths.
class stage_sample {
 public:
  stage_sample(stage id, request_trace* trace = nullptr, std::size_t size = 0,
               bool force = false)
      : id_(id),
        trace_(trace),
        size_(size),
        begin_((trace || force || enabled()) ? now_ns() : 0) {}
  stage_sample(const stage_sample&) = delete;
  ~stage_sample() { finish(); }
  void exclude_inline_serialization() {
    if (!trace_)
      return;
    trace_->synchronous_handler_thread = std::this_thread::get_id();
    excluded_begin_ = trace_->inline_serialization_ns.load();
    exclude_inline_ = true;
  }
  uint64_t finish() {
    if (!begin_)
      return 0;
    auto duration = now_ns() - begin_;
    if (trace_ && id_ == stage::server_serialize_result &&
        trace_->synchronous_handler_thread == std::this_thread::get_id()) {
      trace_->inline_serialization_ns.fetch_add(duration);
    }
    if (exclude_inline_) {
      auto excluded = trace_->inline_serialization_ns.load() - excluded_begin_;
      duration -= std::min(duration, excluded);
    }
    if (trace_)
      trace_->add(id_, duration);
    record_with_size(id_, duration, size_);
    begin_ = 0;
    if (trace_ && trace_->server && id_ == stage::server_dispatch) {
      trace_->dispatch_done.store(true, std::memory_order_release);
      if (trace_->response_end_ns.load())
        trace_->finish();
    }
    return duration;
  }
 private:
  stage id_;
  request_trace* trace_;
  std::size_t size_;
  uint64_t begin_;
  uint64_t excluded_begin_ = 0;
  bool exclude_inline_ = false;
};

}  // namespace coro_io::urma_benchmark_profile

namespace coro_rpc {
namespace rpc_profile = coro_io::urma_benchmark_profile;
}
