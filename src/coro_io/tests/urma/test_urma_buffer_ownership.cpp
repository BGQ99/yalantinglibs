/*
 * Copyright (c) 2026, Alibaba Group Holding Limited;
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
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "asio/any_io_executor.hpp"
#include "asio/io_context.hpp"
#include "async_simple/coro/SyncAwait.h"
#include "doctest.h"
#include "mock_urma.hpp"

// The current URMA headers use unqualified async_read/write/dispatch calls.
// Keep Asio's default deferred token out of overload resolution in this test.
namespace asio {
template <>
struct default_completion_token<any_io_executor> {
  using type = void;
};
template <>
struct default_completion_token<io_context::executor_type> {
  using type = void;
};
}  // namespace asio

#include "ylt/coro_io/urma/urma_io.hpp"

namespace {
std::atomic<std::size_t> duplicate_warnings{0};

void init_logging() {
  static const bool initialized = [] {
    easylog::init_log(easylog::Severity::WARN, "", false, false);
    easylog::add_appender([](easylog::record_t& record) {
      if (record.get_message().find("return duplicated URMA buffer") !=
          std::string_view::npos)
        ++duplicate_warnings;
    });
    return true;
  }();
  (void)initialized;
}

// Real socket and TCP handshake; only the URMA C API is replaced.
// All socket operations and mock completions run on one executor.
struct fixture {
  asio::io_context io;
  coro_io::ExecutorWrapper<> executor{io.get_executor()};
  asio::executor_work_guard<asio::io_context::executor_type> guard{
      asio::make_work_guard(io)};
  asio::ip::tcp::acceptor acceptor{
      io, {asio::ip::address_v4::loopback(), 0}};
  asio::ip::tcp::socket peer{io};
  static constexpr auto info_size =
      struct_pack::get_needed_size(coro_io::urma_socket_t::urma_socket_info{});
  std::array<char, info_size.size()> handshake{};
  std::unique_ptr<coro_io::urma_socket_t> socket;
  std::shared_ptr<coro_io::urma_buffer_pool_t> pool;
  std::thread worker;
  std::size_t warnings_before = 0;
  std::size_t expected_duplicates = 0;

  fixture() {
    init_logging();
    warnings_before = duplicate_warnings.load();
    coro_io::urma_socket_t::config_t config;
    config.device_name = "ownership_test";
    config.recv_buffer_cnt = 2;
    config.send_buffer_cnt = 2;
    config.event_mode = false;
    socket = std::make_unique<coro_io::urma_socket_t>(&executor, config);
    pool = socket->buffer_pool();
    acceptor.async_accept(peer, [this](std::error_code ec) {
      REQUIRE_FALSE(ec);
      asio::async_read(peer, asio::buffer(handshake),
                       [this](std::error_code ec, std::size_t) {
        REQUIRE_FALSE(ec);
        asio::async_write(peer, asio::buffer(handshake),
                          [this](std::error_code ec, std::size_t) {
          REQUIRE_FALSE(ec);
          peer.close(ec);
        });
      });
    });
    worker = std::thread([this] { io.run(); });
    auto endpoint = acceptor.local_endpoint();
    auto ec = async_simple::coro::syncAwait(
        socket->connect(std::array{endpoint}));
    REQUIRE_FALSE(ec);
  }

  template <typename F>
  void run(F&& function) {
    std::packaged_task<void()> task(std::forward<F>(function));
    auto result = task.get_future();
    asio::post(io, std::move(task));
    result.get();
  }

  void receive(std::string_view data, bool fail = false) {
    bool resumed = false;
    socket->post_recv([&](auto result) {
      CHECK(bool(result.first) == fail);
      CHECK(result.second == data.size());
      resumed = true;
    });
    mock_urma::receive(data, fail);
    socket->poll_completion_once();
    REQUIRE(resumed);
  }

  void close() {
    run([this] { socket.reset(); });
    // Drain the cancelled polling timer's handler, which holds shared state.
    run([] {});
  }

  ~fixture() {
    close();
    guard.reset();
    io.stop();
    worker.join();
    CHECK(duplicate_warnings.load() ==
          warnings_before + expected_duplicates);
  }
};

// The guard must be declared after every local object borrowed by the operation.
// On assertion failure or another exception, cancel while the socket and those
// objects are still alive, then wait for the final completion callback.
template <typename Result>
struct pending_operation_guard {
  fixture& f;
  std::shared_future<Result> completion;

  ~pending_operation_guard() {
    if (completion.wait_for(std::chrono::seconds(0)) ==
        std::future_status::ready)
      return;
    f.run([this] { f.socket->close(); });
    completion.wait();
  }
};

void check_pool(const std::shared_ptr<coro_io::urma_buffer_pool_t>& pool,
                std::size_t outstanding) {
  CHECK(pool->outstanding_buffer_count() == outstanding);
  CHECK(pool->free_buffer_count() + outstanding ==
        pool->total_buffer_count());
}
}  // namespace

TEST_CASE("urma ordinary receive clears the socket buffer after release") {
  fixture f;
  f.run([&] {
    auto baseline = f.pool->outstanding_buffer_count();
    f.receive("payload");
    f.socket->set_read_buffer_len(7, 0);
    CHECK(f.socket->get_recv_buffer().addr == 0);
    check_pool(f.pool, baseline);
    // Releasing again must be harmless.
    f.socket->set_read_buffer_len(0, 0);
    check_pool(f.pool, baseline);

    f.receive("prefix-tail");
    f.socket->set_read_buffer_len(7, 4);
    char tail[4]{};
    CHECK(f.socket->consume(tail, sizeof(tail)) == sizeof(tail));
    CHECK(std::string_view(tail, sizeof(tail)) == "tail");
    CHECK(f.socket->get_recv_buffer().addr == 0);
    check_pool(f.pool, baseline);
  });
  f.close();
  check_pool(f.pool, 0);
}

TEST_CASE("urma detached receive has exactly one shared owner") {
  for (bool remaining : {false, true}) {
    for (bool socket_first : {false, true}) {
      CAPTURE(remaining);
      CAPTURE(socket_first);
      fixture f;
      coro_io::owned_data_view view;
      f.run([&] {
        f.receive("prefix-tail");
        if (remaining) {
          f.socket->set_read_buffer_len(7, 4);
          view = f.socket->detach_remain_data_view();
          CHECK(view.size() == 4);
        }
        else {
          view = f.socket->detach_recv_buffer_view(11);
          CHECK(view.size() == 11);
        }
        CHECK(f.socket->get_recv_buffer().addr == 0);
        CHECK(f.socket->remain_read_buffer_size() == 0);
        CHECK(f.socket->detach_recv_buffer_view(11).empty());
      });
      auto copy = view;
      if (socket_first) {
        f.close();
        check_pool(f.pool, 1);
        CHECK(std::string_view(copy).ends_with("tail"));
      }
      auto before = f.pool->outstanding_buffer_count();
      view = {};
      check_pool(f.pool, before);
      copy = {};
      check_pool(f.pool, before - 1);
      f.close();
      check_pool(f.pool, 0);
    }
  }
}

TEST_CASE("urma owner allocation failure preserves socket ownership") {
  for (bool remaining : {false, true}) {
    fixture f;
    f.run([&] {
      f.receive("prefix-tail");
      if (remaining)
        f.socket->set_read_buffer_len(7, 4);
      auto addr = f.socket->get_recv_buffer().addr;
      auto before = f.pool->outstanding_buffer_count();
      mock_urma::fail_next_allocation();
      bool threw = false;
      try {
        if (remaining)
          f.socket->detach_remain_data_view();
        else
          f.socket->detach_recv_buffer_view(11);
      } catch (const std::bad_alloc&) {
        threw = true;
      }
      CHECK(threw);
      CHECK(f.socket->get_recv_buffer().addr == addr);
      check_pool(f.pool, before);
    });
    f.close();
    check_pool(f.pool, 0);
  }
}

TEST_CASE("urma stale socket cannot return a reallocated buffer") {
  fixture f;
  std::vector<coro_io::urma_buffer_t> held;
  f.run([&] {
    f.receive("payload");
    auto addr = f.socket->get_recv_buffer().addr;
    f.socket->set_read_buffer_len(7, 0);
    // Exhaust the pool to include the same address regardless of shard order.
    while (f.pool->free_buffer_count())
      held.push_back(f.pool->get_buffer());
    REQUIRE(std::any_of(held.begin(), held.end(), [addr](const auto& buffer) {
      return reinterpret_cast<uint64_t>(buffer.addr) == addr;
    }));
  });
  f.close();
  check_pool(f.pool, held.size());
  for (auto& buffer : held)
    f.pool->return_buffer(buffer);
  check_pool(f.pool, 0);
}

TEST_CASE("urma duplicate return guard preserves pool accounting") {
  fixture f;
  auto buffer = f.pool->get_buffer();
  auto alias = buffer;  // Deliberate misuse to exercise the existing guard.
  auto before = f.pool->outstanding_buffer_count();
  f.pool->return_buffer(buffer);
  CHECK_FALSE(buffer);
  f.pool->return_buffer(alias);
  CHECK_FALSE(alias);
  f.expected_duplicates = 1;
  check_pool(f.pool, before - 1);
  f.close();
  check_pool(f.pool, 0);
}

TEST_CASE("urma close cleans current cached queued and pending buffers") {
  fixture f;
  f.run([&] {
    f.receive("current");
    // The second completion is cached rather than delivered to a waiter.
    mock_urma::receive("cached");
    f.socket->poll_completion_once();
    mock_urma::hold_send_completions(true);
    auto buffer = f.socket->get_send_buffer();
    f.socket->post_send(buffer, 1, [](auto result) {
      CHECK(result.first == std::errc::operation_canceled);
    });
    f.socket->close();
    mock_urma::hold_send_completions(false);
  });
  f.close();
  check_pool(f.pool, 0);
}

TEST_CASE("urma send completion and submission failures return buffers") {
  fixture f;
  f.run([&] {
    auto baseline = f.pool->outstanding_buffer_count();
    for (bool fail : {false, true}) {
      bool resumed = false;
      if (fail)
        mock_urma::fail_next_send();
      f.socket->post_send(f.socket->get_send_buffer(), 1, [&](auto result) {
        CHECK(bool(result.first) == fail);
        resumed = true;
      });
      f.socket->poll_completion_once();
      CHECK(resumed);
      check_pool(f.pool, baseline);
    }
  });
  f.close();
  check_pool(f.pool, 0);
}

TEST_CASE("urma receive and poll failures are cleaned on close") {
  for (bool poll_failure : {false, true}) {
    fixture f;
    auto failure = std::make_shared<std::promise<std::error_code>>();
    auto result = failure->get_future().share();
    f.run([&] {
      if (poll_failure) {
        f.socket->post_recv([failure](auto completion) {
          failure->set_value(completion.first);
        });
        mock_urma::fail_next_poll();
        // The production watcher sees the failure and closes pending work.
      }
      else {
        mock_urma::fail_next_recv();
        f.receive("failed", true);
      }
      if (!poll_failure)
        f.socket->close();
    });
    if (poll_failure) {
      pending_operation_guard pending{f, result};
      REQUIRE(result.wait_for(std::chrono::seconds(2)) ==
              std::future_status::ready);
      CHECK(result.get() == std::errc::io_error);
    }
    f.close();
    check_pool(f.pool, 0);
  }
}

TEST_CASE("urma view read error releases completed views and current buffer") {
  fixture f;
  // Cache one good completion and one error; run the actual coroutine.
  f.run([&] {
    mock_urma::receive("good");
    mock_urma::receive("bad", true);
    f.socket->poll_completion_once();
  });
  auto [ec, views] = async_simple::coro::syncAwait(
      coro_io::detail::async_urma_read_views(*f.socket, 7));
  CHECK(ec);
  REQUIRE(views.size() == 1);
  CHECK(std::string_view(views[0]) == "good");
  f.close();
  check_pool(f.pool, 1);
  views.clear();
  check_pool(f.pool, 0);
}

TEST_CASE("urma 64 KiB copy receive repeats without duplicate returns") {
  fixture f;
  const std::string chunk(4096, 'x');
  for (int round = 0; round < 10; ++round) {
    std::string output(65536, '\0');
    auto done = std::make_shared<
        std::promise<std::pair<std::error_code, std::size_t>>>();
    auto result = done->get_future().share();
    auto baseline = f.pool->outstanding_buffer_count();
    auto output_buffer = asio::buffer(output);
    coro_io::async_read(*f.socket, output_buffer)
        .start([done](auto completion) {
          if (completion.hasError())
            done->set_exception(completion.getException());
          else
            done->set_value(completion.value());
        });
    pending_operation_guard pending{f, result};
    for (int i = 0; i < 16; ++i) {
      f.run([&] {
        mock_urma::receive(chunk);
        f.socket->poll_completion_once();
      });
    }
    REQUIRE(result.wait_for(std::chrono::seconds(2)) ==
            std::future_status::ready);
    auto [ec, length] = result.get();
    CHECK_FALSE(ec);
    CHECK(length == output.size());
    CHECK(output == std::string(65536, 'x'));
    f.run([&] {
      check_pool(f.pool, baseline);
    });
  }
  f.close();
  check_pool(f.pool, 0);
}

TEST_CASE("urma pending receive is cancelled before timeout unwinding") {
  fixture f;
  std::shared_future<std::error_code> result;
  auto timeout = [&] {
    auto failure = std::make_shared<std::promise<std::error_code>>();
    result = failure->get_future().share();
    f.run([&] {
      f.socket->post_recv([failure](auto completion) {
        failure->set_value(completion.first);
      });
    });
    pending_operation_guard pending{f, result};
    CHECK(result.wait_for(std::chrono::milliseconds(5)) ==
          std::future_status::timeout);
    throw std::runtime_error("simulate timeout assertion unwinding");
  };
  CHECK_THROWS_AS(timeout(), std::runtime_error);
  REQUIRE(result.wait_for(std::chrono::seconds(0)) ==
          std::future_status::ready);
  CHECK(result.get() == std::errc::operation_canceled);
  f.close();
  check_pool(f.pool, 0);
}

TEST_CASE("urma pending read releases borrowed memory after cancellation") {
  fixture f;
  std::shared_future<std::pair<std::error_code, std::size_t>> result;
  auto timeout = [&] {
    std::string output(65536, '\0');
    auto output_buffer = asio::buffer(output);
    auto done = std::make_shared<
        std::promise<std::pair<std::error_code, std::size_t>>>();
    result = done->get_future().share();
    coro_io::async_read(*f.socket, output_buffer)
        .start([done](auto completion) {
          if (completion.hasError())
            done->set_exception(completion.getException());
          else
            done->set_value(completion.value());
        });
    pending_operation_guard pending{f, result};
    CHECK(result.wait_for(std::chrono::milliseconds(5)) ==
          std::future_status::timeout);
    throw std::runtime_error("simulate timeout assertion unwinding");
  };
  CHECK_THROWS_AS(timeout(), std::runtime_error);
  REQUIRE(result.wait_for(std::chrono::seconds(0)) ==
          std::future_status::ready);
  auto [ec, length] = result.get();
  CHECK(ec == std::errc::operation_canceled);
  CHECK(length == 0);
  f.close();
  check_pool(f.pool, 0);
}
