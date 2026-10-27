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
#pragma once

#include <string_view>

namespace mock_urma {
// The mock is single-threaded: use it only on the socket's executor.
void receive(std::string_view data, bool fail_completion = false);
void fail_next_recv();
void fail_next_send();
void fail_next_poll();
void hold_send_completions(bool hold);
void fail_next_allocation();
}  // namespace mock_urma
