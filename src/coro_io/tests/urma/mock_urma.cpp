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
#include "mock_urma.hpp"

#include <urma_api.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <new>

namespace {
urma_device_t device{};
urma_device_t* devices[] = {&device};
urma_eid_info_t eid{};
std::deque<urma_sge_t> receives;
std::deque<urma_cr_t> completions;
bool fail_recv = false;
bool fail_send = false;
bool fail_poll = false;
bool hold_sends = false;
thread_local bool fail_allocation = false;
}  // namespace

// Fail only the next allocation on this thread, for make_shared exception safety.
extern "C" void* __real__Znwm(std::size_t);
extern "C" void* __wrap__Znwm(std::size_t size) {
  if (fail_allocation) {
    fail_allocation = false;
    throw std::bad_alloc{};
  }
  return __real__Znwm(size);
}

namespace mock_urma {
void receive(std::string_view data, bool fail_completion) {
  assert(!receives.empty());
  auto sge = receives.front();
  receives.pop_front();
  assert(data.size() <= sge.len);
  std::memcpy(reinterpret_cast<void*>(sge.addr), data.data(), data.size());
  urma_cr_t cr{};
  cr.flag.bs.s_r = 1;
  cr.status = fail_completion ? URMA_CR_LOC_ACCESS_ERR : URMA_CR_SUCCESS;
  cr.completion_len = data.size();
  completions.push_back(cr);
}
void fail_next_recv() { fail_recv = true; }
void fail_next_send() { fail_send = true; }
void fail_next_poll() { fail_poll = true; }
void hold_send_completions(bool hold) { hold_sends = hold; }
void fail_next_allocation() { fail_allocation = true; }
}  // namespace mock_urma

extern "C" {
urma_status_t urma_init(urma_init_attr_t*) { return URMA_SUCCESS; }
urma_device_t** urma_get_device_list(int* count) {
  std::strcpy(device.name, "ownership_test");
  *count = 1;
  return devices;
}
void urma_free_device_list(urma_device_t**) {}
urma_eid_info_t* urma_get_eid_list(urma_device_t*, uint32_t* count) {
  *count = 1;
  return &eid;
}
void urma_free_eid_list(urma_eid_info_t*) {}
urma_context_t* urma_create_context(urma_device_t*, uint32_t) {
  return new urma_context_t{};
}
urma_status_t urma_delete_context(urma_context_t* ctx) {
  delete ctx;
  return URMA_SUCCESS;
}
urma_status_t urma_query_device(urma_device_t*, urma_device_attr_t* attr) {
  *attr = {};
  attr->dev_cap.max_jfc_depth = 1024;
  attr->dev_cap.max_jfr_depth = 1024;
  attr->dev_cap.max_jfs_depth = 1024;
  attr->dev_cap.rm_tp_cap.bs.ctp = 1;
  return URMA_SUCCESS;
}
urma_status_t urma_user_ctl(urma_context_t*, urma_user_ctl_in_t*,
                            urma_user_ctl_out_t*) {
  return URMA_SUCCESS;
}
urma_target_seg_t* urma_register_seg(urma_context_t*, urma_seg_cfg_t* cfg) {
  auto seg = new urma_target_seg_t{};
  seg->seg.ubva.va = cfg->va;
  seg->seg.len = cfg->len;
  return seg;
}
urma_status_t urma_unregister_seg(urma_target_seg_t* seg) {
  delete seg;
  return URMA_SUCCESS;
}
urma_target_seg_t* urma_import_seg(urma_context_t*, urma_seg_t* seg,
                                    urma_token_t*, uint64_t,
                                    urma_import_seg_flag_t) {
  auto result = new urma_target_seg_t{};
  result->seg = *seg;
  return result;
}
urma_status_t urma_unimport_seg(urma_target_seg_t* seg) {
  delete seg;
  return URMA_SUCCESS;
}
urma_jfc_t* urma_create_jfc(urma_context_t*, urma_jfc_cfg_t*) {
  assert(completions.empty());
  return new urma_jfc_t{};
}
urma_status_t urma_delete_jfc(urma_jfc_t* jfc) {
  completions.clear();
  delete jfc;
  return URMA_SUCCESS;
}
urma_jfr_t* urma_create_jfr(urma_context_t*, urma_jfr_cfg_t*) {
  assert(receives.empty());
  return new urma_jfr_t{};
}
urma_status_t urma_delete_jfr(urma_jfr_t* jfr) {
  receives.clear();
  delete jfr;
  return URMA_SUCCESS;
}
urma_jetty_t* urma_create_jetty(urma_context_t*, urma_jetty_cfg_t*) {
  return new urma_jetty_t{};
}
urma_status_t urma_delete_jetty(urma_jetty_t* jetty) {
  delete jetty;
  return URMA_SUCCESS;
}
urma_target_jetty_t* urma_import_jetty(urma_context_t*, urma_rjetty_t*,
                                        urma_token_t*) {
  return new urma_target_jetty_t{};
}
urma_status_t urma_unimport_jetty(urma_target_jetty_t* jetty) {
  delete jetty;
  return URMA_SUCCESS;
}
urma_status_t urma_post_jfr_wr(urma_jfr_t*, urma_jfr_wr_t* wr,
                              urma_jfr_wr_t**) {
  if (fail_recv) {
    fail_recv = false;
    return URMA_FAIL;
  }
  receives.push_back(*wr->src.sge);
  return URMA_SUCCESS;
}
urma_status_t urma_post_jetty_send_wr(urma_jetty_t*, urma_jfs_wr_t*,
                                     urma_jfs_wr_t**) {
  if (fail_send) {
    fail_send = false;
    return URMA_FAIL;
  }
  if (!hold_sends) {
    urma_cr_t cr{};
    cr.status = URMA_CR_SUCCESS;
    completions.push_back(cr);
  }
  return URMA_SUCCESS;
}
int urma_poll_jfc(urma_jfc_t*, int count, urma_cr_t* cr) {
  if (fail_poll) {
    fail_poll = false;
    return -1;
  }
  int n = std::min<int>(count, completions.size());
  for (int i = 0; i < n; ++i) {
    cr[i] = completions.front();
    completions.pop_front();
  }
  return n;
}
// Event mode is disabled by the fixture. Abort on unexpected event API calls.
urma_jfce_t* urma_create_jfce(urma_context_t*) { std::abort(); }
urma_status_t urma_delete_jfce(urma_jfce_t*) { std::abort(); }
urma_status_t urma_rearm_jfc(urma_jfc_t*, bool) { std::abort(); }
int urma_wait_jfc(urma_jfce_t*, uint32_t, int, urma_jfc_t**) { std::abort(); }
void urma_ack_jfc(urma_jfc_t**, uint32_t*, uint32_t) { std::abort(); }
}
