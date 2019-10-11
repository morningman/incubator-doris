// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/load_channel_mgr.h"

#include "runtime/load_channel.h"
#include "runtime/mem_tracker.h"
#include "service/backend_options.h"
#include "util/stopwatch.hpp"
#include "olap/lru_cache.h"

namespace doris {

LoadChannelMgr::LoadChannelMgr() {
    _lastest_success_channel = new_lru_cache(1024);
}

LoadChannelMgr::~LoadChannelMgr() {
    delete _lastest_success_channel;
}

Status LoadChannelMgr::open(const PTabletWriterOpenRequest& params) {
    UniqueId load_id(params.id());
    std::shared_ptr<LoadChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _load_channels.find(load_id);
        if (it != _load_channels.end()) {
            channel = it->second;
        } else {
            // create a new load channel
            // default mem limit is used to be compatible with old request.
            // new request should be set load_mem_limit.
            const int64_t default_load_mem_limit = 2 * 1024 * 1024 * 1024L; // 2GB
            int64_t load_mem_limit = default_load_mem_limit;
            if (params.has_load_mem_limit()) {
                load_mem_limit = params.load_mem_limit();
            }
            channel.reset(new LoadChannel(load_id, load_mem_limit));
            _load_channels.insert({load_id, channel});
        }
    }

    RETURN_IF_ERROR(channel->open(params));
    return Status::OK();
}

static void dummy_deleter(const CacheKey& key, void* value) {
}

Status LoadChannelMgr::add_batch(
        const PTabletWriterAddBatchRequest& request,
        google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec,
        int64_t* wait_lock_time_ns) {

    UniqueId load_id(request.id());
    // 1. get load channel
    std::shared_ptr<LoadChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _load_channels.find(load_id);
        if (it == _load_channels.end()) {
            auto handle = _lastest_success_channel->lookup(load_id.to_string());
            // success only when eos be true
            if (handle != nullptr && request.has_eos() && request.eos()) {
                _lastest_success_channel->release(handle);
                return Status::OK();
            }
            std::stringstream ss;
            ss << "load channel manager add batch with unknown load id: " << load_id;
            return Status::InternalError(ss.str());
        }
        channel = it->second;
    }

    // 2. add batch to load channel
    if (request.has_row_batch()) {
        RETURN_IF_ERROR(channel->add_batch(request, tablet_vec));
    }

    // 3. handle finish
    if (channel->is_finished()) {
        std::lock_guard<std::mutex> l(_lock);
        _load_channels.erase(load_id);
        auto handle = _lastest_success_channel->insert(
                load_id.to_string(), nullptr, 1, dummy_deleter);
        _lastest_success_channel->release(handle);
    }
    return Status::OK();
}

Status LoadChannelMgr::cancel(const PTabletWriterCancelRequest& params) {
    UniqueId load_id(params.id());
    {
        std::lock_guard<std::mutex> l(_lock);
        _load_channels.erase(load_id);
    }
    return Status::OK();
}

Status LoadChannelMgr::start_bg_worker() {
    _load_channels_clean_thread = std::thread(
        [this] {
            #ifdef GOOGLE_PROFILER
                ProfilerRegisterThread();
            #endif

            uint32_t interval = 60;
            while (true) {
                _start_load_channels_clean();
                sleep(interval);
            }
        });
    _load_channels_clean_thread.detach();
    return Status::OK();
}

Status LoadChannelMgr::_start_load_channels_clean() {
    std::vector<std::shared_ptr<LoadChannel>> need_delete_channels;
    const int32_t max_alive_time = config::streaming_load_rpc_max_alive_time_sec;
    time_t now = time(nullptr);
    {
        std::vector<UniqueId> need_delete_channel_ids;
        std::lock_guard<std::mutex> l(_lock);
        for (auto& kv : _load_channels) {
            time_t last_updated_time = kv.second->last_updated_time();
            if (difftime(now, last_updated_time) >= max_alive_time) {
                need_delete_channel_ids.emplace_back(kv.first);
                need_delete_channels.emplace_back(kv.second);
            }
        }

        for(auto& key: need_delete_channel_ids) {
            _load_channels.erase(key);
            LOG(INFO) << "erase timeout load channel: " << key;
        }
    }

    // we must canel these load channels before destroying them.
    // or some object may be invalid before trying to visit it.
    // eg: MemTracker in load channel
    for (auto& channel : need_delete_channels) {
        channel->cancel();
        LOG(INFO) << "load channel has been safely deleted: " << channel->load_id();
    }

    return Status::OK();
}

}
