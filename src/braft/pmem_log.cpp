// Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Qin,Duohao(qinduohao@baidu.com)

#include <butil/time.h>
#include "braft/log_entry.h"
#include "braft/pmem_log.h"

using braft::plist;
using pmem::obj::pool;
using pmem::obj::persistent_ptr;
using pmem::obj::make_persistent;
using pmem::obj::delete_persistent;
using pmem::obj::transaction;

#define LAYOUT_NAME "pmem_log"

namespace braft {

const char* PmemLogStorage::_s_log = "pmem_log";

int PmemLogStorage::init(ConfigurationManager* configuration_manager) {
    butil::FilePath dir_path(_path);
    butil::File::Error e;
    if (!butil::CreateDirectoryAndGetError(
                dir_path, &e, FLAGS_raft_create_parent_directories)) {
        LOG(ERROR) << "Fail to create " << dir_path.value() << " : " << e;
        return -1;
    }
    pool<PersistLog> pop;
    std::string path(_path);
    path.append("/");
    path.append(_s_log);
    if (pool<PersistRaftMeta>::check(path, LAYOUT_NAME) == 1)
        pop = pool<PersistLog>::open(path, LAYOUT_NAME);
    else
        pop = pool<PersistLog>::create(path, LAYOUT_NAME, PMEMOBJ_MIN_POOL * 10, 0666);
    _state = pop;
    _first_log_index.store(1);
    _last_log_index.store(0);
    return 0;
}

PersistentLog::~PersistLog() {
    log_entry_data->clear();
    delete_persistent<list<LogEntry>>(log_entry_data);
}

LogEntry* PmemLogStorage::get_entry(const int64_t index) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (index < _first_log_index.load(butil::memory_order_relaxed)
            || index > _last_log_index.load(butil::memory_order_relaxed)) {
        return NULL;
    }
    LogEntry* temp = _log_entry_data[index - _first_log_index.load(butil::memory_order_relaxed)];
    temp->AddRef();
    CHECK(temp->id.index == index) << "get_entry entry index not equal. logentry index:"
            << temp->id.index << " required_index:" << index;
    lck.unlock();
    return temp;
}

int64_t PmemLogStorage::get_term(const int64_t index) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (index < _first_log_index.load(butil::memory_order_relaxed)
            || index > _last_log_index.load(butil::memory_order_relaxed)) {
        return 0;
    }
    LogEntry* temp = _log_entry_data.at(index - _first_log_index.load(butil::memory_order_relaxed));
    CHECK(temp->id.index == index) << "get_term entry index not equal. logentry index:"
            << temp->id.index << " required_index:" << index;
    int64_t ret = temp->id.term;
    lck.unlock();
    return ret;
}

int PmemLogStorage::append_entry(const LogEntry* input_entry) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (input_entry->id.index !=
            _last_log_index.load(butil::memory_order_relaxed) + 1) {
        CHECK(false) << "input_entry index=" << input_entry->id.index
                  << " _last_log_index=" << _last_log_index
                  << " _first_log_index=" << _first_log_index;
        return ERANGE;
    }
    input_entry->AddRef();
    _log_entry_data.push_back(const_cast<LogEntry*>(input_entry));
    _last_log_index.fetch_add(1, butil::memory_order_relaxed);
    lck.unlock();
    return 0;
}

int PmemLogStorage::append_entries(const std::vector<LogEntry*>& entries) {
    if (entries.empty()) {
        return 0;
    }
    for (size_t i = 0; i < entries.size(); i++) {
        LogEntry* entry = entries[i];
        append_entry(entry);
    }
    return entries.size();
}

int PmemLogStorage::truncate_prefix(const int64_t first_index_kept) {
    std::deque<LogEntry*> popped;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    while (!_log_entry_data.empty()) {
        LogEntry* entry = _log_entry_data.front();
        if (entry->id.index < first_index_kept) {
            popped.push_back(entry);
            _log_entry_data.pop_front();
        } else {
            break;
        }
    }
    _first_log_index.store(first_index_kept, butil::memory_order_release);
    if (_first_log_index.load(butil::memory_order_relaxed)
            > _last_log_index.load(butil::memory_order_relaxed)) {
        _last_log_index.store(first_index_kept - 1, butil::memory_order_release);
    }
    lck.unlock();

    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->Release();
    }
    return 0;
}

int PmemLogStorage::truncate_suffix(const int64_t last_index_kept) {
    std::deque<LogEntry*> popped;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    while (!_log_entry_data.empty()) {
        LogEntry* entry = _log_entry_data.back();
        if (entry->id.index > last_index_kept) {
            popped.push_back(entry);
            _log_entry_data.pop_back();
        } else {
            break;
        }
    }
    _last_log_index.store(last_index_kept, butil::memory_order_release);
    if (_first_log_index.load(butil::memory_order_relaxed)
            > _last_log_index.load(butil::memory_order_relaxed)) {
        _first_log_index.store(last_index_kept + 1, butil::memory_order_release);
    }
    lck.unlock();

    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->Release();
    }
    return 0;
}

int PmemLogStorage::reset(const int64_t next_log_index) {
    if (next_log_index <= 0) {
        LOG(ERROR) << "Invalid next_log_index=" << next_log_index;
        return EINVAL;
    }
    std::deque<LogEntry*> popped;
    std::unique_lock<raft_mutex_t> lck(_mutex);
    while (!_log_entry_data.empty()) {
        LogEntry* entry = _log_entry_data.back();
        popped.push_back(entry);
        _log_entry_data.pop_back();
    }
    _first_log_index.store(next_log_index, butil::memory_order_relaxed);
    _last_log_index.store(next_log_index - 1, butil::memory_order_relaxed);
    lck.unlock();

    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->Release();
    }
    return 0;
}

LogStorage* PmemLogStorage::new_instance(const std::string& uri) const {
    return new PmemLogStorage(uri);
}

} //  namespace braft
