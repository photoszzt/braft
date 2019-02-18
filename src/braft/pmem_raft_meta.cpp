// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
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

// Authors: Wang,Yao(wangyao02@baidu.com)

#include <errno.h>
#include <butil/time.h>
#include <butil/logging.h>
#include <butil/file_util.h>                         // butil::CreateDirectory
#include "braft/util.h"
#include "braft/protobuf_file.h"
#include "braft/local_storage.pb.h"
#include "braft/pmem_raft_meta.h"

using pmem::obj::pool;
using pmem::obj::persistent_ptr;
using pmem::obj::make_persistent;
using pmem::obj::delete_persistent;
using pmem::obj::transaction;

#define LAYOUT_NAME "pmem_raft_meta"

namespace braft {

const char* PmemRaftMetaStorage::_s_raft_meta = "pmem_raft_meta";

int PmemRaftMetaStorage::init() {
    if (_is_inited) {
        return 0;
    }
    butil::FilePath dir_path(_path);
    butil::File::Error e;
    if (!butil::CreateDirectoryAndGetError(
                dir_path, &e, FLAGS_raft_create_parent_directories)) {
        LOG(ERROR) << "Fail to create " << dir_path.value() << " : " << e;
        return -1;
    }

    pool<PersistRaftMeta> pop;
    std::string path(_path);
    path.append("/");
    path.append(_s_raft_meta);
    if (pool<PersistRaftMeta>::check(path, LAYOUT_NAME) == 1)
        pop = pool<PersistRaftMeta>::open(path, LAYOUT_NAME);
    else
        pop = pool<PersistRaftMeta>::create(path, LAYOUT_NAME, PMEMOBJ_MIN_POOL * 10, 0666);

    int ret = load();
    if (ret == 0) {
        _is_inited = true;
    }
    _state = pop;
    return ret;
}

int PmemRaftMetaStorage::set_term(const int64_t term) {
    if (_is_inited) {
        persistent_ptr<PersistRaftMeta> r = _state.root();
        transaction::run(_state, [&] {
            r->term = term;
        });
        return 0;
    } else {
        LOG(WARNING) << "PmemRaftMetaStorage not init(), path: " << _path;
        return -1;
    }
}

int64_t PmemRaftMetaStorage::get_term() {
    if (_is_inited) {
        persistent_ptr<PersistRaftMeta> r = _state.root();
        return r->term;
    } else {
        LOG(WARNING) << "PmemRaftMetaStorage not init(), path: " << _path;
        return -1;
    }
}

int PmemRaftMetaStorage::set_votedfor(const PeerId& peer_id) {
    if (_is_inited) {
        persistent_ptr<PersistRaftMeta> r = _state.root();
        transaction::run(_state, [&] {
            r->votedfor = make_persistent<PeerId>(peer_id);
        });
        return 0;
    } else {
        LOG(WARNING) << "PmemRaftMetaStorage not init(), path: " << _path;
        return -1;
    }
}

int PmemRaftMetaStorage::set_term_and_votedfor(const int64_t term, const PeerId& peer_id) {
    if (_is_inited) {
        persistent_ptr<PersistRaftMeta> r = _state.root();
        transaction::run(_state, [&] {
            r->term = term;
            r->votedfor = make_persistent<PeerId>(peer_id);
        });
        return save();
    } else {
        LOG(WARNING) << "PmemRaftMetaStorage not init(), path: " << _path;
        return -1;
    }
}

int PmemRaftMetaStorage::load() {
    return 0;
}

int PmemRaftMetaStorage::save() {
    return 0;
}

int PmemRaftMetaStorage::get_votedfor(PeerId* peer_id) {
    if (_is_inited) {
        persistent_ptr<PersistRaftMeta> r = _state.root();
        *peer_id = *r->votedfor;
        return 0;
    } else {
        LOG(WARNING) << "PmemRaftMetaStorage not init(), path: " << _path;
        return -1;
    }
}

PmemRaftMetaStorage::~PmemRaftMetaStorage() {
    _state.close();
}


RaftMetaStorage* PmemRaftMetaStorage::new_instance(const std::string& uri) const {
    return new PmemRaftMetaStorage(uri);
}

PersistRaftMeta::~PersistRaftMeta() {
    delete_persistent<PeerId>(votedfor);
    votedfor = nullptr;
}

}
