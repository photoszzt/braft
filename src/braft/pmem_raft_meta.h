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

#ifndef BRAFT_STABLE_H
#define BRAFT_STABLE_H
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include "braft/storage.h"

namespace braft {

struct PersistRaftMeta {
    pmem::obj::p<int64_t> term;
    pmem::obj::persistent_ptr<PeerId> votedfor;

    ~PersistRaftMeta();
};

class PmemRaftMetaStorage : public RaftMetaStorage {
public:
    explicit PmemRaftMetaStorage(const std::string& path)
        : _is_inited(false), _path(path) {}
    PmemRaftMetaStorage() {}
    virtual ~PmemRaftMetaStorage();

    // init stable storage, check consistency and integrity
    virtual int init();

    // set current term
    virtual int set_term(const int64_t term);

    // get current term
    virtual int64_t get_term();

    // set votefor information
    virtual int set_votedfor(const PeerId& peer_id);

    // get votefor information
    virtual int get_votedfor(PeerId* peer_id);

    // set term and peer_id
    virtual int set_term_and_votedfor(const int64_t term, const PeerId& peer_id);

    RaftMetaStorage* new_instance(const std::string& uri) const;
private:
    static const char* _s_raft_meta;
    int load();
    int save();

    bool _is_inited;
    std::string _path;
    pmem::obj::pool<PersistRaftMeta> _state;
};

}

#endif //~BRAFT_STABLE_H
