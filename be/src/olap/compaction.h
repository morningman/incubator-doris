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

#ifndef DORIS_BE_SRC_OLAP_COMPACTION_H
#define DORIS_BE_SRC_OLAP_COMPACTION_H

#include <vector>

#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/utils.h"
#include "rowset/rowset_id_generator.h"

namespace doris {

class DataDir;
class Merger;

// This class is a base class for compaction.
// The entrance of this class is compact()
// Any compaction should go through four procedures.
//  1. pick rowsets satisfied to compact
//  2. do compaction
//  3. modify rowsets
//  4. gc unused rowstes
class Compaction {
public:
    Compaction(TabletSharedPtr tablet);
    virtual ~Compaction();

    OLAPStatus compact() {
        OLAPStatus st = compact_impl();
        if (_estimated_mem_consumption > 0) {
            Compaction::release_compaction_memory(_estimated_mem_consumption);
        }
        return st;
    }

    virtual OLAPStatus compact_impl() = 0;
public:

    // before doing compaction, the compation thread should call
    // consume_compaction_memory() to take memory. and call
    // release_compaction_memory() after compaction finished to release the memory.

    // try to consume memory. this method will block until there is enough memory to consume,
    // or timeout.
    static OLAPStatus consume_compaction_memory(int64_t consume_mem, int64_t timeout_second);
    // release the compaction memory and notify other waiting threads.
    static OLAPStatus release_compaction_memory(int64_t release_mem);
    
protected:
    virtual OLAPStatus pick_rowsets_to_compact() = 0;
    virtual std::string compaction_name() const = 0;
    virtual ReaderType compaction_type() const = 0;

    OLAPStatus do_compaction();
    OLAPStatus modify_rowsets();
    OLAPStatus gc_unused_rowsets();

    OLAPStatus construct_output_rowset_writer();
    OLAPStatus construct_input_rowset_readers();

    OLAPStatus check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets);
    OLAPStatus check_correctness(const Merger::Statistics& stats);

    // try to get enough memory before doing compaction.
    // this should be called after rowsets are picked, and before doing compaction.
    OLAPStatus consume_memory();

protected:
    TabletSharedPtr _tablet;

    std::vector<RowsetSharedPtr> _input_rowsets;
    std::vector<RowsetReaderSharedPtr> _input_rs_readers;
    int64_t _input_rowsets_size;
    int64_t _input_row_num;

    RowsetSharedPtr _output_rowset;
    std::unique_ptr<RowsetWriter> _output_rs_writer;

    enum CompactionState {
        INITED = 0,
        SUCCESS = 1
    };
    CompactionState _state;

    Version _output_version;
    VersionHash _output_version_hash;

    // estimated mem consumption of compaction
    int64_t _estimated_mem_consumption = 0;

    // the following 3 members are for limiting the memory of all compactions.
    // they are static so that all compaction instances can share them.
    static std::mutex _mem_lock;
    static std::condition_variable _mem_cv;
    // current mem consumption of compaction
    static int64_t _mem_consumption;

    DISALLOW_COPY_AND_ASSIGN(Compaction);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_COMPACTION_H
