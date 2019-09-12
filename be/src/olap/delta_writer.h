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

#ifndef DORIS_BE_SRC_DELTA_WRITER_H
#define DORIS_BE_SRC_DELTA_WRITER_H

#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/schema_change.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "gen_cpp/internal_service.pb.h"
#include "olap/rowset/rowset_writer.h"
#include "util/blocking_queue.hpp"

namespace doris {

class SegmentGroup;
class MemTable;
class Schema;

enum WriteType {
    LOAD = 1,
    LOAD_DELETE = 2,
    DELETE = 3
};

struct WriteRequest {
    int64_t tablet_id;
    int32_t schema_hash;
    WriteType write_type;
    int64_t txn_id;
    int64_t partition_id;
    PUniqueId load_id;
    bool need_gen_rollup;
    TupleDescriptor* tuple_desc;
    // slots are in order of tablet's schema
    const std::vector<SlotDescriptor*>* slots;
};

class DeltaWriter {
public:
    static OLAPStatus open(
        WriteRequest* req, 
        BlockingQueue<std::shared_ptr<MemTable>>* flush_queue,
        DeltaWriter** writer);
    OLAPStatus init();
    DeltaWriter(WriteRequest* req, BlockingQueue<std::shared_ptr<MemTable>>* flush_queue);
    ~DeltaWriter();
    OLAPStatus write(Tuple* tuple);
    // flush the last memtable to flush queue, must call it before close
    OLAPStatus flush();
    OLAPStatus close(google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec);

    OLAPStatus cancel();

    int64_t partition_id() const { return _req.partition_id; }

    void set_flush_status(OLAPStatus st) { _flush_status.store(st); }
    OLAPStatus get_flush_status() { return _flush_status.load(); }
    RowsetWriter* rowset_writer() { return _rowset_writer.get(); }

    void update_flush_time(int64_t flush_ns) {
        _flush_time_ns += flush_ns;
        _flush_count++;
    }

private:
    void _garbage_collection();

private:
    bool _is_init = false;
    WriteRequest _req;
    TabletSharedPtr _tablet;
    RowsetSharedPtr _cur_rowset;
    RowsetSharedPtr _new_rowset;
    TabletSharedPtr _new_tablet;
    std::unique_ptr<RowsetWriter> _rowset_writer;
    std::shared_ptr<MemTable> _mem_table;
    Schema* _schema;
    const TabletSchema* _tablet_schema;
    bool _delta_written_success;
    std::atomic<OLAPStatus> _flush_status;
    int64_t _flush_time_ns;
    int64_t _flush_count;

    // queue for saving immable mem tables
    BlockingQueue<std::shared_ptr<MemTable>>* _flush_queue;
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_DELTA_WRITER_H
