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

#include "olap/delta_writer.h"

#include "olap/schema.h"
#include "olap/memtable.h"
#include "olap/data_dir.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/rowset_id_generator.h"

namespace doris {

OLAPStatus DeltaWriter::open(
        WriteRequest* req,
        BlockingQueue<std::shared_ptr<MemTable>>* flush_queue,
        DeltaWriter** writer) {
    *writer = new DeltaWriter(req, flush_queue);
    return OLAP_SUCCESS;
}

DeltaWriter::DeltaWriter(
        WriteRequest* req,
        BlockingQueue<std::shared_ptr<MemTable>>* flush_queue)
    : _req(*req), _tablet(nullptr),
      _cur_rowset(nullptr), _new_rowset(nullptr), _new_tablet(nullptr),
      _rowset_writer(nullptr), _schema(nullptr), _tablet_schema(nullptr),
      _delta_written_success(false), _flush_status(OLAP_SUCCESS),
      _flush_queue(flush_queue) {

    _mem_table.reset();
}

DeltaWriter::~DeltaWriter() {
    if (!_delta_written_success) {
        _garbage_collection();
    }

    _mem_table.reset();
    SAFE_DELETE(_schema);
    if (_rowset_writer != nullptr) {
        _rowset_writer->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX + _rowset_writer->rowset_id().to_string());
    }
}

void DeltaWriter::_garbage_collection() {
    OLAPStatus rollback_status = OLAP_SUCCESS;
    if (_tablet != nullptr) {
        rollback_status = StorageEngine::instance()->txn_manager()->rollback_txn(_req.partition_id,
            _req.txn_id,_req.tablet_id, _req.schema_hash, _tablet->tablet_uid());
    }
    // has to check rollback status, because the rowset maybe committed in this thread and
    // published in another thread, then rollback will failed
    // when rollback failed should not delete rowset
    if (rollback_status == OLAP_SUCCESS) {
        StorageEngine::instance()->add_unused_rowset(_cur_rowset);
    }
    if (_new_tablet != nullptr) {
        rollback_status = StorageEngine::instance()->txn_manager()->rollback_txn(_req.partition_id, _req.txn_id,
            _new_tablet->tablet_id(), _new_tablet->schema_hash(), _new_tablet->tablet_uid());
        if (rollback_status == OLAP_SUCCESS) {
            StorageEngine::instance()->add_unused_rowset(_new_rowset);
        }
    }
}

OLAPStatus DeltaWriter::init() {
    _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_req.tablet_id, _req.schema_hash);
    if (_tablet == nullptr) {
        LOG(WARNING) << "tablet_id: " << _req.tablet_id << ", "
                     << "schema_hash: " << _req.schema_hash << " not found";
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    {
        ReadLock base_migration_rlock(_tablet->get_migration_lock_ptr(), TRY_LOCK);
        if (!base_migration_rlock.own_lock()) {
            return OLAP_ERR_RWLOCK_ERROR;
        }
        MutexLock push_lock(_tablet->get_push_lock());
        RETURN_NOT_OK(StorageEngine::instance()->txn_manager()->prepare_txn(
                            _req.partition_id, _req.txn_id,
                            _req.tablet_id, _req.schema_hash, _tablet->tablet_uid(), _req.load_id));
        if (_req.need_gen_rollup) {
            AlterTabletTaskSharedPtr alter_task = _tablet->alter_task();
            if (alter_task != nullptr && alter_task->alter_state() != ALTER_FAILED) {
                TTabletId new_tablet_id = alter_task->related_tablet_id();
                TSchemaHash new_schema_hash = alter_task->related_schema_hash();
                LOG(INFO) << "load with schema change." << "old_tablet_id: " << _tablet->tablet_id() << ", "
                        << "old_schema_hash: " << _tablet->schema_hash() <<  ", "
                        << "new_tablet_id: " << new_tablet_id << ", "
                        << "new_schema_hash: " << new_schema_hash << ", "
                        << "transaction_id: " << _req.txn_id;
                _new_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(new_tablet_id, new_schema_hash);
                if (_new_tablet == nullptr) {
                    LOG(WARNING) << "find alter task, but could not find new tablet tablet_id: " << new_tablet_id
                                 << ", schema_hash: " << new_schema_hash;
                    return OLAP_ERR_TABLE_NOT_FOUND;
                }
                ReadLock new_migration_rlock(_new_tablet->get_migration_lock_ptr(), TRY_LOCK);
                if (!new_migration_rlock.own_lock()) {
                    return OLAP_ERR_RWLOCK_ERROR;
                }
                StorageEngine::instance()->txn_manager()->prepare_txn(
                                    _req.partition_id, _req.txn_id,
                                    new_tablet_id, new_schema_hash, _new_tablet->tablet_uid(), _req.load_id);
            }
        }
    }

    RowsetWriterContext writer_context;
    writer_context.rowset_id = StorageEngine::instance()->next_rowset_id();
    writer_context.tablet_uid = _tablet->tablet_uid();
    writer_context.tablet_id = _req.tablet_id;
    writer_context.partition_id = _req.partition_id;
    writer_context.tablet_schema_hash = _req.schema_hash;
    writer_context.rowset_type = DEFAULT_ROWSET_TYPE;
    writer_context.rowset_path_prefix = _tablet->tablet_path();
    writer_context.tablet_schema = &(_tablet->tablet_schema());
    writer_context.rowset_state = PREPARED;
    writer_context.data_dir = _tablet->data_dir();
    writer_context.txn_id = _req.txn_id;
    writer_context.load_id = _req.load_id;
    RETURN_NOT_OK(RowsetFactory::create_rowset_writer(writer_context, &_rowset_writer));

    _tablet_schema = &(_tablet->tablet_schema());
    _schema = new Schema(*_tablet_schema);
    _mem_table = std::make_shared<MemTable>(_tablet->tablet_id(), _schema, _tablet_schema, _req.slots, _req.tuple_desc, _tablet->keys_type());

    _is_init = true;
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::write(Tuple* tuple) {
    if (!_is_init) {
        auto st = init();
        if (st != OLAP_SUCCESS) {
            return st;
        }
    }

    _mem_table->insert(tuple);

    // if memtable is full, add it to the flush queue,
    // and create a new memtable for incoming data
    if (_mem_table->memory_usage() >= config::write_buffer_size) {
        if (_flush_status.load() != OLAP_SUCCESS) {
            // last flush already failed, return error
            return _flush_status.load();
        }

        if (!_flush_queue->blocking_put(_mem_table)) {
            // queue has been shutdown
            return OLAP_ERR_WRITER_FLUSH_QUEUE_SHUTDOWN;
        }
        // create a new memtable for new incoming data
        _mem_table.reset(new MemTable(_tablet->tablet_id(), _schema, _tablet_schema, _req.slots, _req.tuple_desc, _tablet->keys_type()));
    }
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::flush() {
    if (!_is_init) {
        auto st = init();
        if (st != OLAP_SUCCESS) {
            return st;
        }
    }

    // if last flush already failed, just return error
    RETURN_NOT_OK(_flush_status.load());
    // put last memtable to flush queue
    _flush_queue->blocking_put(_mem_table);
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::close(google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec) {
    // return error if flush failed
    OLAPStatus res = _flush_status.load();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to flush memtable for tablet: " << _tablet->tablet_id();
        return res;
    }

    // use rowset meta manager to save meta
    _cur_rowset = _rowset_writer->build();
    if (_cur_rowset == nullptr) {
        LOG(WARNING) << "fail to build rowset";
        return OLAP_ERR_MALLOC_ERROR;
    }
    res = StorageEngine::instance()->txn_manager()->commit_txn(_tablet->data_dir()->get_meta(),
        _req.partition_id, _req.txn_id,_req.tablet_id, _req.schema_hash, _tablet->tablet_uid(), 
        _req.load_id, _cur_rowset, false);
    if (res != OLAP_SUCCESS && res != OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
        LOG(WARNING) << "commit txn: " << _req.txn_id
                     << " for rowset: " << _cur_rowset->rowset_id()
                     << " failed.";
        return res;
    }

    if (_new_tablet != nullptr) {
        LOG(INFO) << "convert version for schema change";
        SchemaChangeHandler schema_change;
        res = schema_change.schema_version_convert(_tablet, _new_tablet, &_cur_rowset, &_new_rowset);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to convert delta for new tablet in schema change."
                         << "res: " << res << ", "
                         << "new_tablet: " << _new_tablet->full_name();
            return res;
        }

        res = StorageEngine::instance()->txn_manager()->commit_txn(_new_tablet->data_dir()->get_meta(),
                    _req.partition_id, _req.txn_id, _new_tablet->tablet_id(), 
                    _new_tablet->schema_hash(), _new_tablet->tablet_uid(),
                    _req.load_id, _new_rowset, false);

        if (res != OLAP_SUCCESS && res != OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
            LOG(WARNING) << "save pending rowset failed. rowset_id:"
                         << _new_rowset->rowset_id();
            return res;
        }
    }

#ifndef BE_TEST
    PTabletInfo* tablet_info = tablet_vec->Add();
    tablet_info->set_tablet_id(_tablet->tablet_id());
    tablet_info->set_schema_hash(_tablet->schema_hash());
    if (_new_tablet != nullptr) {
        tablet_info = tablet_vec->Add();
        tablet_info->set_tablet_id(_new_tablet->tablet_id());
        tablet_info->set_schema_hash(_new_tablet->schema_hash());
    }
#endif

    _delta_written_success = true;

    LOG(INFO) << "close delta writer for tablet: " << _tablet->tablet_id()
        << ", flush cost(ms): " << _flush_cost_ns / 1000 / 1000
        << ", flush times: " << _flush_time;
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::cancel() {
    DCHECK(!_is_init);
    return OLAP_SUCCESS;
}

} // namespace doris
