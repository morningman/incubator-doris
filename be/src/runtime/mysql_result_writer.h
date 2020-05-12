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

#pragma once

#include "runtime/result_writer.h"

namespace doris {

class TupleRow;
class RowBatch;
class ExprContext;
class MysqlRowBuffer;
class BufferControlBlock;
class RuntimeState;

// convert the row batch to mysql protol row
class MysqlResultWriter : public ResultWriter {
public:
    MysqlResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs);
    virtual ~MysqlResultWriter();

    virtual Status init(RuntimeState* state) override;
    // convert one row batch to mysql result and
    // append this batch to the result sink
    virtual Status append_row_batch(RowBatch* batch) override;

    virtual Status close() override;

private:
    // convert one tuple row
    Status add_one_row(TupleRow* row);

    // The expressions that are run to create tuples to be written to hbase.
    BufferControlBlock* _sinker;
    const std::vector<ExprContext*>& _output_expr_ctxs;
    MysqlRowBuffer* _row_buffer;
};

} // end of namespace

