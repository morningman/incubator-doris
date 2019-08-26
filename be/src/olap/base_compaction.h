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

#ifndef DORIS_BE_SRC_OLAP_BASE_COMPACTION_H
#define DORIS_BE_SRC_OLAP_BASE_COMPACTION_H

#include "olap/compaction.h"

namespace doris {

// BaseCompaction is derived from Compaction.
// BaseCompaction will implements
//   1. its policy to pick rowsests
//   2. do compaction to produce new rowset.

class BaseCompaction : public Compaction {
public:
    BaseCompaction(TabletSharedPtr tablet, DataDir* data_dir);
    ~BaseCompaction() override;

    OLAPStatus compact() override;

protected:
    OLAPStatus pick_rowsets_to_compact() override;
    std::string compaction_name() const override {
        return "base compaction";
    }

    ReaderType compaction_type() const override {
        return ReaderType::READER_BASE_COMPACTION;
    }

private:
    DISALLOW_COPY_AND_ASSIGN(BaseCompaction);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_BASE_COMPACTION_H
