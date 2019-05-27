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

package org.apache.doris.persist;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/*
 * Author: Chenmingyu
 * Date: May 27, 2019
 */

public class ExchangePartitionInfo implements Writable {

    private long destDbId;
    private long destTblId;
    private Map<String, Long> partNameToSrcTblId = Maps.newHashMap();
    private Map<String, Long> partNameToSrcDbId = Maps.newHashMap();

    private ExchangePartitionInfo() {

    }

    public ExchangePartitionInfo(long dbId, long tblId, Map<String, OlapTable> partNameToSrcTbl,
            Map<String, Long> partNameToSrcDbId) {
        this.destDbId = dbId;
        this.destTblId = tblId;
        for (Map.Entry<String, OlapTable> entry : partNameToSrcTbl.entrySet()) {
            this.partNameToSrcTblId.put(entry.getKey(), entry.getValue().getId());
        }
        this.partNameToSrcDbId = partNameToSrcDbId;
    }

    public long getDestDbId() {
        return destDbId;
    }

    public long getDestTblId() {
        return destTblId;
    }

    public Map<String, Long> getPartNameToSrcDbId() {
        return partNameToSrcDbId;
    }

    public Map<String, Long> getPartNameToSrcTblId() {
        return partNameToSrcTblId;
    }

    public static ExchangePartitionInfo read(DataInput in) throws IOException {
        ExchangePartitionInfo info = new ExchangePartitionInfo();
        info.readFields(in);
        return info;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(destDbId);
        out.writeLong(destTblId);

        out.writeInt(partNameToSrcTblId.size());
        for (Map.Entry<String, Long> entry : partNameToSrcTblId.entrySet()) {
            Text.writeString(out, entry.getKey());
            out.writeLong(entry.getValue());
            out.writeLong(partNameToSrcDbId.get(entry.getKey()));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        destDbId = in.readLong();
        destTblId = in.readLong();

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String partName = Text.readString(in);
            partNameToSrcTblId.put(partName, in.readLong());
            partNameToSrcDbId.put(partName, in.readLong());
        }
    }
}
