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

package org.apache.doris.catalog;

import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.ReplicaAllocation.AllocationType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Writable;
import org.apache.doris.resource.TagSet;
import org.apache.doris.system.Backend;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/*
 * This class saves the schema of a colocation group
 */
public class ColocateGroupSchema implements Writable {
    private GroupId groupId;
    private List<Type> distributionColTypes = Lists.newArrayList();
    private int bucketsNum;
    @Deprecated
    private short replicationNum;
    private ReplicaAllocation replicaAllocation;

    private boolean isTagSystemConverted = false;

    private ColocateGroupSchema() {

    }

    public ColocateGroupSchema(GroupId groupId, List<Column> distributionCols, int bucketsNum,
            ReplicaAllocation replicaAlloc) {
        this.groupId = groupId;
        this.distributionColTypes = distributionCols.stream().map(c -> c.getType()).collect(Collectors.toList());
        this.bucketsNum = bucketsNum;
        this.replicaAllocation = replicaAlloc;
    }

    @Deprecated
    public ColocateGroupSchema(GroupId groupId, List<Column> distributionCols, int bucketsNum, short replicaNum) {
        this.groupId = groupId;
        this.distributionColTypes = distributionCols.stream().map(c -> c.getType()).collect(Collectors.toList());
        this.bucketsNum = bucketsNum;
        this.replicationNum = replicaNum;
    }

    public GroupId getGroupId() {
        return groupId;
    }

    public int getBucketsNum() {
        return bucketsNum;
    }

    public ReplicaAllocation getReplicaAllocation() {
        return replicaAllocation;
    }

    public List<Type> getDistributionColTypes() {
        return distributionColTypes;
    }

    public void checkColocateSchema(OlapTable tbl) throws DdlException {
        checkDistribution(tbl.getDefaultDistributionInfo());
        checkReplicaAllocation(tbl.getPartitionInfo());
    }

    public void checkDistribution(DistributionInfo distributionInfo) throws DdlException {
        if (distributionInfo instanceof HashDistributionInfo) {
            HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
            // buckets num
            if (info.getBucketNum() != bucketsNum) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_BUCKET_NUM, bucketsNum);
            }
            // distribution col size
            if (info.getDistributionColumns().size() != distributionColTypes.size()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_DISTRIBUTION_COLUMN_SIZE,
                        distributionColTypes.size());
            }
            // distribution col type
            for (int i = 0; i < distributionColTypes.size(); i++) {
                Type targetColType = distributionColTypes.get(i);
                if (!targetColType.equals(info.getDistributionColumns().get(i).getType())) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_DISTRIBUTION_COLUMN_TYPE,
                            info.getDistributionColumns().get(i).getName(), targetColType);
                }
            }
        }
    }

    public void checkReplicaAllocation(PartitionInfo partitionInfo) throws DdlException {
        for (ReplicaAllocation allocation : partitionInfo.idToReplicationAllocation.values()) {
            if (!replicaAllocation.equals(allocation)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_REPLICATION_NUM, replicationNum);
            }
        }
    }

    public static ColocateGroupSchema read(DataInput in) throws IOException {
        ColocateGroupSchema schema = new ColocateGroupSchema();
        schema.readFields(in);
        return schema;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        groupId.write(out);
        out.writeInt(distributionColTypes.size());
        for (Type type : distributionColTypes) {
            ColumnType.write(out, type);
        }
        out.writeInt(bucketsNum);
        replicaAllocation.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        groupId = GroupId.read(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            distributionColTypes.add(ColumnType.read(in));
        }
        bucketsNum = in.readInt();
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_66) {
            replicationNum = in.readShort();
        } else {
            replicaAllocation = ReplicaAllocation.read(in);
        }
    }

    public void convertToTagSystem() {
        if (isTagSystemConverted) {
            return;
        }
        replicaAllocation = new ReplicaAllocation();
        TagSet tagSet = TagSet.copyFrom(Backend.DEFAULT_TAG_SET);
        replicaAllocation.setReplica(AllocationType.LOCAL, tagSet, replicationNum);
        replicationNum = 0;
        isTagSystemConverted = true;
    }
}
