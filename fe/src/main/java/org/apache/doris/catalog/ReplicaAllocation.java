package org.apache.doris.catalog;

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.resource.TagSet;
import org.apache.doris.system.Backend;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class ReplicaAllocation implements Writable {

    private static final ReplicaAllocation DEFAULT_ALLOCATION;
    static {
        DEFAULT_ALLOCATION = new ReplicaAllocation();
        TagSet tagSet = TagSet.copyFrom(Backend.DEFAULT_TAG_SET);
        DEFAULT_ALLOCATION.setReplica(AllocationType.LOCAL, tagSet, FeConstants.default_replication_num);
    }

    public enum AllocationType {
        LOCAL, REMOTE
    }

    private Table<AllocationType, TagSet, Short> typeToTag = HashBasedTable.create();

    public static ReplicaAllocation createDefault() {
        return new ReplicaAllocation(DEFAULT_ALLOCATION);
    }

    public ReplicaAllocation() {

    }

    public ReplicaAllocation(ReplicaAllocation other) {
        typeToTag = HashBasedTable.create(other.typeToTag);
    }

    public void setReplica(AllocationType type, TagSet tagSet, short num) {
        typeToTag.put(type, tagSet, num);
    }

    public short getReplicaNumByType(AllocationType type) {
        if (!typeToTag.containsRow(type)) {
            return 0;
        }
        return (short) typeToTag.row(type).values().stream().mapToInt(Short::valueOf).sum();
    }

    public short getReplicaNum() {
        return (short) typeToTag.values().stream().mapToInt(Short::valueOf).sum();
    }

    public Map<TagSet, Short> getTagMapByType(AllocationType type) {
        if (!typeToTag.containsRow(type)) {
            return Maps.newHashMap();
        }
        return Maps.newHashMap(typeToTag.row(type));
    }

    public static ReplicaAllocation read(DataInput in) throws IOException {
        ReplicaAllocation allocation = new ReplicaAllocation();
        allocation.readFields(in);
        return allocation;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(typeToTag.rowKeySet().size());
        for (AllocationType type : typeToTag.rowKeySet()) {
            Text.writeString(out, type.name());
            Map<TagSet, Short> map = typeToTag.row(type);
            out.writeInt(map.size());
            for (Map.Entry<TagSet, Short> entry : map.entrySet()) {
                entry.getKey().write(out);
                out.writeShort(entry.getValue());
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            AllocationType type = AllocationType.valueOf(Text.readString(in));
            int num = in.readInt();
            for (int j = 0; j < num; j++) {
                TagSet tagSet = TagSet.read(in);
                short replicaNum = in.readShort();
                typeToTag.put(type, tagSet, replicaNum);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        // TODO
        return sb.toString();
    }

    public boolean isSameAlloc(ReplicaAllocation allocation) {
        return typeToTag.equals(allocation.typeToTag);
    }
}
