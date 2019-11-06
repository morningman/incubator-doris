package org.apache.doris.catalog;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.resource.TagSet;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class ReplicaAllocation implements Writable {
    public enum AllocationType {
        LOCAL, REMOTE
    }

    private Map<AllocationType, Short> typeToNum = Maps.newHashMap();
    private Map<AllocationType, TagSet> typeToTag = Maps.newHashMap();

    public ReplicaAllocation() {

    }

    public void setReplica(AllocationType type, TagSet tagSet, short num) {
        typeToNum.put(type, num);
        typeToTag.put(type, tagSet);
    }

    public short getReplicaNumByType(AllocationType type) {
        return typeToNum.getOrDefault(type, (short) 0);
    }

    public static ReplicaAllocation read(DataInput in) throws IOException {
        ReplicaAllocation allocation = new ReplicaAllocation();
        allocation.readFields(in);
        return allocation;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(typeToNum.size());
        for (AllocationType type : typeToNum.keySet()) {
            Text.writeString(out, type.name());
            out.writeShort(typeToNum.get(type));
            typeToTag.get(type).write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            AllocationType type = AllocationType.valueOf(Text.readString(in));
            short num = in.readShort();
            TagSet tagSet = TagSet.read(in);
            typeToNum.put(type, num);
            typeToTag.put(type, tagSet);
        }

    }
}
