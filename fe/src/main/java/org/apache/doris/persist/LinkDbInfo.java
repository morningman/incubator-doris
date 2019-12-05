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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LinkDbInfo implements Writable {
    private String cluster;
    private String name;
    private long id;

    public LinkDbInfo() {
        this.cluster = "";
        this.name = "";
    }

    public LinkDbInfo(String name, long id) {
        this.name = name;
        this.id = id;
        this.cluster = "";
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public static LinkDbInfo read(DataInput in) throws IOException {
        LinkDbInfo info = new LinkDbInfo();
        info.readFields(in);
        return info;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, cluster);
        Text.writeString(out, name);
        out.writeLong(id);

    }

    private void readFields(DataInput in) throws IOException {
        cluster = Text.readString(in);
        name = Text.readString(in);
        id = in.readLong();

    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }
}
