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

import com.google.gson.annotations.SerializedName;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/*  TableProperty contains additional information about OlapTable
 *  TableProperty includes properties to persistent the additional information
 *  Different properties is recognized by prefix such as dynamic_partition
 *  If there is different type properties is added.Write a method such as buildDynamicProperty to build it.
 */
public class TableProperty implements Writable {
    private static final String DYNAMIC_PARTITION_PROPERTY_PREFIX = "dynamic_partition";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    private DynamicPartitionProperty dynamicPartitionProperty;

    public TableProperty(Map<String, String> properties) {
        this.properties = properties;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public DynamicPartitionProperty getDynamicPartitionProperty() {
        return dynamicPartitionProperty;
    }

    public void buildDynamicProperty() {
        HashMap<String, String> dynamicPartitionProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(DYNAMIC_PARTITION_PROPERTY_PREFIX)) {
                dynamicPartitionProperties.put(entry.getKey(), entry.getValue());
            }
        }
        dynamicPartitionProperty = new DynamicPartitionProperty(dynamicPartitionProperties);
    }

    void modifyTableProperties(Map<String, String> modifyProperties) {
        properties.putAll(modifyProperties);
        buildDynamicProperty();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TableProperty read(DataInput in) throws IOException {
        TableProperty prop = GsonUtils.GSON.fromJson(Text.readString(in), TableProperty.class);
        prop.buildDynamicProperty();
        return prop;
    }
}
