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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.TagUtil;
import org.apache.doris.resource.TagSet;
import org.apache.doris.system.Backend;

import java.util.List;
import java.util.Map;

public class AddBackendClause extends BackendClause {
    private static final String PROP_TAG_PREFIX = "tag.";

    // following members are generated after analyzing
    private TagSet tagSet = Backend.DEFAULT_TAG_SET;

    public AddBackendClause(List<String> hostPorts, Map<String, String> properties) {
        super(hostPorts, properties);
    }

    public TagSet getTagSet() {
        return tagSet;
    }

    @Override
    protected void checkProperties() throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return;
        }
        
        TagSet userTagSet = TagUtil.analyzeTagProperties(properties);
        // use user specified tags to substitute the default tag of Backend
        tagSet.substituteMerge(userTagSet);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD ");
        sb.append("BACKEND ");
        for (int i = 0; i < hostPorts.size(); i++) {
            sb.append("\"").append(hostPorts.get(i)).append("\"");
            if (i != hostPorts.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
