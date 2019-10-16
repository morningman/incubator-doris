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

import java.util.Map;

public class AddFrontendClause extends FrontendClause {
    public AddFrontendClause(String hostPort, FrontendNodeType type, Map<String, String> properties) {
        super(hostPort, type, properties);
    }

    @Override
    protected void checkProperties() throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return;
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER CLUSTER ADD " + role.name() + " \"");
        sb.append(hostPort).append("\"");
        return sb.toString();
    }
}
