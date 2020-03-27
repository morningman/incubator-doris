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

package org.apache.doris.qe;

import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginMgr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class AuditEventProcessor {
    private static final Logger LOG = LogManager.getLogger(AuditEventProcessor.class);

    private PluginMgr pluginMgr;

    private List<Plugin> auditPlugins;
    private long lastUpdateTime = 0;
    private static final long UPDATE_PLUGIN_INTERVAL_MS = 60 * 1000; // 1min

    public AuditEventProcessor(PluginMgr pluginMgr) {
        this.pluginMgr = pluginMgr;
    }

    public void handleAuditEvent(AuditEvent auditEvent) {
        // update audit plugin list
        if (auditPlugins == null || System.currentTimeMillis() - lastUpdateTime > UPDATE_PLUGIN_INTERVAL_MS) {
            auditPlugins = pluginMgr.getActivePluginList(PluginType.AUDIT);
        }
        for (Plugin plugin : auditPlugins) {
            if (((AuditPlugin) plugin).eventFilter(auditEvent.getType())) {
                ((AuditPlugin) plugin).exec(auditEvent);
            }
        }
    }
}
