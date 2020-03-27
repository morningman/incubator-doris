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

package org.apache.doris.plugin;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

public class AuditEvent {
    public enum EventType {
        CONNECTION,
        DISCONNECTION,
        BEFORE_QUERY,
        AFTER_QUERY
    }

    @Retention(RetentionPolicy.RUNTIME)
    public static @interface AuditField {
        String value() default "";
    }

    private EventType type;

    @AuditField(value = "Timestamp")
    private long timestamp = -1;
    @AuditField(value = "Client")
    private String clientIp = "";
    @AuditField(value = "User")
    private String user = "";
    @AuditField(value = "Db")
    private String db = "";
    @AuditField(value = "State")
    private String state = "";
    @AuditField(value = "Time")
    private long queryTime = -1;
    @AuditField(value = "ScanBytes")
    private long scanBytes = -1;
    @AuditField(value = "ScanRows")
    private long scanRows = -1;
    @AuditField(value = "ReturnRows")
    private long returnRows = -1;
    @AuditField(value = "StmtId")
    private long stmtId = -1;
    @AuditField(value = "QueryId")
    private String queryId = "";
    @AuditField(value = "IsQuery")
    private boolean isQuery = false;
    @AuditField(value = "Stmt")
    private String stmt = "";

    private AuditEvent() {

    }

    public static class AuditEventBuilder {

        private AuditEvent auditEvent = new AuditEvent();

        public AuditEventBuilder() {
        }

        public void reset() {
            auditEvent = new AuditEvent();
        }

        public AuditEventBuilder setEventType(EventType eventType) {
            auditEvent.type = eventType;
            return this;
        }

        public AuditEventBuilder setTimestamp(long timestamp) {
            auditEvent.timestamp = timestamp;
            return this;
        }

        public AuditEventBuilder setClientIp(String clientIp) {
            auditEvent.clientIp = clientIp;
            return this;
        }

        public AuditEventBuilder setUser(String user) {
            auditEvent.user = user;
            return this;
        }

        public AuditEventBuilder setDb(String db) {
            auditEvent.db = db;
            return this;
        }

        public AuditEventBuilder setState(String state) {
            auditEvent.state = state;
            return this;
        }

        public AuditEventBuilder setQueryTime(long queryTime) {
            auditEvent.queryTime = queryTime;
            return this;
        }

        public AuditEventBuilder setScanBytes(long scanBytes) {
            auditEvent.scanBytes = scanBytes;
            return this;
        }

        public AuditEventBuilder setScanRows(long scanRows) {
            auditEvent.scanRows = scanRows;
            return this;
        }

        public AuditEventBuilder setReturnRows(long returnRows) {
            auditEvent.returnRows = returnRows;
            return this;
        }

        public AuditEventBuilder setStmtId(long stmtId) {
            auditEvent.stmtId = stmtId;
            return this;
        }

        public AuditEventBuilder setQueryId(String queryId) {
            auditEvent.queryId = queryId;
            return this;
        }

        public AuditEventBuilder setIsQuery(boolean isQuery) {
            auditEvent.isQuery = isQuery;
            return this;
        }

        public AuditEventBuilder setStmt(String stmt) {
            auditEvent.stmt = stmt;
            return this;
        }

        public AuditEvent build() {
            return this.auditEvent;
        }
    }

    public EventType getType() {
        return type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getClientIp() {
        return clientIp;
    }

    public String getUser() {
        return user;
    }

    public String getDb() {
        return db;
    }

    public String getState() {
        return state;
    }

    public long getQueryTime() {
        return queryTime;
    }

    public long getScanBytes() {
        return scanBytes;
    }

    public long getScanRows() {
        return scanRows;
    }

    public long getReturnRows() {
        return returnRows;
    }

    public long getStmtId() {
        return stmtId;
    }

    public String getQueryId() {
        return queryId;
    }

    public boolean isQuery() {
        return isQuery;
    }

    public String getStmt() {
        return stmt;
    }
}
