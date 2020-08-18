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

package org.apache.doris.qe.submitter;


import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class SQLSubmitter {
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL_PATTERN = "jdbc:mysql://127.0.0.1:%d/%s";

    private ThreadPoolExecutor executor = ThreadPoolManager.newDaemonCacheThreadPool(2, "SQL submitter", true);

    public Future<QueryResultSet> submit(SQLQueryContext queryCtx) {
        Worker worker = new Worker(ConnectContext.get(), queryCtx);
        return executor.submit(worker);
    }

    private static class Worker implements Callable<QueryResultSet> {

        private ConnectContext ctx;
        private SQLQueryContext queryCtx;

        public Worker(ConnectContext ctx, SQLQueryContext queryCtx) {
            this.ctx = ctx;
            this.queryCtx = queryCtx;
        }

        @Override
        public QueryResultSet call() throws Exception {
            Connection conn = null;
            Statement stmt = null;
            String dbUrl = String.format(DB_URL_PATTERN, Config.query_port, ctx.getDatabase());
            try {
                Class.forName(JDBC_DRIVER);
                conn = DriverManager.getConnection(dbUrl, queryCtx.user, queryCtx.passwd);
                stmt = conn.prepareStatement(queryCtx.sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = stmt.executeQuery(queryCtx.sql);
                QueryResultSet resultSet = generateResult(rs);

                rs.close();
                stmt.close();
                conn.close();
                return resultSet;
            } finally {
                try {
                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException se2) {
                }
                try {
                    if (conn != null) conn.close();
                } catch (SQLException se) {
                    se.printStackTrace();
                }
            }
        }

        public QueryResultSet generateResult(ResultSet rs) throws SQLException {
            Map<String, Object> result = Maps.newHashMap();
            ResultSetMetaData metaData = rs.getMetaData();
            int colNum = metaData.getColumnCount();
            // 1. metadata
            List<Map<String, String>> metaFields = Lists.newArrayList();
            // index start from 1
            for (int i = 1; i <= colNum; ++i) {
                Map<String, String> field = Maps.newHashMap();
                field.put("name", metaData.getColumnName(i));
                field.put("type", metaData.getColumnTypeName(i));
                metaFields.add(field);
            }
            // 2. data
            List<List<Object>> rows = Lists.newArrayList();
            long rowCount = 0;
            while (rs.next() && rowCount < queryCtx.limit) {
                List<Object> row = Lists.newArrayListWithCapacity(colNum);
                // index start from 1
                for (int i = 1; i <= colNum; ++i) {
                    row.add(rs.getObject(i));
                }
                rows.add(row);
                rowCount++;
            }
            result.put("meta", metaFields);
            result.put("data", rows);
            return new QueryResultSet(result);
        }
    }

    public static class SQLQueryContext {
        public String sql;
        public String user;
        public String passwd;
        public long limit; // limit the number of rows returned by the query

        public SQLQueryContext(String sql, String user, String passwd, long limit) {
            this.sql = sql;
            this.user = user;
            this.passwd = passwd;
            this.limit = limit;
        }
    }
}
