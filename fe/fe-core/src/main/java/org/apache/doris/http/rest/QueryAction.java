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

package org.apache.doris.http.rest;

import org.apache.doris.common.DdlException;
import org.apache.doris.http.entity.ResponseEntityBuilder;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.submitter.QueryResultSet;
import org.apache.doris.qe.submitter.SQLSubmitter;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * For query via http
 */
@RestController
public class QueryAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(QueryAction.class);
    private static SQLSubmitter sqlSubmitter = new SQLSubmitter();

    private static final String PARAM_SYNC = "sync";
    private static final String PARAM_LIMIT = "limit";

    private static final long DEFAULT_ROW_LIMIT = 1000;
    private static final long MAX_ROW_LIMIT = 10000;

    /**
     * Execute a SQL.
     * Request body:
     * {
     *     "sql" : "select * from tbl1",
     *     "variables": {
     *         "exec_mem_limit" : 2147483648
     *     }
     * }
     */
    @RequestMapping(path = "/api/query/{" + NS_KEY + "}/{" + DB_KEY + "}", method = {RequestMethod.POST})
    public Object exeuteSQL(
            @PathVariable(value = NS_KEY) String ns,
            @PathVariable(value = DB_KEY) String dbName,
            HttpServletRequest request, HttpServletResponse response,
            @RequestBody String sqlBody) throws DdlException {
        ActionAuthorizationInfo authInfo = checkWithCookie(request, response, false);

        if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            return ResponseEntityBuilder.badRequest("Only support 'default_cluster' now");
        }

        boolean isSync = true;
        String syncParam = request.getParameter(PARAM_SYNC);
        if (!Strings.isNullOrEmpty(syncParam)) {
            isSync = syncParam.equals("1");
        }

        String limitParam = request.getParameter(PARAM_LIMIT);
        long limit = DEFAULT_ROW_LIMIT;
        if (!Strings.isNullOrEmpty(limitParam)) {
            limit = Math.min(Long.valueOf(limitParam), MAX_ROW_LIMIT);
        }

        Type type = new TypeToken<QueryRequestBody>() {
        }.getType();
        QueryRequestBody queryRequestBody = new Gson().fromJson(sqlBody, type);

        LOG.info("sql: {}", queryRequestBody.sql);
        LOG.info("var: {}", queryRequestBody.variables);

        ConnectContext.get().setDatabase(getFullDbName(dbName));
        // 1. Set session variable
        setSessionVariables(queryRequestBody.variables);
        // 2. Submit SQL
        SQLSubmitter.SQLQueryContext queryCtx = new SQLSubmitter.SQLQueryContext(
                queryRequestBody.sql, authInfo.fullUserName, authInfo.password, limit
        );
        Future<QueryResultSet> future = sqlSubmitter.submit(queryCtx);

        if (isSync) {
            try {
                QueryResultSet resultSet = future.get();
                return ResponseEntityBuilder.ok(resultSet.getResult());
            } catch (InterruptedException e) {
                LOG.warn("failed to execute query", e);
                return ResponseEntityBuilder.okWithCommonError("Failed to execute sql: " + e.getMessage());
            } catch (ExecutionException e) {
                LOG.warn("failed to execute query", e);
                return ResponseEntityBuilder.okWithCommonError("Failed to execute sql: " + e.getMessage());
            }
        } else {
            return ResponseEntityBuilder.okWithCommonError("Not support async query execution");
        }
    }

    private void setSessionVariables(Map<String, String> variables) {
    }

    private static class QueryRequestBody {
        public String sql;
        public Map<String, String> variables;
    }
}
