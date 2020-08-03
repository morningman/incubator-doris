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

import io.netty.handler.codec.http.HttpHeaderNames;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class LoadController extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(LoadController.class);

    public static final String SUB_LABEL_NAME_PARAM = "sub_label";

    private ExecuteEnv execEnv = ExecuteEnv.getInstance();

    private boolean isStreamLoad = false;

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_load", method = RequestMethod.PUT)
    public Object load(HttpServletRequest request, HttpServletResponse response,
                       @PathVariable(value = DB_KEY) String db, @PathVariable(value = TABLE_KEY) String table)
            throws DdlException {
        this.isStreamLoad = false;
        executeCheckPassword(request, response);
        return executeWithoutPassword(request, response, db, table);
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_stream_load", method = RequestMethod.PUT)
    public Object streamLoad(HttpServletRequest request,
                             HttpServletResponse response,
                             @PathVariable(value = DB_KEY) String db, @PathVariable(value = TABLE_KEY) String table) {
        this.isStreamLoad = true;
        executeCheckPassword(request, response);
        return executeWithoutPassword(request, response, db, table);
    }

    private Object executeWithoutPassword(HttpServletRequest request,
                                          HttpServletResponse response, String db, String table) {
        String dbName = db;
        String tableName = table;
        String urlStr = request.getRequestURI();
        // A 'Load' request must have 100-continue header
        if (request.getHeader(HttpHeaderNames.EXPECT.toString()) == null) {
            return ResponseEntityBuilder.notFound("There is no 100-continue header");
        }

        final String clusterName = ConnectContext.get().getClusterName();
        if (Strings.isNullOrEmpty(clusterName)) {
            return ResponseEntityBuilder.badRequest("No cluster selected.");
        }

        if (Strings.isNullOrEmpty(dbName)) {
            return ResponseEntityBuilder.badRequest("No database selected.");
        }

        if (Strings.isNullOrEmpty(tableName)) {
            return ResponseEntityBuilder.badRequest("No table selected.");
        }

        String fullDbName = ClusterNamespace.getFullName(clusterName, dbName);

        String label = request.getParameter(LABEL_KEY);
        if (isStreamLoad) {
            label = request.getHeader(LABEL_KEY);
        }

        if (Strings.isNullOrEmpty(label)) {
            return ResponseEntityBuilder.badRequest("No label selected.");
        }

        // check auth
        checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tableName, PrivPredicate.LOAD);

        if (!isStreamLoad && !Strings.isNullOrEmpty(request.getParameter(SUB_LABEL_NAME_PARAM))) {
            // only multi mini load need to redirect to Master, because only Master has the info of table to
            // the Backend which the file exists.
            RedirectView redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }
        }

        // Choose a backend sequentially.
        List<Long> backendIds = Catalog.getCurrentSystemInfo().seqChooseBackendIds(1, true, false, clusterName);
        if (backendIds == null) {
            return ResponseEntityBuilder.okWithCommonError("No backend alive.");
        }

        Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendIds.get(0));
        if (backend == null) {
            return ResponseEntityBuilder.okWithCommonError("No backend alive.");
        }

        TNetworkAddress redirectAddr = new TNetworkAddress(backend.getHost(), backend.getHttpPort());

        if (!isStreamLoad) {
            String subLabel = request.getParameter(SUB_LABEL_NAME_PARAM);
            if (!Strings.isNullOrEmpty(subLabel)) {
                try {
                    redirectAddr = execEnv.getMultiLoadMgr().redirectAddr(fullDbName, label, tableName, redirectAddr);
                } catch (DdlException e) {
                    return ResponseEntityBuilder.okWithCommonError(e.getMessage());
                }
            }
        }

        LOG.info("redirect load action to destination={}, stream: {}, db: {}, tbl: {}, label: {}",
                redirectAddr.toString(), isStreamLoad, dbName, tableName, label);

        RedirectView redirectView = redirectTo(request, redirectAddr);
        return redirectView;
    }


}
