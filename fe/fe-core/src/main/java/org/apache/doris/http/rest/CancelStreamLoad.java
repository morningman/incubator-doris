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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import com.google.common.base.Strings;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * To cancel a load transaction with given load label
 */
@RestController
public class CancelStreamLoad extends RestBaseController{

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_cancel", method = RequestMethod.POST)
    public Object execute(@PathVariable(value = DB_KEY) final String dbName,
                          HttpServletRequest request, HttpServletResponse response) throws DdlException {
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build();
        executeCheckPassword(request, response);

        RedirectView redirectView = redirectToMaster(request, response);
        if (redirectView != null) {
            return redirectView;
        }

        if (Strings.isNullOrEmpty(dbName)) {
            entity.setMsgWithCode("No database selected", RestApiStatusCode.COMMON_ERROR);
            return entity;
        }

        String fullDbName = getFullDbName(dbName);

        String label = request.getParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            entity.setMsgWithCode("No label specified", RestApiStatusCode.COMMON_ERROR);
            return entity;
        }

        Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
        if (db == null) {
            entity.setMsgWithCode("unknown database, database=" + dbName, RestApiStatusCode.COMMON_ERROR);
            return entity;
        }

        // TODO(cmy): Currently we only check priv in db level.
        // Should check priv in table level.
        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), fullDbName, PrivPredicate.LOAD)) {
            entity.setMsgWithCode("Access denied for user '" + ConnectContext.get().getQualifiedUser()
                    + "' to database '" + fullDbName + "'", RestApiStatusCode.COMMON_ERROR);
        }

        try {
            Catalog.getCurrentGlobalTransactionMgr().abortTransaction(db.getId(), label, "user cancel");
        } catch (UserException e) {
            entity.setMsgWithCode(e.getMessage(), RestApiStatusCode.COMMON_ERROR);
        }

        return  entity;
    }

}
