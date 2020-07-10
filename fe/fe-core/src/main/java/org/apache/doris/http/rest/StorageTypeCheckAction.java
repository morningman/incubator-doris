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
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TStorageType;

import com.google.common.base.Strings;

import org.json.JSONObject;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class StorageTypeCheckAction extends RestBaseController {


    @RequestMapping(path = "/api/_check_storagetype",method = RequestMethod.GET)
    protected Object check_storagetype(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        executeCheckPassword(request,response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");

        String dbName = request.getParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No database selected");
            return entity;
        }

        String fullDbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(), dbName);
        Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
        if (db == null) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("Database " + dbName + " does not exist");
            return entity;
        }

        JSONObject root = new JSONObject();
        db.readLock();
        try {
            List<Table> tbls = db.getTables();
            for (Table tbl : tbls) {
                if (tbl.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTbl = (OlapTable) tbl;
                JSONObject indexObj = new JSONObject();
                for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTbl.getIndexIdToMeta().entrySet()) {
                    MaterializedIndexMeta indexMeta = entry.getValue();
                    if (indexMeta.getStorageType() == TStorageType.ROW) {
                        indexObj.put(olapTbl.getIndexNameById(entry.getKey()), indexMeta.getStorageType().name());
                    }
                }
                root.put(tbl.getName(), indexObj);
            }
        } finally {
            db.readUnlock();
        }
        entity.setData(root);
        return entity;
    }
}
