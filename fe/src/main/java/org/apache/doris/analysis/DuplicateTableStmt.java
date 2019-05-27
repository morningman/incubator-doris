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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;

// DUPLICATE TABLE tbl [PARTITION(p1, p2, ...)] TO new_tbl;
public class DuplicateTableStmt extends DdlStmt {

    private TableRef tblRef;
    private String newTblName;

    public DuplicateTableStmt(TableRef tblRef, String newTblName) {
        this.tblRef = tblRef;
        this.newTblName = newTblName;
    }

    public TableRef getTblRef() {
        return tblRef;
    }

    public String getNewTblName() {
        return newTblName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        tblRef.getName().analyze(analyzer);

        if (tblRef.hasExplicitAlias()) {
            throw new AnalysisException("Not support duplicate table with alias");
        }

        // check access
        // it requires SHOW privilege
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tblRef.getName().getDb(),
                tblRef.getName().getTbl(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SHOW");
        }

        FeNameFormat.checkTableName(newTblName);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DUPLICATE TABLE ");
        sb.append(tblRef.getName().toSql());
        if (tblRef.getPartitions() != null && !tblRef.getPartitions().isEmpty()) {
            sb.append(" PARTITION (");
            sb.append(Joiner.on(", ").join(tblRef.getPartitions()));
            sb.append(")");
        }
        sb.append(" TO ").append(newTblName);
        return sb.toString();
    }

}
