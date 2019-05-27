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
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

// clause which is used to exchange partition 
public class ExchangePartitionClause extends AlterClause {

    private String partitionName;
    private TableName dbTbl;

    public String getPartitionName() {
        return partitionName;
    }

    public String getDbName() {
        return dbTbl.getDb();
    }

    public String getTblName() {
        return dbTbl.getTbl();
    }

    public ExchangePartitionClause(String partitionName, TableName dbTbl) {
        this.partitionName = partitionName;
        this.dbTbl = dbTbl;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(partitionName)) {
            throw new AnalysisException("Partition name is not set");
        }

        dbTbl.analyze(analyzer);

        // check the source table's privilege
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbTbl.getDb(), dbTbl.getTbl(),
                PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER TABLE",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(), dbTbl.getTbl());
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("EXCHANGE PARTITION ");
        sb.append(partitionName);
        sb.append(" WITH ").append(dbTbl.toSql());
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
