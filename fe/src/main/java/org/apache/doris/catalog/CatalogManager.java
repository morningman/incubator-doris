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

package org.apache.doris.catalog;

import org.apache.doris.analysis.DuplicateTableStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.persist.CreateTableInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * Author: Chenmingyu
 * Date: May 27, 2019
 */

public class CatalogManager {
    private static final Logger LOG = LogManager.getLogger(CatalogManager.class);
    
    private Catalog catalog;

    public CatalogManager(Catalog catalog) {
        this.catalog = catalog;
    }
    
    /*
     * Duplicate a specified table
     * This will duplicate a new table with same schema as specified one, without any data in it.
     */
    public void duplicateTable(DuplicateTableStmt duplicateTableStmt) throws DdlException {
        TableRef tblRef = duplicateTableStmt.getTblRef();
        TableName dbTbl = tblRef.getName();
        String newTblName = duplicateTableStmt.getNewTblName();

        // check, and save some info which need to be checked again later
        Map<String, Long> origPartitions = Maps.newHashMap();
        OlapTable copiedTbl = null;
        Database db = catalog.getDb(dbTbl.getDb());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbTbl.getDb());
        }

        db.readLock();
        try {
            Table table = db.getTable(dbTbl.getTbl());
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, dbTbl.getTbl());
            }

            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Only support truncate OLAP table");
            }

            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getState() != OlapTableState.NORMAL) {
                throw new DdlException("Table' state is not NORMAL: " + olapTable.getState());
            }
            
            // check new table name
            if (db.getTable(newTblName) != null) {
                throw new DdlException("Table " + newTblName + " already exist");
            }

            if (tblRef.getPartitions() != null && !tblRef.getPartitions().isEmpty()) {
                for (String partName: tblRef.getPartitions()) {
                    Partition partition = olapTable.getPartition(partName);
                    if (partition == null) {
                        throw new DdlException("Partition " + partName + " does not exist");
                    }
                    
                    origPartitions.put(partName, partition.getId());
                }
            } else {
                for (Partition partition : olapTable.getPartitions()) {
                    origPartitions.put(partition.getName(), partition.getId());
                }
            }
            
            copiedTbl = olapTable.selectiveCopy(origPartitions.keySet(), true);
            Map<String, Short> partRepNum = Maps.newHashMap();
            for (Partition partition : copiedTbl.getPartitions()) {
                partRepNum.put(partition.getName(), copiedTbl.getPartitionInfo().getReplicationNum(partition.getId()));
            }
            
            copiedTbl.resetIdsForRestore(catalog, db, -1, partRepNum);
        } finally {
            db.readUnlock();
        }
        
        // 2. use the copied table to create partitions
        List<Partition> newPartitions = Lists.newArrayList();
        // tabletIdSet to save all newly created tablet ids.
        Set<Long> tabletIdSet = Sets.newHashSet();
        try {
            for (Partition partition : copiedTbl.getPartitions()) {
                Partition newPartition = catalog.createPartitionWithIndices(db.getClusterName(),
                        db.getId(), copiedTbl.getId(), partition.getId(), partition.getName(),
                        copiedTbl.getIndexIdToShortKeyColumnCount(),
                        copiedTbl.getIndexIdToSchemaHash(),
                        copiedTbl.getIndexIdToStorageType(),
                        copiedTbl.getIndexIdToSchema(),
                        copiedTbl.getKeysType(),
                        copiedTbl.getDefaultDistributionInfo(),
                        copiedTbl.getPartitionInfo().getDataProperty(partition.getId()).getStorageMedium(),
                        copiedTbl.getPartitionInfo().getReplicationNum(partition.getId()),
                        null /* version info */,
                        copiedTbl.getCopiedBfColumns(),
                        copiedTbl.getBfFpp(),
                        tabletIdSet,
                        false /* not restore */);
                newPartitions.add(newPartition);
            }
        } catch (DdlException e) {
            // create partition failed, remove all newly created tablets
            for (Long tabletId : tabletIdSet) {
                Catalog.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
            throw e;
        }
        Preconditions.checkState(copiedTbl.getPartitions().size() == newPartitions.size());

        // all partitions are created successfully, try to replace the old partitions.
        db.writeLock();
        try {
            // check new table name again
            if (db.getTable(newTblName) != null) {
                throw new DdlException("Table " + newTblName + " already exist");
            }

            // use new partitions to replace the old ones.
            for (Partition newPartition : newPartitions) {
                copiedTbl.replacePartition(newPartition);
            }

            // must set table name after replace partition. because set name may change the partition name
            // if table is unpartitioned
            copiedTbl.setName(newTblName);

            // add table to database
            db.createTable(copiedTbl);

            // write edit log
            // here we use the create table info edit log
            CreateTableInfo info = new CreateTableInfo(db.getFullName(), copiedTbl);
            catalog.getEditLog().logCreateTable(info);
        } finally {
            db.writeUnlock();
        }
        
        LOG.info("finished to duplicate table {}, partitions: {}, new table: {}",
                tblRef.getName().toSql(), tblRef.getPartitions(), newTblName);
    }
}
