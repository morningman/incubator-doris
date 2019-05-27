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

package org.apache.doris.alter;

import org.apache.doris.analysis.AddColumnClause;
import org.apache.doris.analysis.AddColumnsClause;
import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.AddRollupClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.ColumnRenameClause;
import org.apache.doris.analysis.DropColumnClause;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.DropRollupClause;
import org.apache.doris.analysis.ExchangePartitionClause;
import org.apache.doris.analysis.ModifyColumnClause;
import org.apache.doris.analysis.ModifyPartitionClause;
import org.apache.doris.analysis.ModifyTablePropertiesClause;
import org.apache.doris.analysis.PartitionRenameClause;
import org.apache.doris.analysis.ReorderColumnsClause;
import org.apache.doris.analysis.RollupRenameClause;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRenameClause;
import org.apache.doris.backup.BackupHandler;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.persist.ExchangePartitionInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Alter {
    private static final Logger LOG = LogManager.getLogger(Alter.class);

    private AlterHandler schemaChangeHandler;
    private AlterHandler rollupHandler;
    private SystemHandler clusterHandler;

    public Alter() {
        schemaChangeHandler = new SchemaChangeHandler();
        rollupHandler = new RollupHandler();
        clusterHandler = new SystemHandler();
    }

    public void start() {
        schemaChangeHandler.start();
        rollupHandler.start();
        clusterHandler.start();
    }

    public void processAlterTable(AlterTableStmt stmt) throws DdlException {
        TableName dbTableName = stmt.getTbl();
        String dbName = dbTableName.getDb();
        final String clusterName = stmt.getClusterName();

        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // check cluster capacity
        Catalog.getCurrentSystemInfo().checkClusterCapacity(clusterName);

        // schema change ops can appear several in one alter stmt without other alter ops entry
        boolean hasSchemaChange = false;
        // rollup ops, if has, should appear one and only one entry
        boolean hasRollup = false;
        // partition ops, if has, should appear one and only one entry
        boolean hasPartition = false;
        // rename ops, if has, should appear one and only one entry
        boolean hasRename = false;
        // modify properties ops, if has, should appear one and only one entry
        boolean hasModifyProp = false;
        // exchange partition op,
        boolean hasExchangePartition = false;

        // check conflict alter ops first
        List<AlterClause> alterClauses = stmt.getOps();
        // check conflict alter ops first                 
        // if all alter clauses are DropPartitionClause, no need to call checkQuota.
        boolean allDropPartitionClause = true;
        
        for (AlterClause alterClause : alterClauses) {
            if (!(alterClause instanceof DropPartitionClause)) {
                allDropPartitionClause = false;
                break;
            }
        }

        if (!allDropPartitionClause) {
            // check db quota
            db.checkQuota();
        }

        for (AlterClause alterClause : alterClauses) {
            if ((alterClause instanceof AddColumnClause
                    || alterClause instanceof AddColumnsClause
                    || alterClause instanceof DropColumnClause
                    || alterClause instanceof ModifyColumnClause
                    || alterClause instanceof ReorderColumnsClause
                    || alterClause instanceof ModifyTablePropertiesClause)
                    && !hasRollup && !hasPartition && !hasRename && !hasExchangePartition) {
                hasSchemaChange = true;
            } else if (alterClause instanceof AddRollupClause && !hasSchemaChange && !hasRollup && !hasPartition
                    && !hasRename && !hasModifyProp && !hasExchangePartition) {
                hasRollup = true;
            } else if (alterClause instanceof DropRollupClause && !hasSchemaChange && !hasRollup && !hasPartition
                    && !hasRename && !hasModifyProp && !hasExchangePartition) {
                hasRollup = true;
            } else if (alterClause instanceof AddPartitionClause && !hasSchemaChange && !hasRollup && !hasPartition
                    && !hasRename && !hasModifyProp && !hasExchangePartition) {
                hasPartition = true;
            } else if (alterClause instanceof DropPartitionClause && !hasSchemaChange && !hasRollup && !hasPartition
                    && !hasRename && !hasModifyProp && !hasExchangePartition) {
                hasPartition = true;
            } else if (alterClause instanceof ModifyPartitionClause && !hasSchemaChange && !hasRollup
                    && !hasPartition && !hasRename && !hasModifyProp && !hasExchangePartition) {
                hasPartition = true;
            } else if ((alterClause instanceof TableRenameClause || alterClause instanceof RollupRenameClause
                    || alterClause instanceof PartitionRenameClause || alterClause instanceof ColumnRenameClause)
                    && !hasSchemaChange && !hasRollup && !hasPartition && !hasRename && !hasModifyProp
                    && !hasExchangePartition) {
                hasRename = true;
            } else if (alterClause instanceof ModifyTablePropertiesClause && !hasSchemaChange && !hasRollup
                    && !hasPartition && !hasRename && !hasModifyProp && !hasExchangePartition) {
                hasModifyProp = true;
            } else if (alterClause instanceof ExchangePartitionClause && !hasSchemaChange && !hasRollup
                    && !hasPartition && !hasRename && !hasModifyProp) {
                hasExchangePartition = true;
            } else {
                throw new DdlException("Conflicting alter clauses. see help for more information");
            }
        } // end for alter clauses

        boolean hasAddPartition = false;
        String tableName = dbTableName.getTbl();
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Do not support alter non-OLAP table[" + tableName + "]");
            }

            OlapTable olapTable = (OlapTable) table;

            if (olapTable.getPartitions().size() == 0) {
                throw new DdlException("table with empty parition cannot do schema change. [" + tableName + "]");
            }

            if (olapTable.getState() == OlapTableState.SCHEMA_CHANGE
                    || olapTable.getState() == OlapTableState.RESTORE) {
                throw new DdlException("Table[" + table.getName() + "]'s state[" + olapTable.getState()
                        + "] does not allow doing ALTER ops");
                // here we pass NORMAL and ROLLUP
                // NORMAL: ok to do any alter ops
                // ROLLUP: we allow user DROP a rollup index when it's under ROLLUP
            }
            
            if (hasSchemaChange || hasModifyProp || hasRollup) {
                // check if all tablets are healthy, and no tablet is in tablet scheduler
                boolean isStable = olapTable.isStable(Catalog.getCurrentSystemInfo(),
                        Catalog.getCurrentCatalog().getTabletScheduler(),
                        db.getClusterName());
                if (!isStable) {
                    throw new DdlException("Table [" + olapTable.getName() + "] is not stable."
                            + " Some tablets of this table may not be healthy or are being scheduled."
                            + " You need to repair the table first"
                            + " or stop cluster balance. See 'help admin;'.");
                }
            }

            if (hasSchemaChange || hasModifyProp) {
                schemaChangeHandler.process(alterClauses, clusterName, db, olapTable);
            } else if (hasRollup) {
                rollupHandler.process(alterClauses, clusterName, db, olapTable);
            } else if (hasPartition) {
                Preconditions.checkState(alterClauses.size() == 1);
                AlterClause alterClause = alterClauses.get(0);
                if (alterClause instanceof DropPartitionClause) {
                    Catalog.getInstance().dropPartition(db, olapTable, ((DropPartitionClause) alterClause));
                } else if (alterClause instanceof ModifyPartitionClause) {
                    Catalog.getInstance().modifyPartition(db, olapTable, ((ModifyPartitionClause) alterClause));
                } else {
                    hasAddPartition = true;
                }
            } else if (hasRename) {
                processRename(db, olapTable, alterClauses);
            }
        } finally {
            db.writeUnlock();
        }

        if (hasAddPartition) {
            // add partition op should done outside db lock. cause it contain synchronized create operation
            Preconditions.checkState(alterClauses.size() == 1);
            AlterClause alterClause = alterClauses.get(0);
            if (alterClause instanceof AddPartitionClause) {
                Catalog.getInstance().addPartition(db, tableName, (AddPartitionClause) alterClause);
            } else {
                Preconditions.checkState(false);
            }
        } else if (hasExchangePartition) {
            // exchange partition should relock the databases in order
            processExchangePartition(db, tableName, alterClauses);
        }
    }

    public void processAlterCluster(AlterSystemStmt stmt) throws DdlException {
        clusterHandler.process(Arrays.asList(stmt.getAlterClause()), stmt.getClusterName(), null, null);
    }

    private void processRename(Database db, OlapTable table, List<AlterClause> alterClauses) throws DdlException {
        for (AlterClause alterClause : alterClauses) {
            if (alterClause instanceof TableRenameClause) {
                Catalog.getInstance().renameTable(db, table, (TableRenameClause) alterClause);
                break;
            } else if (alterClause instanceof RollupRenameClause) {
                Catalog.getInstance().renameRollup(db, table, (RollupRenameClause) alterClause);
                break;
            } else if (alterClause instanceof PartitionRenameClause) {
                Catalog.getInstance().renamePartition(db, table, (PartitionRenameClause) alterClause);
                break;
            } else if (alterClause instanceof ColumnRenameClause) {
                Catalog.getInstance().renameColumn(db, table, (ColumnRenameClause) alterClause);
                break;
            } else {
                Preconditions.checkState(false);
            }
        }
    }

    private void processExchangePartition(Database destDb, String destTblName, List<AlterClause> alterClauses)
            throws DdlException {

        // get and check database
        TreeMap<String, Database> dbs = Maps.newTreeMap();
        dbs.put(destDb.getFullName(), destDb);
        for (AlterClause alterClause : alterClauses) {
            if (alterClause instanceof ExchangePartitionClause) {
                ExchangePartitionClause clause = (ExchangePartitionClause) alterClause;
                String dbName = clause.getDbName();
                Database srcDb = Catalog.getCurrentCatalog().getDb(dbName);
                if (srcDb == null) {
                    throw new DdlException("Database " + dbName + " does not exist");
                }
                dbs.put(dbName, srcDb);
            }
        }

        // lock database in order
        for (Database db : dbs.values()) {
            db.writeLock();
        }
        try {
            // get dest table
            Table destTbl = destDb.getTable(destTblName);
            if (destTbl == null || destTbl.getType() != TableType.OLAP) {
                throw new DdlException("Table " + destTblName + " does not exist or is not OLAP table");
            }
            OlapTable destOlapTbl = (OlapTable) destTbl;

            // check if all tablets are healthy, and no tablet is in tablet scheduler
            boolean isStable = destOlapTbl.isStable(Catalog.getCurrentSystemInfo(),
                    Catalog.getCurrentCatalog().getTabletScheduler(),
                    destDb.getClusterName());
            if (!isStable) {
                throw new DdlException("Table [" + destOlapTbl.getName() + "] is not stable."
                        + " Some tablets of this table may not be healthy or are being scheduled."
                        + " You need to repair the table first"
                        + " or stop cluster balance. See 'help admin;'.");
            }

            // check all src tables
            Map<String, OlapTable> partNameToSrcTbl = Maps.newHashMap();
            Map<String, Long> partNameToSrcDbId = Maps.newHashMap();
            for (AlterClause alterClause : alterClauses) {
                if (alterClause instanceof ExchangePartitionClause) {
                    ExchangePartitionClause clause = (ExchangePartitionClause) alterClause;
                    Database db = Catalog.getCurrentCatalog().getDb(clause.getDbName());
                    Table srcTbl = db.getTable(clause.getTblName());
                    if (srcTbl == null || srcTbl.getType() != TableType.OLAP) {
                        throw new DdlException("Table " + clause.getTblName() + " does not exist or is not OLAP table");
                    }

                    if (partNameToSrcTbl.containsKey(clause.getPartitionName())) {
                        throw new DdlException("Duplicated exchanging partition: " + clause.getPartitionName());
                    }

                    OlapTable srcOlapTbl = (OlapTable) srcTbl;

                    if (!srcOlapTbl.isStable(Catalog.getCurrentSystemInfo(),
                            Catalog.getCurrentCatalog().getTabletScheduler(),
                            db.getClusterName())) {
                        throw new DdlException("Table [" + srcOlapTbl.getName() + "] is not stable."
                                + " Some tablets of this table may not be healthy or are being scheduled."
                                + " You need to repair the table first"
                                + " or stop cluster balance. See 'help admin;'.");
                    }

                    int srcSig = srcOlapTbl.getSignature(BackupHandler.SIGNATURE_VERSION, Lists.newArrayList(clause.getPartitionName()), true, true);
                    int destSig = destOlapTbl.getSignature(BackupHandler.SIGNATURE_VERSION, Lists.newArrayList(clause.getPartitionName()), true, true);
                    if (srcSig == -1 || destSig == -1) {
                        throw new DdlException("Partition " + clause.getPartitionName() + " does not exist");
                    }
                    if (srcSig != destSig) {
                        throw new DdlException("Table with different schema: " + clause.getTblName());
                    }

                    partNameToSrcTbl.put(clause.getPartitionName(), srcOlapTbl);
                    partNameToSrcDbId.put(clause.getPartitionName(), db.getId());
                }
            }

            // exchange partitions
            for (Map.Entry<String, OlapTable> entry : partNameToSrcTbl.entrySet()) {
                OlapTable srcTbl = entry.getValue();
                srcTbl.exchangePartition(partNameToSrcDbId.get(entry.getKey()), destDb.getId(), destOlapTbl, entry.getKey());
            }

            // write edit log
            ExchangePartitionInfo info = new ExchangePartitionInfo(destDb.getId(), destTbl.getId(),
                    partNameToSrcTbl, partNameToSrcDbId);
            Catalog.getCurrentCatalog().getEditLog().logExchangePartition(info);

            LOG.info("finished to exchange partition for table: " + destTblName);

        } finally {
            for (Database db : dbs.descendingMap().values()) {
                db.writeUnlock();
            }
        }
    }

    public void replayExchangePartition(ExchangePartitionInfo info) {
        long destDbId = info.getDestDbId();
        long destTblId = info.getDestTblId();
        Map<String, Long> partNameToSrcDbId = info.getPartNameToSrcDbId();
        Map<String, Long> partNameToSrcTblId = info.getPartNameToSrcTblId();

        TreeMap<String, Database> dbs = Maps.newTreeMap();
        Database destDb = Catalog.getCurrentCatalog().getDb(destDbId);
        dbs.put(destDb.getFullName(), destDb);
        for (long dbId : partNameToSrcDbId.values()) {
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
            dbs.put(db.getFullName(), db);
        }

        for (Database db : dbs.values()) {
            db.writeLock();
        }
        try {
            OlapTable destOlapTbl = (OlapTable) destDb.getTable(destTblId);
            for (Map.Entry<String, Long> entry : partNameToSrcTblId.entrySet()) {
                Database db = Catalog.getCurrentCatalog().getDb(partNameToSrcDbId.get(entry.getKey()));
                OlapTable srcTbl = (OlapTable) db.getTable(entry.getValue());
                srcTbl.exchangePartition(partNameToSrcDbId.get(entry.getKey()), destDb.getId(), destOlapTbl,
                        entry.getKey());
            }

            LOG.info("finished to replay exchange partition for table: " + destOlapTbl.getName());
        } finally {
            for (Database db : dbs.descendingMap().values()) {
                db.writeUnlock();
            }
        }
    }

    public AlterHandler getSchemaChangeHandler() {
        return this.schemaChangeHandler;
    }

    public AlterHandler getRollupHandler() {
        return this.rollupHandler;
    }

    public AlterHandler getClusterHandler() {
        return this.clusterHandler;
    }
}
