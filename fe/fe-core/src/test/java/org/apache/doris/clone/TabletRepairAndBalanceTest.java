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

package org.apache.doris.clone;

import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.DdlExecutor;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class TabletRepairAndBalanceTest {
    private static final Logger LOG = LogManager.getLogger(TabletRepairAndBalanceTest.class);

    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDirBase = "fe";
    private static String runningDir = runningDirBase + "/mocked/TabletRepairAndBalanceTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext connectContext;

    private static List<Backend> backends = Lists.newArrayList();

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println(runningDir);
        FeConstants.runningUnitTest = true;
        FeConstants.tablet_checker_interval_ms = 1000;
        Config.tablet_repair_delay_factor_second = 1;
        // 5 backends:
        // 127.0.0.1
        // 127.0.0.2
        // 127.0.0.3
        // 127.0.0.4
        // 127.0.0.5
        UtFrameUtils.createDorisClusterWithMultiTag(runningDir, 5);
        connectContext = UtFrameUtils.createDefaultCtx();

        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
    }

    @AfterClass
    public static void TearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDirBase);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    private static void alterTable(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().getAlterInstance().processAlterTable(alterTableStmt);
    }

    @Test
    public void test1() throws Exception {
        backends = Catalog.getCurrentSystemInfo().getClusterBackends(SystemInfoService.DEFAULT_CLUSTER);
        Assert.assertEquals(5, backends.size());

        // set tag for all backends. 0-2 to zone1, 4 and 5 to zone2
        for (int i = 0; i < backends.size(); ++i) {
            Backend be = backends.get(i);
            String tag = "zone1";
            if (i > 2) {
                tag = "zone2";
            }
            String stmtStr = "alter system modify backend \"" + be.getHost() + ":" + be.getHeartbeatPort()
                    + "\" set ('tag.location' = '" + tag + "')";
            AlterSystemStmt stmt = (AlterSystemStmt) UtFrameUtils.parseAndAnalyzeStmt(stmtStr, connectContext);
            DdlExecutor.execute(Catalog.getCurrentCatalog(), stmt);
        }
        Tag zone1 = Tag.create(Tag.TYPE_LOCATION, "zone1");
        Tag zone2 = Tag.create(Tag.TYPE_LOCATION, "zone2");
        Assert.assertEquals(zone1, backends.get(0).getTag());
        Assert.assertEquals(zone1, backends.get(1).getTag());
        Assert.assertEquals(zone1, backends.get(2).getTag());
        Assert.assertEquals(zone2, backends.get(3).getTag());
        Assert.assertEquals(zone2, backends.get(4).getTag());

        // create table
        // 1. no default tag
        String createStr = "create table test.tbl1\n" +
                "(k1 date, k2 int)\n" +
                "partition by range(k1)\n" +
                "(\n" +
                " partition p1 values less than(\"2021-06-01\"),\n" +
                " partition p2 values less than(\"2021-07-01\"),\n" +
                " partition p3 values less than(\"2021-08-01\")\n" +
                ")\n" +
                "distributed by hash(k2) buckets 10;";
        ExceptionChecker.expectThrows(DdlException.class, () -> createTable(createStr));

        // nodes of zone2 not enough
        String createStr2 = "create table test.tbl1\n" +
                "(k1 date, k2 int)\n" +
                "partition by range(k1)\n" +
                "(\n" +
                " partition p1 values less than(\"2021-06-01\"),\n" +
                " partition p2 values less than(\"2021-07-01\"),\n" +
                " partition p3 values less than(\"2021-08-01\")\n" +
                ")\n" +
                "distributed by hash(k2) buckets 10\n" +
                "properties\n" +
                "(\n" +
                "    \"replication_allocation\" = \"tag.location.zone1: 2, tag.location.zone2: 3\"\n" +
                ")";
        ExceptionChecker.expectThrows(DdlException.class, () -> createTable(createStr2));

        // normal
        String createStr3 = "create table test.tbl1\n" +
                "(k1 date, k2 int)\n" +
                "partition by range(k1)\n" +
                "(\n" +
                " partition p1 values less than(\"2021-06-01\"),\n" +
                " partition p2 values less than(\"2021-07-01\"),\n" +
                " partition p3 values less than(\"2021-08-01\")\n" +
                ")\n" +
                "distributed by hash(k2) buckets 10\n" +
                "properties\n" +
                "(\n" +
                "    \"replication_allocation\" = \"tag.location.zone1: 2, tag.location.zone2: 1\"\n" +
                ")";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createStr3));
        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test");
        OlapTable tbl = (OlapTable)db.getTable("tbl1");

        // alter table's replica allocation failed
        String alterStr = "alter table test.tbl1 set (\"replication_allocation\" = \"tag.location.zone1: 2, tag.location.zone2: 3\");";
        ExceptionChecker.expectThrows(DdlException.class, () -> alterTable(alterStr));
        ReplicaAllocation tblReplicaAlloc = tbl.getDefaultReplicaAllocation();
        Assert.assertEquals(3, tblReplicaAlloc.getTotalReplicaNum());
        Assert.assertEquals(Short.valueOf((short) 2), tblReplicaAlloc.getReplicaNumByTag(Tag.create(Tag.TYPE_LOCATION, "zone1")));
        Assert.assertEquals(Short.valueOf((short) 1), tblReplicaAlloc.getReplicaNumByTag(Tag.create(Tag.TYPE_LOCATION, "zone2")));

        // alter partition's replica allocation succeed
        String alterStr2 = "alter table test.tbl1 modify partition p1 set (\"replication_allocation\" = \"tag.location.zone1: 1, tag.location.zone2: 2\");";
        ExceptionChecker.expectThrowsNoException(() -> alterTable(alterStr2));
        Partition p1 = tbl.getPartition("p1");
        ReplicaAllocation p1ReplicaAlloc = tbl.getPartitionInfo().getReplicaAllocation(p1.getId());
        Assert.assertEquals(3, p1ReplicaAlloc.getTotalReplicaNum());
        Assert.assertEquals(Short.valueOf((short) 1), p1ReplicaAlloc.getReplicaNumByTag(Tag.create(Tag.TYPE_LOCATION, "zone1")));
        Assert.assertEquals(Short.valueOf((short) 2), p1ReplicaAlloc.getReplicaNumByTag(Tag.create(Tag.TYPE_LOCATION, "zone2")));

        // check backend get() methods
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        Set<Tag> tags = infoService.getTagsByCluster(SystemInfoService.DEFAULT_CLUSTER);
        Assert.assertEquals(2, tags.size());

        // check tablet and replica number
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        Table<Long, Long, Replica> replicaMetaTable = invertedIndex.getReplicaMetaTable();
        Assert.assertEquals(30, replicaMetaTable.rowKeySet().size());
        Assert.assertEquals(5, replicaMetaTable.columnKeySet().size());
        while(true) {
            Thread.sleep(2000);
            System.out.println("sleep 2");
        }
    }
}

