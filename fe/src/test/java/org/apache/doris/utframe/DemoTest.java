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

package org.apache.doris.utframe;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.utframe.MockedBackendFactory.DefaultBeThriftServiceImpl;
import org.apache.doris.utframe.MockedBackendFactory.DefaultHeartbeatServiceImpl;
import org.apache.doris.utframe.MockedBackendFactory.DefaultPBackendServiceImpl;
import org.apache.doris.utframe.MockedFrontend.EnvVarNotSetException;
import org.apache.doris.utframe.MockedFrontend.FeStartException;
import org.apache.doris.utframe.MockedFrontend.NotInitException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

/*
 * This demo shows how to run unit test with mocked FE and BE.
 * It will
 *  1. start a mocked FE and a mocked BE.
 *  2. Create a database and a tbl.
 *  3. Make a schema change to tbl.
 *  4. send a query and get query plan
 */
public class DemoTest {

    private static int fe_http_port;
    private static int fe_rpc_port;
    private static int fe_query_port;
    private static int fe_edit_log_port;

    private static int be_heartbeat_port;
    private static int be_thrift_port;
    private static int be_brpc_port;
    private static int be_http_port;

    @BeforeClass
    public static void beforeClass() throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        // get DORIS_HOME
        final String dorisHome = System.getenv("DORIS_HOME");
        if (Strings.isNullOrEmpty(dorisHome)) {
            throw new EnvVarNotSetException("env DORIS_HOME is not set");
        }

        getRandomPort();

        // start fe in "DORIS_HOME/fe/mocked/"
        MockedFrontend frontend = MockedFrontend.getInstance();
        Map<String, String> feConfMap = Maps.newHashMap();
        // set additional fe config
        feConfMap.put("http_port", String.valueOf(fe_http_port));
        feConfMap.put("rpc_port", String.valueOf(fe_rpc_port));
        feConfMap.put("query_port", String.valueOf(fe_query_port));
        feConfMap.put("edit_log_port", String.valueOf(fe_edit_log_port));
        feConfMap.put("tablet_create_timeout_second", "10");
        frontend.init(dorisHome + "/fe/mocked/DemoTest", feConfMap);
        frontend.start(new String[0]);

        // start be
        MockedBackend backend = MockedBackendFactory.createBackend("127.0.0.1",
                be_heartbeat_port, be_thrift_port, be_brpc_port, be_http_port,
                new DefaultHeartbeatServiceImpl(be_thrift_port, be_http_port, be_brpc_port),
                new DefaultBeThriftServiceImpl(), new DefaultPBackendServiceImpl());
        backend.setFeAddress(new TNetworkAddress("127.0.0.1", frontend.getRpcPort()));
        backend.start();

        // add be
        List<Pair<String, Integer>> bes = Lists.newArrayList();
        bes.add(Pair.create(backend.getHost(), backend.getHeartbeatPort()));
        Catalog.getCurrentSystemInfo().addBackends(bes, false, "default_cluster");

        // sleep to wait first heartbeat
        Thread.sleep(5000);
    }

    // generate all port from between 20000 ~ 30000
    private static void getRandomPort() {
        Random r = new Random(System.currentTimeMillis());
        int basePort = 20000 + r.nextInt(9000);
        fe_http_port = basePort + 1;
        fe_rpc_port = basePort + 2;
        fe_query_port = basePort + 3;
        fe_edit_log_port = basePort + 4;

        be_heartbeat_port = basePort + 5;
        be_thrift_port = basePort + 6;
        be_brpc_port = basePort + 7;
        be_http_port = basePort + 8;
    }

    @Test
    public void testCreateDbAndTable() throws Exception {
        // 1. create connect context
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        // 2. create database db1
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        System.out.println(Catalog.getCurrentCatalog().getDbNames());
        // 3. create table tbl1
        String createTblStmtStr = "create table db1.tbl1(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
        // 4. get and test the created db and table
        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:db1");
        Assert.assertNotNull(db);
        db.readLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable("tbl1");
            Assert.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assert.assertEquals("Doris", tbl.getEngine());
            Assert.assertEquals(1, tbl.getBaseSchema().size());
        } finally {
            db.readUnlock();
        }
        // 5. process a schema change job
        String alterStmtStr = "alter table db1.tbl1 add column k2 int default '1'";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(alterStmtStr, ctx);
        Catalog.getCurrentCatalog().getAlterInstance().processAlterTable(alterTableStmt);
        // 6. check alter job
        Map<Long, AlterJobV2> alterJobs = Catalog.getCurrentCatalog().getSchemaChangeHandler().getAlterJobsV2();
        Assert.assertEquals(1, alterJobs.size());
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println("alter job " + alterJobV2.getDbId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(5000);
            }
            System.out.println("alter job " + alterJobV2.getDbId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
        db.readLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable("tbl1");
            Assert.assertEquals(2, tbl.getBaseSchema().size());
        } finally {
            db.readUnlock();
        }

        // query
        // TODO: we can not process real query for now. So it has to be a explain query
        String queryStr = "explain select * from db1.tbl1";
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, queryStr);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        List<PlanFragment> fragments = planner.getFragments();
        Assert.assertEquals(1, fragments.size());
        PlanFragment fragment = fragments.get(0);
        Assert.assertTrue(fragment.getPlanRoot() instanceof OlapScanNode);
        Assert.assertEquals(0, fragment.getChildren().size());
    }
}
