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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.DdlException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class DecommissionBackendJobV2 extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(DecommissionBackendJobV2.class);

    private List<Long> backendIds = Lists.newArrayList();
    private List<Long> finishedBackendIds = Lists.newArrayList();

    public DecommissionBackendJobV2(long jobId, List<Long> backendIds) {
        super(jobId, JobType.DECOMMISSION_BACKEND, -1, -1, "", -1);
        this.backendIds = backendIds;
    }

    private DecommissionBackendJobV2() {
        super(JobType.DECOMMISSION_BACKEND);
    }

    @Override
    protected void runPendingJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.PENDING, jobState);
        this.jobState = JobState.RUNNING;
    }

    /*
     * runWaitingTxnJob():
     */
    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.WAITING_TXN, jobState);
        this.jobState = JobState.RUNNING;
    }

    /*
     * runRunningJob()
     */
    @Override
    protected void runRunningJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);

        SystemInfoService systemInfoService = Catalog.getCurrentSystemInfo();
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        for (Long beId : backendIds) {
            Backend backend = systemInfoService.getBackend(beId);
            if (backend == null || !backend.isDecommissioned()) {
                finishedBackendIds.add(beId);
                continue;
            }

            List<Long> backendTabletIds = invertedIndex.getTabletIdsByBackendId(beId);
            if (backendTabletIds.isEmpty()) {
                LOG.info("no tablet in {}", beId);
                finishedBackendIds.add(beId);
            }

            LOG.info("backend {} lefts {} replicas to decommission: {}", beId, backendTabletIds.size(),
                    backendTabletIds.size() <= 20 ? backendTabletIds : "too many");
        }

        if (finishedBackendIds.size() < backendIds.size()) {
            return;
        }

        // drop backends
        for (Long backendId : backendIds) {
            try {
                systemInfoService.dropBackend(backendId);
                LOG.info("backend {} is dropped after decommission", backendId);
            } catch (DdlException e) {
                // does not matter, may be backend not exist
                LOG.info("backend {} is dropped failed after decommission {}", backendId, e.getMessage());
            }
        }

        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();

        Catalog.getCurrentCatalog().getEditLog().logAlterJob(this);
        LOG.info("decommission job finished: {}", jobId);
    }

    @Override
    protected boolean cancelImpl(String errMsg) {
        return false;
    }

    public static DecommissionBackendJobV2 read(DataInput in) throws IOException {
        DecommissionBackendJobV2 job = new DecommissionBackendJobV2();
        job.readFields(in);
        return job;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(backendIds.size());
        for (Long beId : backendIds) {
            out.writeLong(beId);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            backendIds.add(in.readLong());
        }
    }

    private void replayFinished(AlterJobV2 replayedJob) {
        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
    }

    private void replayCancelled(AlterJobV2 replayedJob) {
        this.jobState = JobState.CANCELLED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        switch (replayedJob.jobState) {
            case PENDING:
            case WAITING_TXN:
                this.jobState = JobState.RUNNING;
                break;
            case FINISHED:
                replayFinished(replayedJob);
                break;
            case CANCELLED:
                replayCancelled(replayedJob);
                break;
            default:
                break;
        }
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
    }
}
