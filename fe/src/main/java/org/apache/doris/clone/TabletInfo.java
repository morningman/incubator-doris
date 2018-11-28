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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.clone.SchedException.Status;
import org.apache.doris.clone.TabletScheduler.Slot;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CloneTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/*
 * TabletInfo contains all information which is created during tablet scheduler processing.
 */
public class TabletInfo implements Comparable<TabletInfo> {
    private static final Logger LOG = LogManager.getLogger(TabletInfo.class);

    // max interval time of a tablet being scheduled
    private static final long MAX_NOT_BEING_SCHEDULED_INTERVAL_MS = 30 * 60 * 1000L; // 30 min
    // min interval time of adjusting a tablet's priority
    private static final long MIN_ADJUST_PRIORITY_INTERVAL_MS = 5 * 60 * 1000L; // 5 min
    // min clone task timeout
    private static final long MIN_CLONE_TASK_TIMEOUT_MS = 3 * 60 * 1000L; // 3 min
    // max clone task timeout
    private static final long MAX_CLONE_TASK_TIMEOUT_MS = 2 * 60 * 60 * 1000L; // 2 hour
    // approximate min clone speed (MB/s)
    private static final long MIN_CLONE_SPEED_MB_PER_SECOND = 5; // 5MB/sec
    // threshold of times a tablet failed to be scheduled
    private static final int SCHED_FAILED_COUNTER_THRESHOLD = 5;
    // threshold of times a clone task failed to be processed
    private static final int RUNNING_FAILED_COUNTER_THRESHOLD = 3;

    public enum Priority {
        LOW,
        NORMAL,
        HIGH,
        VERY_HIGH;

        // VERY_HIGH can only be downgraded to NORMAL
        // LOW can only be upgraded to HIGH
        public Priority adjust(Priority origPriority, boolean isUp) {
            switch (this) {
                case VERY_HIGH:
                    return isUp ? VERY_HIGH : HIGH;
                case HIGH:
                    return isUp ? (origPriority == LOW ? HIGH : VERY_HIGH) : NORMAL;
                case NORMAL:
                    return isUp ? HIGH : (origPriority == Priority.VERY_HIGH ? NORMAL : LOW);
                default:
                    return isUp ? NORMAL : LOW;
            }
        }

    }

    public enum State {
        PENDING, RUNNING, FINISHED, CANCELLED
    }

    private Priority origPriority;
    // dynamic priority will be set during tablet schedule processing,
    // it can not be prior than origin priority.
    // dynamic priority is also used in priority queue compare in tablet scheduler.
    private Priority dynamicPriority;

    // we change the dynamic priority based on how many times it fails to be scheduled
    private int failedSchedCounter = 0;
    // clone task failed time
    private int failedRunningCounter = 0;

    // last time this tablet being scheduled
    private long lastSchedTime = 0;
    // last time adjust priority
    private long lastAdjustPrioTime = 0;

    // an approximate timeout of this task, only be set when sending clone task.
    private long timeoutMs = 0;

    private State state;
    private TabletStatus tabletStatus;

    private String cluster;
    private long dbId;
    private long tblId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private int schemaHash;
    private TStorageMedium storageMedium;

    private long createTime = -1;
    private long finishedTime = -1;

    // components which will be set during processing
    private Tablet tablet = null;
    private long visibleVersion = -1;
    private long visibleVersionHash = -1;
    private long committedVersion = -1;
    private long committedVersionHash = -1;

    private Replica srcReplica = null;
    private long srcPathHash = -1;
    private long destBackendId = -1;
    private long destPathHash = -1;
    private String errMsg = null;

    private CloneTask cloneTask = null;

    private SystemInfoService infoService;

    public TabletInfo(String cluster, long dbId, long tblId, long partId, long idxId, long tabletId,
            int schemaHash, TStorageMedium storageMedium, long createTime) {
        this.cluster = cluster;
        this.dbId = dbId;
        this.tblId = tblId;
        this.partitionId = partId;
        this.indexId = idxId;
        this.tabletId = tabletId;
        this.schemaHash = schemaHash;
        this.storageMedium = storageMedium;
        this.createTime = createTime;
        this.infoService = Catalog.getCurrentSystemInfo();
        this.state = State.PENDING;
    }

    public Priority getOrigPriority() {
        return origPriority;
    }

    public void setOrigPriority(Priority origPriority) {
        this.origPriority = origPriority;
        // reset dynamic priority along with the origin priority being set.
        this.dynamicPriority = origPriority;
    }

    public Priority getDynamicPriority() {
        return dynamicPriority;
    }

    public void increaseFailedSchedCounter() {
        ++failedSchedCounter;
    }

    public void increaseFailedRunningCounter() {
        ++failedRunningCounter;
    }

    public int getFailedRunningCounter() {
        return failedRunningCounter;
    }

    public void setLastSchedTime(long lastSchedTime) {
        this.lastSchedTime = lastSchedTime;
    }

    public State getState() {
        return state;
    }

    public void setState(State state, String errMsg) {
        this.state = state;
        this.errMsg = errMsg;
    }

    public TabletStatus getTabletStatus() {
        return tabletStatus;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTblId() {
        return tblId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public String getCluster() {
        return cluster;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getCommittedVersion() {
        return visibleVersion;
    }

    public long getCommittedVersionHash() {
        return visibleVersionHash;
    }

    public void setTablet(Tablet tablet) {
        this.tablet = tablet;
    }

    // database lock should be held.
    public List<Replica> getReplicas() {
        return tablet.getReplicas();
    }

    public void setVersionInfo(long visibleVersion, long visibleVersionHash,
            long committedVersion, long committedVersionHash) {
        this.visibleVersion = visibleVersion;
        this.visibleVersionHash = visibleVersionHash;
        this.committedVersion = committedVersion;
        this.committedVersionHash = committedVersionHash;
    }

    public void setDestination(Long destBe, long destPathHash) {
        this.destBackendId = destBe;
        this.destPathHash = destPathHash;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public CloneTask getCloneTask() {
        return cloneTask;
    }

    // database lock should be held.
    public long getTabletSize() {
        long max = Long.MIN_VALUE;
        for (Replica replica : tablet.getReplicas()) {
            if (replica.getDataSize() > max) {
                max = replica.getDataSize();
            }
        }
        return max;
    }

    // database lock should be held.
    public boolean containsBE(long beId) {
        for (Replica replica : tablet.getReplicas()) {
            if (replica.getBackendId() == beId) {
                return true;
            }
        }
        return false;
    }

    // database lock should be held.
    public void chooseSrcReplica(Map<Long, Slot> backendsWorkingSlots) throws SchedException {
        /*
         * get all candidate source replicas
         * 1. source replica should be healthy.
         * 2. slot of this source replica is available. 
         */
        List<Replica> candidates = Lists.newArrayList();
        for (Replica replica : tablet.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null || !be.isAlive()) {
                // backend which is in decommission can still be the source backend
                continue;
            }

            if (replica.getLastFailedVersion() > 0) {
                continue;
            }

            if (!replica.checkVersionCatchUp(visibleVersion, visibleVersionHash)) {
                continue;
            }

            candidates.add(replica);
        }

        if (candidates.isEmpty()) {
            throw new SchedException(Status.SCHEDULE_FAILED, "unable to find source replica");
        }

        // choose a replica which slot is available from candidates.
        for (Replica srcReplica : candidates) {
            Slot slot = backendsWorkingSlots.get(srcReplica.getBackendId());
            if (slot == null) {
                continue;
            }

            long srcPathHash = slot.takeSlot(srcReplica.getPathHash());
            if (srcPathHash != -1) {
                this.srcReplica = srcReplica;
                this.srcPathHash = srcPathHash;
                return;
            }
        }

        throw new SchedException(Status.SCHEDULE_FAILED, "unable to find source slot");
    }

    /*
     * Same rules as choosing source replica for supplement.
     * But we need to check that we can not choose the same replica as dest replica,
     * because replica info is changing all the time.
     */
    public void chooseSrcReplicaForVersionIncomplete(Map<Long, Slot> backendsWorkingSlots)
            throws SchedException {
        chooseSrcReplica(backendsWorkingSlots);
        if (srcReplica.getBackendId() == destBackendId) {
            throw new SchedException(Status.SCHEDULE_FAILED, "the chosen source replica is in dest backend");
        }
    }

    /*
     * Rules to choose a destination replica for version incomplete
     * 1. replica's last failed version > 0
     * 2. better to choose a replica which has a lower last failed version
     * 3. best to choose a replica if its last success version > last failed version
     * 
     * database lock should be held.
     */
    public void chooseDestReplicaForVersionIncomplete(Map<Long, Slot> backendsWorkingSlots)
            throws SchedException {
        Replica chosenReplica = null;
        for (Replica replica : tablet.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null || !be.isAvailable()) {
                continue;
            }
            if (replica.getLastFailedVersion() <= 0) {
                continue;
            }

            if (chosenReplica == null) {
                chosenReplica = replica;
            } else if (replica.getLastSuccessVersion() > replica.getLastFailedVersion()) {
                chosenReplica = replica;
                break;
            } else if (replica.getLastFailedVersion() < chosenReplica.getLastFailedVersion()) {
                // its better to select a low last failed version replica
                chosenReplica = replica;
            }
        }

        if (chosenReplica == null) {
            throw new SchedException(Status.SCHEDULE_FAILED, "unable to choose dest replica");
        }

        // check if the dest replica has available slot
        Slot slot = backendsWorkingSlots.get(chosenReplica.getBackendId());
        if (slot == null) {
            throw new SchedException(Status.SCHEDULE_FAILED, "backend of dest replica is missing");
        }

        long destPathHash = slot.takeSlot(chosenReplica.getPathHash());
        if (destPathHash == -1) {
            throw new SchedException(Status.SCHEDULE_FAILED, "unable to take slot of dest path");
        }

        this.destBackendId = chosenReplica.getBackendId();
        this.destPathHash = chosenReplica.getPathHash();
    }

    /*
     * release all resources before finishing this task
     */
    public void releaseResource(TabletScheduler tabletScheduler) {
        if (srcReplica != null) {
            Preconditions.checkState(srcPathHash != -1);
            Slot slot = tabletScheduler.getBackendsWorkingSlots().get(srcReplica.getBackendId());
            if (slot != null) {
                slot.freeSlot(srcPathHash);
            }
        }

        if (destPathHash != -1) {
            Slot slot = tabletScheduler.getBackendsWorkingSlots().get(destBackendId);
            if (slot != null) {
                slot.freeSlot(destPathHash);
            }
        }

        if (cloneTask != null) {
            AgentTaskQueue.removeTask(cloneTask.getBackendId(), TTaskType.CLONE, cloneTask.getSignature());
        }

        reset();
    }

    // reset to save memory after state is done
    private void reset() {
        Preconditions.checkState(state == State.FINISHED || state == State.CANCELLED);
        this.tablet = null;
        this.srcReplica = null;
        this.infoService = null;
        this.cloneTask = null;
    }

    public void deleteReplica(Replica replica) {
        tablet.deleteReplicaByBackendId(replica.getBackendId());
    }

    // database lock should be held.
    public CloneTask createCloneReplicaAndTask() throws SchedException {
        Backend srcBe = infoService.getBackend(srcReplica.getBackendId());
        if (srcBe == null) {
            errMsg = "src backend " + srcReplica.getBackendId() + " does not exist";
            throw new SchedException(Status.SCHEDULE_FAILED, errMsg);
        }

        Backend destBe = infoService.getBackend(destBackendId);
        if (destBe == null) {
            errMsg = "dest backend " + srcReplica.getBackendId() + " does not exist";
            throw new SchedException(Status.SCHEDULE_FAILED, errMsg);
        }

        // create the clone task and clone replica.
        // we use visible version in clone task, but set the clone replica's last failed version to
        // committed version.
        // because the clone task can only clone the data with visible version.
        // so after clone is finished, the clone replica's version is visible version, but its last
        // failed version is committed version, which is larger than visible version.
        // So after clone, this clone replica is still version incomplete, and it will be repaired in
        // another clone task.
        // That is, we may need to use 2 clone tasks to create a new replica. It is inefficient,
        // but there is no other way now.
        TBackend tSrcBe = new TBackend(srcBe.getHost(), srcBe.getBePort(), srcBe.getHttpPort());
        cloneTask = new CloneTask(destBackendId, dbId, tblId, partitionId, indexId,
                tblId, schemaHash, Lists.newArrayList(tSrcBe), storageMedium,
                visibleVersion, visibleVersionHash);
        cloneTask.setPathHash(srcPathHash, destPathHash);

        Replica cloneReplica = new Replica(
                Catalog.getCurrentCatalog().getNextId(), destBackendId,
                -1 /* version */, 0 /* version hash */,
                -1 /* data size */, -1 /* row count */,
                ReplicaState.CLONE,
                committedVersion, committedVersionHash, /* use committed version as last failed version */
                -1 /* last success version */, 0 /* last success version hash */);

        // addReplica() method will add this replica to tablet inverted index too.
        tablet.addReplica(cloneReplica);

        timeoutMs = getApproximateTimeoutMs();

        this.state = State.RUNNING;
        return cloneTask;
    }

    // timeout is between MIN_CLONE_TASK_TIMEOUT_MS and MAX_CLONE_TASK_TIMEOUT_MS
    private long getApproximateTimeoutMs() {
        long tabletSize = getTabletSize();
        long timeoutMs = tabletSize / 1024 / 1024 / MIN_CLONE_SPEED_MB_PER_SECOND * 1000;
        timeoutMs = Math.max(timeoutMs, MIN_CLONE_TASK_TIMEOUT_MS);
        timeoutMs = Math.min(timeoutMs, MAX_CLONE_TASK_TIMEOUT_MS);
        return timeoutMs;
    }

    public void finishCloneTask(CloneTask cloneTask, TTabletInfo reportedTablet) throws SchedException {
        Preconditions.checkState(state == State.RUNNING);
        Preconditions.checkArgument(cloneTask.getTaskVersion() == CloneTask.VERSION_2);
        
        try {
            // check clone task
            if (dbId != cloneTask.getDbId() || tblId != cloneTask.getTableId()
                    || partitionId != cloneTask.getPartitionId() || indexId != cloneTask.getIndexId()
                    || tabletId != cloneTask.getTableId() || destBackendId != cloneTask.getBackendId()) {
                String msg = String.format("clone task does not match the tablet info"
                        + ". clone task %d-%d-%d-%d-%d-%d"
                        + ", tablet info: %d-%d-%d-%d-%d-%d",
                        cloneTask.getDbId(), cloneTask.getTableId(), cloneTask.getPartitionId(),
                        cloneTask.getIndexId(), cloneTask.getTabletId(), cloneTask.getBackendId(),
                        dbId, tblId, partitionId, indexId, tablet.getId(), destBackendId);
                throw new SchedException(Status.SCHEDULE_FAILED, msg);
            }
            
            // Here we do not check if the clone version is equal to the partition's visible version.
            // Because in case of high frequency loading, clone version always lags behind the visible version,
            // so the clone job will never succeed, which cause accumulation of quorum finished load jobs.
            
            // But we will check if the clone replica's version is larger than or equal to the task's version.
            // (which is 'visibleVersion(hash)' saved)
            // We should discard the clone replica with stale version.
            if (reportedTablet.getVersion() < visibleVersion
                    || (reportedTablet.getVersion() == visibleVersion
                    && reportedTablet.getVersion_hash() != visibleVersionHash)) {
                errMsg = String.format("the clone replica's version is stale. %d-%d, task visible version: %d-%d",
                                       reportedTablet.getVersion(), reportedTablet.getVersion_hash(),
                                       visibleVersion, visibleVersionHash);
                throw new SchedException(Status.SCHEDULE_FAILED, errMsg);
            }
            
            // update clone replica's version
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
            if (db == null) {
                throw new SchedException(Status.UNRECOVERABLE, "db does not exist");
            }
            db.writeLock();
            try {
                OlapTable olapTable = (OlapTable) db.getTable(tblId);
                if (olapTable == null) {
                    throw new SchedException(Status.UNRECOVERABLE, "tbl does not exist");
                }
                
                Partition partition = olapTable.getPartition(partitionId);
                if (partition == null) {
                    throw new SchedException(Status.UNRECOVERABLE, "partition does not exist");
                }
                
                MaterializedIndex index = partition.getIndex(indexId);
                if (index == null) {
                    throw new SchedException(Status.UNRECOVERABLE, "index does not exist");
                }
                
                if (schemaHash != olapTable.getSchemaHashByIndexId(indexId)) {
                    throw new SchedException(Status.UNRECOVERABLE, "schema hash is not consistent. index's: "
                            + olapTable.getSchemaHashByIndexId(indexId)
                            + ", task's: " + schemaHash);
                }
                
                Tablet tablet = index.getTablet(tabletId);
                if (tablet == null) {
                    throw new SchedException(Status.UNRECOVERABLE, "tablet does not exist");
                }
                
                Replica replica = tablet.getReplicaByBackendId(destBackendId);
                if (replica == null) {
                    throw new SchedException(Status.UNRECOVERABLE, "replica does not exist. backend id: " + destBackendId);
                }
                
                /*
                 * we set visible version as clone task version, and committed version as clone replica's
                 * last failed version.
                 * If at that moment, the visible version == committed version, then the report tablet's version(hash)
                 * should be equal to the clone replica's last failed version(hash).
                 * We check it here.
                 */
                if (replica.getLastFailedVersion() == reportedTablet.getVersion()
                        && replica.getLastFailedVersionHash() != reportedTablet.getVersion_hash()) {
                    // do not throw exception, cause we want this clone task retry again.
                    errMsg = "replica's last failed version equals to report version: "
                            + replica.getLastFailedTimestamp() + " but hash is different: "
                            + replica.getLastFailedVersionHash() + " vs. " + reportedTablet.getVersion_hash();
                    throw new SchedException(Status.SCHEDULE_FAILED, errMsg);
                }
                
                replica.setState(ReplicaState.NORMAL);
                replica.updateVersionInfo(reportedTablet.getVersion(), reportedTablet.getVersion_hash(),
                                          reportedTablet.getData_size(), reportedTablet.getRow_count());
                
                finishedTime = System.currentTimeMillis();
                state = State.FINISHED;
                
                ReplicaPersistInfo info = ReplicaPersistInfo.createForClone(dbId, tblId, partitionId, indexId,
                                                                            tabletId, destBackendId, replica.getId(),
                                                                            reportedTablet.getVersion(),
                                                                            reportedTablet.getVersion_hash(),
                                                                            reportedTablet.getData_size(),
                                                                            reportedTablet.getRow_count(),
                                                                            replica.getLastFailedVersion(),
                                                                            replica.getLastFailedVersionHash(),
                                                                            replica.getLastSuccessVersion(),
                                                                            replica.getLastSuccessVersionHash());
                Catalog.getInstance().getEditLog().logAddReplica(info);
                
                LOG.info("clone finished: {}", this);
            } finally {
                db.writeUnlock();
            }
        } catch (SchedException e) {
            // if failed to too many times, remove this task
            ++failedRunningCounter;
            if (failedRunningCounter > RUNNING_FAILED_COUNTER_THRESHOLD) {
                throw new SchedException(Status.UNRECOVERABLE, e.getMessage());
            }
            throw e;
        }
    }

    /*
     * we try to adjust the priority based on schedule history
     * 1. If failed counter is larger than FAILED_COUNTER_THRESHOLD, which means this tablet is being scheduled
     *    at least FAILED_TIME_THRESHOLD times and all are failed. So we downgrade its priority.
     *    Also reset the failedCounter, or it will be downgraded forever.
     *    
     * 2. Else, if it has been a long time since last time the tablet being scheduled, we upgrade its
     *    priority to let it more available to be scheduled.
     *    
     * The time gap between adjustment should be larger than MIN_ADJUST_PRIORITY_INTERVAL_MS, to avoid
     * being downgraded too fast.
     *    
     * eg:
     *    A tablet has been scheduled 5 times and all failed. its priority will be downgraded. And if it is
     *    scheduled 5 times and all failed again, it will be downgraded again, until to the LOW.
     *    And than, because of LOW, this tablet can not be scheduled for a long time, add it will be upgraded
     *    to NORMAL, if still not being scheduled, it will be upgraded up to VERY_HIGH.
     */
    public void adjustPriority() {
        long currentTime = System.currentTimeMillis();
        if (lastAdjustPrioTime == 0) {
            // skip the first time we adjust this priority
            lastAdjustPrioTime = currentTime;
            return;
        } else {
            if (currentTime - lastAdjustPrioTime < MIN_ADJUST_PRIORITY_INTERVAL_MS) {
                return;
            }
        }

        boolean isDowngrade = false;
        boolean isUpgrade = false;

        if (failedSchedCounter > SCHED_FAILED_COUNTER_THRESHOLD) {
            isDowngrade = true;
        } else {
            long lastTime = lastSchedTime == 0 ? createTime : lastSchedTime;
            if (currentTime - lastTime > MAX_NOT_BEING_SCHEDULED_INTERVAL_MS) {
                isUpgrade = true;
            }
        }

        Priority originDynamicPriority = dynamicPriority;
        if (isDowngrade) {
            dynamicPriority = dynamicPriority.adjust(origPriority, false /* downgrade */);
            failedSchedCounter = 0;
            LOG.debug("downgrade dynamic priority from {} to {}, origin: {}, tablet: {}",
                      originDynamicPriority.name(), dynamicPriority.name(), origPriority.name(), tabletId);
        } else if (isUpgrade) {
            dynamicPriority = dynamicPriority.adjust(origPriority, true /* upgrade */);
            // no need to set lastSchedTime, lastSchedTime is set each time we schedule this tablet
            LOG.debug("upgrade dynamic priority from {} to {}, origin: {}, tablet: {}",
                      originDynamicPriority.name(), dynamicPriority.name(), origPriority.name(), tabletId);
        }
    }

    public boolean isTimeout() {
        if (state != TabletInfo.State.RUNNING) {
            return false;
        }

        Preconditions.checkState(lastSchedTime != 0 && timeoutMs != 0);
        return System.currentTimeMillis() - lastSchedTime > timeoutMs;
    }

    @Override
    public int compareTo(TabletInfo o) {
        if (dynamicPriority.ordinal() < o.getDynamicPriority().ordinal()) {
            return -1;
        } else if (dynamicPriority.ordinal() > o.getDynamicPriority().ordinal()) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tablet id: ").append(tabletId).append(", status: ").append(tabletStatus.name());
        sb.append(", state: ").append(state.name());
        if (srcReplica != null) {
            sb.append(". from backend: ").append(srcReplica.getBackendId());
            sb.append(", src path hash: ").append(srcPathHash);
        }
        if (destPathHash != -1) {
            sb.append(". to backend: ").append(destBackendId);
            sb.append(", dest path hash: ").append(destPathHash);
        }
        if (errMsg != null) {
            sb.append(". err: ").append(errMsg);
        }
        return sb.toString();
    }
}
