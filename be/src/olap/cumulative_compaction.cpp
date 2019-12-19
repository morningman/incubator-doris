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

#include "olap/cumulative_compaction.h"
#include "util/doris_metrics.h"

namespace doris {

CumulativeCompaction::CumulativeCompaction(TabletSharedPtr tablet)
    : Compaction(tablet),
      _cumulative_rowset_size_threshold(config::cumulative_compaction_budgeted_bytes)
{ }

CumulativeCompaction::~CumulativeCompaction() { }

OLAPStatus CumulativeCompaction::compact() {
    if (!_tablet->init_succeeded()) {
        return OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS;
    }

    MutexLock lock(_tablet->get_cumulative_lock(), TRY_LOCK);
    if (!lock.own_lock()) {
        LOG(INFO) << "The tablet is under cumulative compaction. tablet=" << _tablet->full_name();
        return OLAP_ERR_CE_TRY_CE_LOCK_ERROR;
    }

    // 1.calculate cumulative point 
    RETURN_NOT_OK(_tablet->calculate_cumulative_point());

    // 2. pick rowsets to compact
    RETURN_NOT_OK(pick_rowsets_to_compact());

    // 3. do cumulative compaction, merge rowsets
    RETURN_NOT_OK(do_compaction());

    // 4. set state to success
    _state = CompactionState::SUCCESS;

    // 5. set cumulative point
    _tablet->set_cumulative_layer_point(_input_rowsets.back()->end_version() + 1);
    
    // 6. garbage collect input rowsets after cumulative compaction 
    RETURN_NOT_OK(gc_unused_rowsets());

    // 7. add metric to cumulative compaction
    DorisMetrics::cumulative_compaction_deltas_total.increment(_input_rowsets.size());
    DorisMetrics::cumulative_compaction_bytes_total.increment(_input_rowsets_size);

    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::pick_rowsets_to_compact() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    _tablet->pick_candicate_rowsets_to_cumulative_compaction(&candidate_rowsets);

    if (candidate_rowsets.size() <= 1) {
        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    RETURN_NOT_OK(check_version_continuity(candidate_rowsets));

    std::vector<RowsetSharedPtr> transient_rowsets;
    size_t num_overlapping_segments = 0;
    // the last delete version we meet when traversing candidate_rowsets
    Version last_delete_version { -1, -1 };

    // traverse rowsets from begin to penultimate rowset.
    // Because VersionHash will calculated from chosen rowsets.
    // If ultimate singleton rowset is chosen, VersionHash
    // will be different from the value recorded in FE.
    // So the ultimate singleton rowset is revserved.
    for (size_t i = 0; i < candidate_rowsets.size() - 1; ++i) {
        RowsetSharedPtr rowset = candidate_rowsets[i];
        if (_tablet->version_for_delete_predicate(rowset->version())) {
            last_delete_version = rowset->version();
            if (num_overlapping_segments >= config::min_cumulative_compaction_num_singleton_deltas) {
                _input_rowsets = transient_rowsets;
                break;
            }
            transient_rowsets.clear();
            num_overlapping_segments = 0;
            continue;
        }

        if (num_overlapping_segments >= config::max_cumulative_compaction_num_singleton_deltas
            && transient_rowsets.size() >= 2) {
            // the threshold of files to compacted one time
            break;
        }

        if (rowset->start_version() == rowset->end_version()) {
            num_overlapping_segments += rowset->num_segments();
        } else {
            num_overlapping_segments++;
        }
        transient_rowsets.push_back(rowset); 
    }

    if (num_overlapping_segments >= config::min_cumulative_compaction_num_singleton_deltas) {
        _input_rowsets = transient_rowsets;
    }

    // Cumulative compaction will process with at least 2 rowsets.
    // So when there is no rowset or only 1 rowset being chosen, we should return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS:
    if (_input_rowsets.size() <= 1) {
        if (last_delete_version.first != -1) {
            // we meet a delete version, should increase the cumulative point to let base compaction handle the delete version.
            // plus 1 to skip the delete version.
            // NOTICE: after that, the cumulative point may be larger than max version of this tablet, but it doen't matter.
            _tablet->set_cumulative_layer_point(last_delete_version.first + 1);
            return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
        }

        // we did not meet any delete version. which means num_overlapping_segments is not enough to do cumulative compaction.
        // We should wait until there are more rowsets to come, and keep the cumulative point unchanged.
        // But in order to avoid the stall of compaction because no new rowset arrives later, we should increase
        // the cumulative point after waiting for a long time, to ensure that the base compaction can continue.

        // check both last success time of base and cumulative compaction
        int64_t last_cumu_compaction_success_time = _tablet->last_cumu_compaction_success_time();
        int64_t last_base_compaction_success_time = _tablet->last_base_compaction_success_time();
        
        int64_t interval_threshold = config::base_compaction_interval_seconds_since_last_operation;
        int64_t now = time(NULL);
        int64_t cumu_interval = now - last_cumu_compaction_success_time;
        int64_t base_interval = now - last_base_compaction_success_time;
        if (cumu_interval > interval_threshold && base_interval > interval_threshold) {
            _tablet->set_cumulative_layer_point(candidate_rowsets.back()->start_version());
        }

        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    return OLAP_SUCCESS;
}

}  // namespace doris

