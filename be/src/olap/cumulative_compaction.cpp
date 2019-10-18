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
    : Compaction(tablet) {
}

CumulativeCompaction::~CumulativeCompaction() { }

OLAPStatus CumulativeCompaction::compact_impl() {
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

    // 3. try to get enough memory
    RETURN_NOT_OK(consume_memory());

    // 4. do cumulative compaction, merge rowsets
    RETURN_NOT_OK(do_compaction());

    // 5. set state to success
    _state = CompactionState::SUCCESS;

    // 6. set cumulative point
    _tablet->set_cumulative_layer_point(_input_rowsets.back()->end_version() + 1);
    
    // 7. garbage collect input rowsets after cumulative compaction 
    RETURN_NOT_OK(gc_unused_rowsets());

    // 8. add metric to cumulative compaction
    DorisMetrics::cumulative_compaction_deltas_total.increment(_input_rowsets.size());
    DorisMetrics::cumulative_compaction_bytes_total.increment(_input_rowsets_size);

    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::pick_rowsets_to_compact() {
    _input_rowsets.clear();
    std::vector<RowsetSharedPtr> candidate_rowsets;
    _tablet->pick_candicate_rowsets_to_cumulative_compaction(&candidate_rowsets);

    if (candidate_rowsets.size() <= 1) {
        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    RETURN_NOT_OK(check_version_continuity(candidate_rowsets));

    std::vector<RowsetSharedPtr> transient_rowsets;
    size_t num_overlapping_segments = 0;
    for (size_t i = 0; i < candidate_rowsets.size() - 1; ++i) {
        // iterate until (candidate_rowsets.size() - 1) because:
        // VersionHash will calculated from chosen rowsets.
        // If ultimate singleton rowset is chosen, VersionHash
        // will be different from the value recorded in FE.
        // So the ultimate singleton rowset is reserved.
        RowsetSharedPtr rowset = candidate_rowsets[i];
        if (_tablet->version_for_delete_predicate(rowset->version())) {
            // cumulative compaction can not handle delete version.
            // so if we meet a delete version:
            // A. if we already select enought rowset to do compaction, finish selecting.
            // B. not enough rowset number, discard all previous selected rowset, skip this delta version, and continue to select.
            if (num_overlapping_segments >= config::min_cumulative_compaction_num_singleton_deltas) {
                // case A
                _input_rowsets = transient_rowsets;
                break;
            }
            // case B
            transient_rowsets.clear();
            num_overlapping_segments = 0;
            continue;
        }

        if (rowset->start_version() == rowset->end_version()) {
            // segments in single version rowset may be overlapped, so increase the num_overlapping_segments
            // with the number of segments in this rowset.
            num_overlapping_segments += rowset->num_segments();
        } else {
            // segments in a multi version rowset are not overlapped, so only increase num_overlapping_segments by 1
            num_overlapping_segments++;
        }
        transient_rowsets.push_back(rowset); 

        if (num_overlapping_segments >= config::max_cumulative_compaction_num_singleton_deltas) {
            // select enough number of rowset to do compaction, or the memory reach limit, finish selecting.
            break;
        }
    }

    if (num_overlapping_segments >= config::min_cumulative_compaction_num_singleton_deltas) {
        _input_rowsets = transient_rowsets;
    }
		
    if (_input_rowsets.empty()) {
        // There are no rowsets choosed to do cumulative compaction.
        // Under this circumstance, cumulative_point should be set.
        // Otherwise, the next round will not choose rowsets. 
        _tablet->set_cumulative_layer_point(candidate_rowsets.back()->start_version());
    }

    if (_input_rowsets.size() <= 1) {
        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    return OLAP_SUCCESS;
}

}  // namespace doris

