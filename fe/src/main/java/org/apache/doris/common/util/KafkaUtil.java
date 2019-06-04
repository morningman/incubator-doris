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

package org.apache.doris.common.util;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TKafkaLoadInfo;
import org.apache.doris.thrift.TKafkaMetaProxyRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TProxyRequest;
import org.apache.doris.thrift.TProxyResult;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaUtil {
    private static final Logger LOG = LogManager.getLogger(KafkaUtil.class);

    public static List<Integer> getAllKafkaPartitions(String brokerList, String topic,
            Map<String, String> convertedCustomProperties) throws UserException {
        BackendService.Client client = null;
        TNetworkAddress address = null;
        boolean ok = false;
        try {
            List<Long> backendIds = Catalog.getCurrentSystemInfo().getBackendIds(true);
            if (backendIds.isEmpty()) {
                throw new LoadException("Failed to get all partitions. No alive backends");
            }
            Collections.shuffle(backendIds);
            Backend be = Catalog.getCurrentSystemInfo().getBackend(backendIds.get(0));
            address = new TNetworkAddress(be.getHost(), be.getBePort());
            client = ClientPool.backendPool.borrowObject(address);

            TProxyRequest request = new TProxyRequest();
            TKafkaMetaProxyRequest kafkaRequest = new TKafkaMetaProxyRequest();
            TKafkaLoadInfo loadInfo = new TKafkaLoadInfo(brokerList, topic, Maps.newHashMap());
            loadInfo.setProperties(convertedCustomProperties);
            kafkaRequest.setKafka_info(loadInfo);
            request.setKafka_meta_request(kafkaRequest);

            TProxyResult res = client.get_info(request);
            ok = true;

            if (res.getStatus().getStatus_code() != TStatusCode.OK) {
                throw new LoadException("Failed to get all partitions of kafka topic: " + topic + ". error: "
                        + res.getStatus().getError_msgs());
            }

            return res.getKafka_meta_result().getPartition_ids();
        } catch (Exception e) {
            LOG.warn("failed to get partitions.", e);
            throw new LoadException(
                    "Failed to get all partitions of kafka topic: " + topic + ". error: " + e.getMessage());
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
    }
}
