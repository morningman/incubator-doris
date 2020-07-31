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

package org.apache.doris.http.rest;

import com.google.common.collect.Maps;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

// This class is a RESTFUL interface to get query profile.
// It will be used in query monitor to collect profiles.   
// Usage:
//      wget http://fe_host:fe_http_port/api/profile?query_id=123456
public class ProfileAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(ProfileAction.class);

    @RequestMapping(path = "/api/profile",method = RequestMethod.GET)
    protected Object profile(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        executeCheckPassword(request, response);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        String queryId = request.getParameter("query_id");
        if (queryId == null) {
            entity.setCode(HttpStatus.BAD_REQUEST.value());
            entity.setMsg("not valid parameter");
            return entity;
        }

        String queryProfileStr = ProfileManager.getInstance().getProfile(queryId);
        if (queryProfileStr == null) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("query id " + queryId + " not found.");
            return entity;
        }

        Map<String, String> result = Maps.newHashMap();
        result.put("profile", queryProfileStr);
        entity.setData(result);
        return entity; 
    }
}
