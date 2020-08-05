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

package org.apache.doris.http.controller;

import org.apache.doris.http.HttpAuthManager;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/rest/v1")
public class LoginController extends BaseController {

    @RequestMapping(path = "/login", method = RequestMethod.POST)
    public Object login(HttpServletRequest request, HttpServletResponse response, @RequestBody String body) {
        JSONObject root = new JSONObject(body);
        Map<String, Object> result = root.toMap();
        String auth = result.get("username") + ":" + result.get("password").toString();
        request.setAttribute("Authorization", auth);
        Map<String, Object> msg = new HashMap<>();
        try {
            if (checkAuthWithCookie(request, response)) {
                msg.put("code", 200);
                msg.put("msg", "Login success!");
            } else {
                msg.put("code", 401);
                msg.put("msg", "Login fail!");
            }
            return msg;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return msg;
    }

}