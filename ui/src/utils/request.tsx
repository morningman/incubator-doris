/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
 
/**
 * @file test cron
 * @author lpx
 * @since 2020/08/19
 */
import React from 'react';
import {message, Modal} from 'antd';
import {ExclamationCircleOutlined} from '@ant-design/icons';
import {Trans} from 'react-i18next';
let basePath;
function checkStatus(response) {
    if (response.status >= 200 && response.status < 300) {
        return response;
    }
    if (response.status === 401) {
        Modal.confirm({
            title: <Trans>tips</Trans>,
            icon: <ExclamationCircleOutlined/>,
            content: <Trans>loginExpirtMsg</Trans>,
            onOk() {
                window.location.href = window.location.origin + '/login';
            },
            onCancel() {
                //
            }
        });
        return;
    }
    const error = new Error(response.statusText);
    error.response = response;
    message.error(response.statusText);
    throw error;
}

/**
 * Requests a URL, returning a promise.
 *
 * @param  {string} url       The URL we want to request
 * @param  {Object} options  The parameters to be passed; options may not be passed when GET, the default is {}
 * @param  {string} options  POST, DELETE, GET, PUT
 * @param  {Object} options  Parameters that need to be passed to the backend, such as {id: 1}
 * @param  {boolean} options Whether to upload a file, if you upload a file, you do not need the default headers and convert the body to a string
 * @param  {boolean} tipSuccess false: Do not show successful update true: show successful update
 * @param  {boolean} tipError  false: Don't prompt error true: display error message
 * @param  {boolean} fullResponse false: Whether to return all requested information
 * @return {Object}
 */
export default async function request(url, options = {}, tipSuccess = false, tipError = true, fullResponse = false) {
    if(!basePath){
        const data = await fetch('/api/basepath')
        basePath = await data.json();
    }
    const newOptions = {credentials: 'include', ...options};
    if (newOptions.method === 'POST' || newOptions.method === 'PUT') {
        newOptions.headers = newOptions.isUpload
            ? {
                ...newOptions.headers
            }
            : {
                'Content-Type': 'application/json; charset=utf-8',
                ...newOptions.headers
            };
    }
    if (typeof newOptions.body === 'object' && !newOptions.isUpload) {
        newOptions.body = JSON.stringify(newOptions.body);
    }
    if (basePath.data?.path && basePath.data?.enable) {
        url = basePath.data.path + url
    }
    const response = await fetch(url, newOptions);
    if (
        response.url.includes('dataIntegrationApi')
        && (
            newOptions.method === 'PUT'
            || newOptions.method === 'POST'
            || newOptions.method === 'DELETE'
        )
    ) {
        return response;
    }
    checkStatus(response);

    if (options && options.download) {
        return response.blob();
    }
    const data = await response.json();
    if ('code' in data || 'msg' in data) {
        const code = data.code;
        const msg = data.msg;
        if (msg === 'success' || code === 0 || code === 200) {
            if (tipSuccess) {
                message.success(<Trans>successfulOperation</Trans>, msg);
            }
        } else if (tipError && code !== 0 && msg !== 'success') {
            message.error(msg);
        }
    }

    if (fullResponse) {
        return {data, response};
    }
    return data;
}
