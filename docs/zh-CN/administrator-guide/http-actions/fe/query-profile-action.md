---
{
    "title": "QUERY PROFILE ACTION",
    "language": "zh-CN"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Query Profile Action

## Request

```
GET /rest/v1/query_profile/<query_id>
```

## Description

Query Profile Action 用于获取 Query 的 profile。
    
## Path parameters

* `<query_id>`

    可选参数。当不指定时，返回最新的 query 列表。当指定时，返回指定 query 的 profile。

## Query parameters

无

## Request body

无

## Response

* 不指定 `<query_id>`

    ```
    {
    	"msg": "success",
    	"code": 0,
    	"data": {
    		"columns_names": ["User", "Default Db", "SQL", "Query Type", "Start Time", "End Time", "Cost", "Query State", "Profile"],
    		"href_column": "Profile",
    		"rows": [{
    			"User": "root",
    			"Query Type": "Query",
    			"Total": "316ms",
    			"QueryId": "fbb2582c72884d29-9dd9de3b42b740e9",
    			"Default Db": "default_cluster:db1",
    			"Sql Statement": "select count(*) from store_sales5",
    			"Start Time": "2020-08-26 16:27:20",
    			"Query State": "EOF",
    			"End Time": "2020-08-26 16:27:20",
    			"__hrefPath": "/query_profile/query_id=fbb2582c72884d29-9dd9de3b42b740e9"
    		}]
    	},
    	"count": 1
    }
    ```
    
    返回结果同 `System Action`。
    
* 指定 `<query_id>`

    ```
    {
    	"msg": "success",
    	"code": 0,
    	"data": "Query:</br>&nbsp;&nbsp;&nbsp;&nbsp;Summary:</br>...",
    	"count": 0
    }
    ```
    
    `data` 为 profile 的文本内容。