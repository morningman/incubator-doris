---
{
    "title": "Query Action",
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

# Query Action


## Request

```
POST /api/query/<ns_name>/<db_name>
```

## Description

Query Action 用于指定 SQL 查询并返回结果。
    
## Path parameters

* `<db_name>`

    指定数据库名称。该数据库会被视为当前session的默认数据库，如果在 SQL 中的表名没有限定数据库名称的话，则使用该数据库。

## Query parameters

无

## Request body

```
{
    "sql" : "select * from tbl1",
    "variables": {
        "exec_mem_limit" : 2147483648
    }
}
```

* sql 字段为具体的 SQL
* variables 字段为一些需要设置的会话变量

### Response

```
{
	"msg": "OK",
	"code": 0,
	"data": [{
		"meta": [{
				"name": "col1",
				"type": "INT"
			},
			{
				"name": "col2",
				"type": "BIGINT"
			}
		],
		"data": [
			[1, 2],
			[1, 3],
			[2, 4]
		],
		"status": [{
			"server_status": 2,
			"warning_count": 0
		}]
	}],
	"count": 3
}
```

* meta 字段描述返回的列信息
* data 字段返回结果行。其中每一行的中的列类型，需要通过 meta 字段内容判断。
* status 字段返回 MySQL 的一些信息，如告警行数，状态码等。