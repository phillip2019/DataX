# DataX RedisWriter 说明


------------

## 1 快速介绍

RedisWriter提供了写入redis内存数据的能力。支持各种异构数据源MySQL、Oracle、SqlServer、PostgreSQL、Hive、HBase...导入redis，考虑到性能，
本插件做了pipeline批量写redis。

**本地文件写入内容为hash格式二维表**


## 2 功能与限制

rediswriter支持的功能有：

1. 导入数据到redis（默认）

2. 根据数据源删除redis key

3. 根据数据源删除hash类型的field

rediswriter支持导入redis的数据类型有：

1. string

2. list

3. hash

## 3 功能说明


### 3.1 配置样例

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3
      }
    },
    "content": [
      {
        "reader": {
          "name": "hdfsreader",
          "parameter": {
            "path": "/user/hive/warehouse/test.db/redis_writer",
            "defaultFS": "hdfs://xxxx:8020",
            "column": [
              {
                "index": 0,
                "name": "uid",
                "type": "string"
              },
              {
                "index": 1,
                "name": "channels",
                "type": "string"
              },
              {
                "index": 2,
                "name": "name",
                "type": "string"
              }],
            "fileType": "text",
            "encoding": "UTF-8",
            "fieldDelimiter": "\t"
          }
        },
        "writer": {
          "name": "rediswriter",
          "parameter": {
            "redisMode": "cluster",
            "address": "xxxxxx:6379,xxxxxx:6479,xxxxxx:6379,xxxxxx:6479,xxxxxx:6379,xxxxxx:6479",
            "auth": "Pye9WQAYsgetVrLw",
            "writeType":"hash",
            "config":{
              "colKey":{"name":"uid","index":0},
              "colValue":[{"name":1, "index":2}],
              "valueDelimiter": ",",
              "hashKey": "dynamic",
              "expire": 300,
              "keyPrefix":"datax:hash:"
            }
          }
        }
      }
    ]
  }
}
```

### 3.2 参数说明

### 3.2.1 一级参数

* **redisMode**

	* 描述：redis的部署模式，支持集群模式和单机模式，值：cluster或singleton <br />

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **address**

	* 描述：redis的地址，单机模式：host:port，集群模式：host1:port1,host2:port2,  <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **auth**

	* 描述：redis密码，没有则不加这个参数	  <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **db**

	* 描述：redis database, 写入redis的对应数据库， 请注意，此参数只在单机模式生效，集群默认只能写入db 0	  <br />

	* 必选：否 <br />

	* 默认值：0 <br />

* **writeType**

	* 描述：写入redis的数据类型：string、list、hash	  <br />

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **writeMode**

	* 描述：写入的模式，默认是写数据，设为delete是删数据		  <br />

	* 必选：否 <br />

	* 默认值：默认insert <br />


#### 3.2.2 二级参数

* **strKey**

	* 描述：(公共参数)自定义的redis key值，不通过数据源来定。 <br />

	    strKey和colKey二选一

	* 必选：否 <br />

	* 默认值： 无 <br />


* **colKey**

	* 描述：(公共参数)对应数据源的column，作为redis的key	。 <br />

	    strKey和colKey二选一

	* 必选：否 <br />

	* 默认值：无 <br />

* **expire**

	* 描述：(公共参数)redis key的过期时间，单位秒 <br />

	* 必选：否 <br />

	* 默认值：-1 <br />

* **hashKey**

	* 描述：(公共参数)redis hash key的模式，可选值为: dynamic(动态);  static(静态)<br />

	* 必选：否 <br />

	* 默认值：dynamic <br />	

* **batchSize**

	* 描述：(公共参数)pipline批量每次导入redis的的大小。 <br />

	* 必选：否 <br />

	* 默认值：1000 <br />

* **keyPrefix**

	* 描述：(公共参数)redis key值的自定义前缀。 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **keySuffix**

	* 描述：(公共参数)redis key值的自定义后缀。 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **colValue**

	* 描述：(公共参数)redis value值对应的数据源列配置。 <br />
	    
	    ```json
        	{
            	"colValue": [{
            		"name": "channels",
            		"index": 1
            	}, {
            		"name": "name",
            		"index": 2
            	}]
            }
        ```
	    name    (公共参数)对应的数据源列名  必须  <br />
	    index   (公共参数)对应的数据源列索引	  必须

	* 必选：除writeMode为delete时必需 <br />

	* 默认值：无 <br />

* **valueDelimiter**

	* 描述：(redis list类型参数)对应数据源column值的分隔符,只支持string类型的数据源column。 <br />

	* 必选：writeType为list时必需 <br />

	* 默认值：无 <br />

* **pushType**

	* 描述：(redis list类型参数)list类型的push类型，有lpush，rpush，overwrite，默认overwrite。 <br />

	* 必选：否 <br />

	* 默认值：overwrite <br />

* **hashFields**

	* 描述：(redis hash类型参数)hash类型要删除的field，逗号隔开，次参数只对删除hash类型的field时有效。 <br />

	* 必选：删除hash类型field时必需 <br />

	* 默认值：无 <br />


### 3.3 类型转换


## 4 性能报告



## 5 约束限制

略

## 6 FAQ

略


