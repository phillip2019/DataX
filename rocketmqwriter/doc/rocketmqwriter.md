# DataX RocketMQWriter 说明


------------

## 1 快速介绍

RocketMQWriter提供了将数据写入到RocketMQ消息队列的能力。支持将各种异构数据源如MySQL、Oracle、SqlServer、PostgreSQL、Hive、HBase等的数据写入到RocketMQ，考虑到性能和消息的可靠性，插件提供了多种发送模式和批量写入的功能。

**插件支持多种消息序列化格式，灵活满足不同业务需求**


## 2 功能与限制

RocketMQWriter支持的功能有：

1. 异步批量发送模式（默认，提供最高吞吐量）
2. FIFO顺序发送模式（保证消息顺序）
3. 延时消息发送模式（按照设定的延时级别发送）

RocketMQWriter支持的消息序列化格式有：

1. JSON格式（默认）
2. 字符串格式
3. 二进制格式

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
          "name": "mysqlreader",
          "parameter": {
            "username": "root",
            "password": "root",
            "column": ["id", "name", "age", "email"],
            "connection": [
              {
                "table": ["user"],
                "jdbcUrl": ["jdbc:mysql://localhost:3306/test"]
              }
            ]
          }
        },
        "writer": {
          "name": "rocketmqwriter",
          "parameter": {
            "namesrvAddr": "localhost:9876",
            "topic": "user_topic",
            "tag": "datax_tag",
            "producerGroup": "datax_producer_group",
            "mode": "normal",
            "messageSerializer": "json",
            "maxBatchSize": 32,
            "maxRetryCount": 3,
            "sendTimeoutMillis": 3000,
            "keyField": "id",
            "column": ["id", "name", "age", "email"]
          }
        }
      }
    ]
  }
}
```

### 3.2 参数说明

#### 3.2.1 一级参数

* **namesrvAddr**

	* 描述：RocketMQ的NameServer地址，格式为host:port，多个地址用分号分隔 <br />

	* 必选：是 <br />

	* 默认值：无 <br />
	
* **topic**

	* 描述：RocketMQ的消息主题  <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **tag**

	* 描述：RocketMQ的消息标签，用于消息过滤	  <br />

	* 必选：否 <br />

	* 默认值：空字符串 <br />

* **producerGroup**

	* 描述：RocketMQ的生产者组名	  <br />

	* 必选：否 <br />

	* 默认值：datax_rocketmq_producer <br />

* **accessKey**

	* 描述：RocketMQ的ACL认证AccessKey	  <br />

	* 必选：否 <br />

	* 默认值：无 <br />
	
* **secretKey**

	* 描述：RocketMQ的ACL认证SecretKey		  <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **mode**

	* 描述：消息发送模式，支持normal（普通异步发送）、fifo（顺序发送）、delay（延时发送）	  <br />

	* 必选：否 <br />

	* 默认值：normal <br />

* **messageSerializer**

	* 描述：消息序列化器，支持json、string、bytes	  <br />

	* 必选：否 <br />

	* 默认值：json <br />


#### 3.2.2 高级参数

* **maxBatchSize**

	* 描述：单次批量发送的最大消息数量 <br />

	* 必选：否 <br />

	* 默认值：32 <br />


* **maxBatchByteSize**

	* 描述：单次批量发送的最大字节数	<br />

	* 必选：否 <br />

	* 默认值：4194304 (4MB) <br />

* **maxRetryCount**

	* 描述：消息发送失败时的最大重试次数 <br />

	* 必选：否 <br />

	* 默认值：3 <br />

* **sendTimeoutMillis**

	* 描述：消息发送的超时时间，单位毫秒 <br />

	* 必选：否 <br />

	* 默认值：3000 <br />	

* **keyField**

	* 描述：用作消息Key的字段名，用于消息查询和消息轨迹。<br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **delayLevel**

	* 描述：延时消息的延时级别，仅在mode为delay时有效。RocketMQ支持的延时级别为：1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h。<br />

	* 必选：mode为delay时必需 <br />

	* 默认值：0 <br />

* **column**

	* 描述：列名配置，用于指定哪些列会被写入到RocketMQ消息中。支持配置为["*"]，表示写入所有列。<br />

	* 必选：否 <br />

	* 默认值：["*"] <br />


### 3.3 消息序列化格式说明

#### 3.3.1 JSON格式（json）

将记录序列化为JSON对象，其中key为列名，value为列值。例如：

```json
{
  "column_0": "value1",
  "column_1": 123,
  "column_2": true
}
```

#### 3.3.2 字符串格式（string）

将记录的所有列值用逗号连接成字符串。例如：

```
value1,123,true
```

#### 3.3.3 二进制格式（bytes）

将记录的第一列数据转换为二进制格式。如果第一列是BYTES类型，则直接使用；否则将其转换为字符串后的UTF-8编码字节。


## 4 性能报告

### 4.1 环境准备

#### 4.1.1 测试环境

|  类别  |    名称     |
| ------ | ---------- |
|  CPU   | 8核 2.4GHz  |
|  内存   | 16GB        |
|  网络   | 千兆网卡     |
|  操作系统 | CentOS 7.6 |

#### 4.1.2 RocketMQ版本

RocketMQ 4.9.4

#### 4.1.3 DataX版本

DataX 3.0

### 4.2 测试结果

| 并发数 | 批量大小(条) | TPS  | 流量(MB/s) | CPU使用率(%) |
| ----- | ---------- | ---- | --------- | ----------- |
|   1   |     32     | 8000 |    2.4    |     20      |
|   3   |     32     | 23000|    7.0    |     45      |
|   5   |     32     | 35000|   10.8    |     75      |

## 5 约束限制

1. RocketMQ消息大小限制为4MB
2. 不同模式下的吞吐量表现不同，FIFO模式和延时消息模式的吞吐量会低于普通模式
3. 使用ACL认证时，确保RocketMQ版本支持ACL功能（4.4.0及以上版本）

## 6 FAQ

### 6.1 RocketMQWriter支持哪些类型的消息？

目前支持普通消息、顺序消息和延时消息。暂不支持事务消息。

### 6.2 如何处理发送失败的消息？

通过maxRetryCount参数可以设置消息发送失败时的重试次数。发送失败的记录会被收集为脏数据。

### 6.3 如何确保消息的顺序性？

使用mode参数设置为fifo模式可以确保消息按照相同的顺序发送到相同的队列中，从而保证消息的顺序性。

### 6.4 如何发送延时消息？

使用mode参数设置为delay模式，并通过delayLevel参数设置延时级别。

### 6.5 RocketMQWriter对RocketMQ版本有什么要求？

建议使用RocketMQ 4.8.0或更高版本，以获得更好的性能和功能支持。
