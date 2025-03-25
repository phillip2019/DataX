# Redis SET类型写入说明

## 功能描述

Redis SET类型支持用于向Redis中写入无序集合数据。适用于标签、用户权限、特征集合等无序且不重复的数据集场景。

## 配置说明

### 基本配置

```json
{
  "writer": {
    "name": "rediswriter",
    "parameter": {
      "redisMode": "singleton",      // Redis模式: singleton(单机), cluster(集群)
      "address": "127.0.0.1:6379",   // Redis地址
      "auth": "",                    // Redis密码
      "writeType": "set",            // 写入类型: 使用set
      "writeMode": "insert",         // 写入模式: insert(插入), delete(删除)
      "config": {
        "colKey": {                  // 用作Key的列
          "index": 0,                // 列索引
          "name": "key"              // 列名称
        },
        "colValue": {                // 用作Value(成员)的列
          "index": 1,
          "name": "value"
        },
        "valueDelimiter": ",",       // 值分隔符(可选)，用于拆分一个值为多个成员
        "keyPrefix": "prefix:",      // 键前缀(可选)
        "keySuffix": "",             // 键后缀(可选)
        "valuePrefix": "",           // 值前缀(可选)
        "valueSuffix": "",           // 值后缀(可选)
        "expire": 86400,             // 过期时间(秒)，-1表示永不过期
        "batchSize": 1000            // 批处理大小
      }
    }
  }
}
```

### 重要参数说明

| 参数 | 说明 | 是否必选 | 默认值 |
| ---- | ---- | -------- | ------ |
| writeType | 设置为"set"表示使用Set类型 | 是 | 无 |
| colKey | 指定用作Redis键的列 | 与strKey二选一必填 | 无 |
| strKey | 静态键名，所有数据写入同一个键 | 与colKey二选一必填 | 无 |
| colValue | 指定用作Redis Set成员的列 | 是 | 无 |
| valueDelimiter | 值分隔符，用于将一个字段拆分为多个成员 | 否 | 无(不拆分) |

## 应用场景

### 1. 用户标签系统
```json
{
  "writeType": "set",
  "config": {
    "colKey": {"index": 0},   // 用户ID列
    "colValue": {"index": 1},  // 标签列
    "valueDelimiter": ",",     // 标签以逗号分隔
    "keyPrefix": "user:tags:"  // 键前缀
  }
}
```

输入数据:
```
user001,sports,finance,tech
```

写入Redis后:
```
SMEMBERS user:tags:user001
1) "sports"
2) "finance"
3) "tech"
```

### 2. 权限管理系统
```json
{
  "writeType": "set",
  "config": {
    "colKey": {"index": 0},   // 角色ID列
    "colValue": {"index": 1},  // 权限列
    "valueDelimiter": "|",     // 权限以|分隔
    "keyPrefix": "role:perms:" // 键前缀
  }
}
```

### 3. IP黑名单
```json
{
  "writeType": "set",
  "config": {
    "strKey": "blacklist:ip",  // 使用固定键
    "colValue": {"index": 0}   // IP地址列
  }
}
```

## 性能优化建议

1. 合理设置`batchSize`参数，通常建议1000-5000之间
2. 使用`valueDelimiter`可以减少Redis网络请求次数
3. 对于大量成员的Set，建议使用较短的成员值减少内存占用
4. 合理设置过期时间，避免数据无限增长

## 注意事项

1. SET类型会自动去除重复成员，相同的值只会存储一次
2. SET类型不保证成员顺序
3. SET类型适合频繁查询成员是否存在的场景
4. 当`valueDelimiter`为空时，整个字段值作为一个成员添加
