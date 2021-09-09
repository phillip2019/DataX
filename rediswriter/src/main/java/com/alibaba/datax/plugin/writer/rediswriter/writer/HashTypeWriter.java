package com.alibaba.datax.plugin.writer.rediswriter.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswriter.Key;
import com.alibaba.datax.plugin.writer.rediswriter.RedisWriteAbstract;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lijf@2345.com
 * @date 2020/5/19 16:18
 * @desc hash类型写redis
 */
public class HashTypeWriter extends RedisWriteAbstract {
    List<Configuration> hashFieldIndexs;

    public HashTypeWriter(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void checkAndGetParams() {
        super.checkAndGetParams();
        Configuration detailConfig = configuration.getConfiguration(Key.CONFIG);
        // hash类型默认以数据源对应的列名作为hash的filed名
        hashFieldIndexs = detailConfig.getListConfiguration(Key.COLVALUE);
    }

    @Override
    public void addToPipLine(RecordReceiver lineReceiver) {
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            String redisKey;
            if (null != keyIndex) {
                String key = record.getColumn(keyIndex).asString();
                redisKey = keyPrefix + key + keySuffix;
            } else {
                redisKey = keyPrefix + strKey + keySuffix;
            }
            Map<String, String> m = new HashMap<>(hashFieldIndexs.size());
            m.clear();
            // hash类型已数据源column名作为filed
            for (Configuration hashFieldIndex : hashFieldIndexs) {
                String filed = hashFieldIndex.getString(Key.COL_NAME);
                Integer index = hashFieldIndex.getInt(Key.COL_INDEX);
                String value = record.getColumn(index).asString();
                value = valuePrefix + value + valueSuffix;
                m.put(filed, value);
            }
            pipelined.hmset(redisKey, m);

            // 若expire为-1，则设置此redisKey永不过期
            if (expire != -1) {
                pipelined.expire(redisKey, expire);
            }

            records++;
            // 若records超过batchSize，则开始同步写入
            if (records >= batchSize) {
                syncData();
            }
        }
    }

}
