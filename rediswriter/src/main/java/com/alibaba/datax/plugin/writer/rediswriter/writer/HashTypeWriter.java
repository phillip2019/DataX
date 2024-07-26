package com.alibaba.datax.plugin.writer.rediswriter.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswriter.Key;
import com.alibaba.datax.plugin.writer.rediswriter.RedisWriteAbstract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.datax.plugin.writer.rediswriter.Constant.HASH_KEY_DYNAMIC;
import static com.alibaba.datax.plugin.writer.rediswriter.Constant.HASH_KEY_STATIC;

/**
 * @author lijf@2345.com
 * @date 2020/5/19 16:18
 * @desc hash类型写redis
 */
public class HashTypeWriter extends RedisWriteAbstract {
    private static final Logger LOG = LoggerFactory.getLogger(HashTypeWriter.class);

    List<Configuration> hashFieldIndexs;

    String hashKey;

    public HashTypeWriter(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void checkAndGetParams() {
        super.checkAndGetParams();
        Configuration detailConfig = configuration.getConfiguration(Key.CONFIG);
        // hash类型默认以数据源对应的列名作为hash的filed名
        hashFieldIndexs = detailConfig.getListConfiguration(Key.COLVALUE);
        hashKey = detailConfig.getString(Key.HASH_KEY);
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

            String nameValue;
            // hash类型以数据源column中值作为hash key
            for (Configuration hashFieldIndex : hashFieldIndexs) {
                nameValue = null;
                if (HASH_KEY_DYNAMIC.equals(hashKey)) {
                    Integer nameIndex = hashFieldIndex.getInt(Key.COL_NAME);
                    nameValue = record.getColumn(nameIndex).asString();
                } else if (HASH_KEY_STATIC.equals(hashKey)) {
                    nameValue = hashFieldIndex.getString(Key.COL_NAME);
                } else {
                    LOG.info("错误hashKey类型， 当前hashKey类型为: {}, 允许的hashKey为: {}、{}", hashKey, HASH_KEY_DYNAMIC, HASH_KEY_STATIC);
                    System.exit(-1);
                }
                Integer index = hashFieldIndex.getInt(Key.COL_INDEX);
                String value = record.getColumn(index).asString();
                value = valuePrefix + value + valueSuffix;
                m.put(nameValue, value);
            }
            // 先删除旧数据
            pipelined.unlink(redisKey);
            pipelined.hset(redisKey, m);

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
