package com.alibaba.datax.plugin.writer.rediswriter.writer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswriter.RedisWriteAbstract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author lijf@2345.com
 * @date 2020/5/19 16:15
 * @desc string类型写redis
 */
public class StringTypeWriter extends RedisWriteAbstract {
    private static final Logger LOG = LoggerFactory.getLogger(StringTypeWriter.class);

    public StringTypeWriter(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void checkAndGetParams() {
        super.checkAndGetParams();
    }

    @Override
    public void addToPipLine(RecordReceiver lineReceiver) {
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            String redisKey;
            String redisValue;

            Column valueColumn = record.getColumn(valueIndex);
            if (valueColumn == null) {
                LOG.error("获取valueIndex={}对应的值失败，请检查配置是否正确!!!", valueIndex);
                throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, String.format("获取valueIndex=%d对应的值失败，请检查配置是否正确!!!", valueIndex));
            }
            redisValue = valueColumn.asString();
            if (null != keyIndex) {
                String key = record.getColumn(keyIndex).asString();
                redisKey = keyPrefix + key + keySuffix;

            } else {
                redisKey = keyPrefix + strKey + keySuffix;
            }
            redisValue = valuePrefix + redisValue + valueSuffix;
            LOG.info("开始写入redisKey={}, redisValue: {}...", redisKey, redisValue);
            pipelined.set(redisKey, redisValue);
            LOG.info("写入redisKey={}, redisValue: {}完成", redisKey, redisValue);
            // 若expire为-1，则设置此redisKey永不过期
            if (expire == -1) {
                pipelined.persist(redisKey);
            } else {
                pipelined.expire(redisKey, expire);
            }
            records++;
        }
    }

}
