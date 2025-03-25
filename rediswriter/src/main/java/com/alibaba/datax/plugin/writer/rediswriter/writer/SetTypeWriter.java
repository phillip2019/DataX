package com.alibaba.datax.plugin.writer.rediswriter.writer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswriter.Key;
import com.alibaba.datax.plugin.writer.rediswriter.RedisWriteAbstract;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SET类型写入Redis
 */
public class SetTypeWriter extends RedisWriteAbstract {
    private static final Logger LOG = LoggerFactory.getLogger(SetTypeWriter.class);
    
    private String valueDelimiter;

    public SetTypeWriter(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void checkAndGetParams() {
        super.checkAndGetParams();
        Configuration detailConfig = this.configuration.getConfiguration(Key.CONFIG);
        
        // SET类型可以选择使用分隔符来分割一条记录为多个成员添加到集合中
        valueDelimiter = detailConfig.getString(Key.SET_VALUE_DELIMITER, null);
    }

    @Override
    public void addToPipLine(RecordReceiver lineReceiver) {
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            String redisKey;
            
            // 构建Redis键
            if (null != keyIndex) {
                String key = record.getColumn(keyIndex).asString();
                redisKey = keyPrefix + key + keySuffix;
            } else {
                redisKey = keyPrefix + strKey + keySuffix;
            }
            
            // 获取值并添加到SET
            Column valueColumn = record.getColumn(valueIndex);
            if (valueColumn == null) {
                LOG.error("获取valueIndex={}对应的值失败，请检查配置是否正确!!!", valueIndex);
                throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, 
                    String.format("获取valueIndex=%d对应的值失败，请检查配置是否正确!!!", valueIndex));
            }
            
            String columnValue = valueColumn.asString();
            
            // 如果有分隔符，则将值拆分为多个成员
            if (StringUtils.isNotBlank(valueDelimiter)) {
                String[] members = columnValue.split(valueDelimiter);
                for (String member : members) {
                    if (StringUtils.isNotBlank(member)) {
                        String redisValue = valuePrefix + member.trim() + valueSuffix;
                        LOG.debug("添加成员到SET: key={}, member={}", redisKey, redisValue);
                        pipelined.sadd(redisKey, redisValue);
                    }
                }
            } else {
                // 不需要拆分，直接添加单个成员
                String redisValue = valuePrefix + columnValue + valueSuffix;
                LOG.debug("添加成员到SET: key={}, member={}", redisKey, redisValue);
                pipelined.sadd(redisKey, redisValue);
            }
            
            // 设置过期时间（如果需要）
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
