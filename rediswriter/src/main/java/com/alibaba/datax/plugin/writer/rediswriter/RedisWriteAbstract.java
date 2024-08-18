package com.alibaba.datax.plugin.writer.rediswriter;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.List;


/**
 * @author lijf@2345.com
 * @date 2020/5/19 15:56
 * @desc 写redis对象抽象类
 */
public abstract class RedisWriteAbstract {
    private static final Logger logger = LoggerFactory.getLogger(RedisWriteAbstract.class);

    protected Configuration configuration;
    protected PipelineBase pipelined;
    Object redisClient;
    protected int records;
    protected String keyPrefix;
    protected String keySuffix;
    protected String valuePrefix;
    protected String valueSuffix;
    protected Integer batchSize;
    protected Long expire;
    protected String strKey;
    protected Integer keyIndex;
    protected Integer valueIndex;

    public RedisWriteAbstract(Configuration configuration) {
        this.configuration = configuration;
    }


    public PipelineBase getRedisPipelineBase(Configuration configuration) {
        String mode = configuration.getNecessaryValue(Key.REDISMODE, CommonErrorCode.CONFIG_ERROR);
        String addr = configuration.getNecessaryValue(Key.ADDRESS, CommonErrorCode.CONFIG_ERROR);
        String auth = configuration.getString(Key.AUTH);
        Integer db = configuration.getInt(Key.DB, 0);
        if (Constant.CLUSTER.equalsIgnoreCase(mode)) {
            redisClient = RedisWriterHelper.getJedisCluster(addr, auth);
        } else {
            redisClient = RedisWriterHelper.getJedis(addr, auth, db);
        }
        return RedisWriterHelper.getPipeLine(redisClient);
    }

    /**
     * 初始化公共参数
     */
    public void initCommonParams() {
        Configuration detailConfig = this.configuration.getConfiguration(Key.CONFIG);
        batchSize = detailConfig.getInt(Key.BATCH_SIZE, 1000);
        keyPrefix = detailConfig.getString(Key.KEY_PREFIX, "");
        keySuffix = detailConfig.getString(Key.KEY_SUFFIX, "");
        valuePrefix = detailConfig.getString(Key.VALUE_PREFIX, "");
        valueSuffix = detailConfig.getString(Key.VALUE_SUFFIX, "");
        expire = detailConfig.getLong(Key.EXPIRE, Integer.MAX_VALUE);
        pipelined = getRedisPipelineBase(configuration);
    }

    /**
     * 检查和解析参数
     */
    public void checkAndGetParams() {
        Configuration detailConfig = configuration.getConfiguration(Key.CONFIG);

        String colKey = detailConfig.getString(Key.COLKEY, null);
        String strKey = detailConfig.getString(Key.STRING_KEY, null);

        if ((StringUtils.isBlank(colKey) && StringUtils.isBlank(strKey))) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "strKey或colKey不能为空！请检查配置");
        }
        if ((StringUtils.isNotBlank(colKey) && StringUtils.isNotBlank(strKey))) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "strKey或colKey不能同时存在！请检查配置");
        }

        if (StringUtils.isNotBlank(colKey)) {
            keyIndex = detailConfig.getConfiguration(Key.COLKEY).getInt(Key.COL_INDEX);
        } else {
            this.strKey = strKey;
        }
        String colValue = detailConfig.getString(Key.COLVALUE, null);
        if (StringUtils.isNotBlank(colKey) && StringUtils.isBlank(colValue)) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "colValue不能为空！请检查配置");
        }
        String writeType = configuration.getString(Key.WRITE_TYPE);
        // hash类型的colValue配置里面有多个column，要考虑排除获取valueIndex，HashTypeWriter子类单独处理
        if (!Constant.WRITE_TYPE_HASH.equalsIgnoreCase(writeType)) {
            valueIndex = detailConfig.getConfiguration(Key.COLVALUE).getInt(Key.COL_INDEX);
        }
    }

    /**
     * 把数据add到pipeline
     *
     * @param lineReceiver PipelineBase
     */
    public abstract void addToPipLine(RecordReceiver lineReceiver);

    /**
     * 先删除老数据
     **/
    public void deleteOldData() {
        logger.info("Start delete old data, keyPrefix: {}", keyPrefix);
        List<String> keyResultList = new ArrayList<>();
        String cursor = ScanParams.SCAN_POINTER_START;

        ScanParams params = new ScanParams().count(batchSize).match(keyPrefix + "*");

        do {
            ScanResult<String> scanResult;
            if (redisClient instanceof Jedis) {
                scanResult = ((Jedis) redisClient).scan(cursor, params);
            } else if (redisClient instanceof JedisCluster) {
                scanResult = ((JedisCluster) redisClient).scan(cursor, params);
            } else {
                throw new IllegalArgumentException("Unsupported JedisCommands implementation");
            }
            List<String> keys = scanResult.getResult();
            keyResultList.addAll(keys);
            cursor = scanResult.getCursor();
            // 如果游标值为0，表示遍历完整个数据库，退出循环
        } while (!"0".equals(cursor));

        if (keyResultList.isEmpty()) {
            return;
        }
        logger.info("Delete old data size: {}", keyResultList.size());
        // 删除所有keyResultList,按批次删除，一批batchSize条，使用pipelined
        for (int i = 0; i < keyResultList.size(); i += batchSize) {
            List<String> subList = keyResultList.subList(i, Math.min(i + batchSize, keyResultList.size()));
            for (String key : subList) {
                pipelined.del(key);
                records++;
            }
            this.syncData();
            logger.info("Delete old data, total size: {}, process percentage: {}", keyResultList.size(), (i + batchSize) * 100.0 / keyResultList.size());
        }
        logger.info("End delete old data, keyPrefix: {}, deleted size: {}", keyPrefix, keyResultList.size());
    }

    /**
     * 正式写入数据到redis
     */
    public void syncData() {
        // 若数据大于0，开始写入同步数据
        if (records > batchSize) {
            RedisWriterHelper.syncData(pipelined);
            records = 0;
        }
    }


    public void syncAllData() {
        RedisWriterHelper.syncData(pipelined);
    }

    /**
     * 关闭资源
     */
    public void close() {
        RedisWriterHelper.syncData(pipelined);
        RedisWriterHelper.close(redisClient);
    }
}
