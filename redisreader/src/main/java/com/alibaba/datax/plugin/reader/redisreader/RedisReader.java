package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;

import com.alibaba.datax.plugin.reader.redisreader.JsonStorageReaderUtil.RedisKeyTypeEnum;
import com.google.common.collect.Collections2;
import redis.clients.jedis.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;

import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;

/**
 * Created by crabo yang on 17-8-31.
 */
public class RedisReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        private String host = null;
        private String auth = null;
        private int port = 6897;
        private int readBatchCount = 5000;
        private boolean testOnBorrow;

        private List<String> listKeys = null;

        private int connectionTimeout = -1;
        public static JedisPool JEDIS_POOL;

        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();

            host = this.originConfig.getString(Key.HOST);
            auth = this.originConfig.getString(Key.AUTH_PWD);
            port = this.originConfig.getInt(Key.PORT);
            testOnBorrow = this.originConfig.getBool(Key.TEST_ON_BORROW, false);

            listKeys = this.originConfig.getList(Key.ListKey, String.class);

            readBatchCount = this.originConfig.getInt(Key.READ_BATCH_SIZE, 5000);
            connectionTimeout = this.originConfig.getInt(Key.CONN_TIMEOUT);
        }

        @Override
        public void prepare() {
            LOG.debug("prepare() begin...");
            if (listKeys.isEmpty())
                throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR
                        , "No target Keys defined in config.");
            // 生成连接池配置信息
            JedisPoolConfig config = new JedisPoolConfig();
            // 	资源池允许最大空闲的连接数， 默认8
            config.setMaxIdle(10);
            // 资源池中最大连接数, 默认8
            config.setMaxTotal(30);
            // 资源池确保最少空闲的连接数
            config.setMinIdle(0);
            // 当资源池用尽后，调用者是否要等待。只有当为true时，下面的maxWaitMillis才会生效，默认true
            config.setBlockWhenExhausted(true);
            // 最大等待时间，毫秒，默认-1：表示永不超时
            config.setMaxWaitMillis(5 * 1000);
            //向资源池借用连接时是否做连接有效性检测(ping)，无效连接会被移除
            if (testOnBorrow)
                config.setTestOnBorrow(true);

            // 是否开启jmx监控，可用于监控
            config.setJmxEnabled(true);

            // 在应用初始化的时候生成连接池
            JEDIS_POOL = new JedisPool(config, host, port, connectionTimeout);
            LOG.info("您即将读取的listKeys length为: [{}], \n {}", this.listKeys.size(), this.listKeys);
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            if (JEDIS_POOL != null && !JEDIS_POOL.isClosed()) {
                JEDIS_POOL.close();
                JEDIS_POOL.destroy();
                JEDIS_POOL = null;
            }
        }

        // warn: 如果源目录为空会报错，拖空目录意图=>空文件显示指定此意图
        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

            // warn:每个slice拖且仅拖一个文件,
            // int splitNumber = adviceNumber;
            int splitNumber = this.listKeys.size();
            if (0 == splitNumber) {
                throw DataXException.asDataXException(
                        CommonErrorCode.CONFIG_ERROR, String
                                .format("未能找到待读取的主题,请确认您的配置项listKey: %s",
                                        this.originConfig.getString(Key.ListKey)));
            }

            // scan match pattern
            List<String> targetListKeys = sourceTopic2TargetTopic(this.listKeys);
            LOG.info("source list keys size: {}, target keys size: {}", this.listKeys.size(), targetListKeys.size());

            List<List<String>> splitedSourceFiles = this.splitSourceFiles(
                    targetListKeys, splitNumber);

            for (List<String> topics : splitedSourceFiles) {
                Configuration splitedConfig = this.originConfig.clone();
                splitedConfig.set(Constant.SOURCE_KEYS, topics);
                splitedConfig.set(Key.AUTH_PWD, auth);
                readerSplitConfigs.add(splitedConfig);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        /**
         * @description 提取redis 模式匹配key
         * @author xiaowei.song
         * @param sourceList pattern key
         * @return targetList target string key
         **/
        private List<String> sourceTopic2TargetTopic(final List<String> sourceList) {
            List<String> targetList = new ArrayList<String>();
            Jedis client = getRedisClient();
            ScanParams sp = null;
            ScanResult<String> scanResult = null;
            for (String s : sourceList) {
                sp = new ScanParams();
                sp.count(readBatchCount);
                sp.match(s);

                String cursor = ScanParams.SCAN_POINTER_START;
                do {
                    scanResult = client.scan(cursor, sp);
                    List<String> keys = scanResult.getResult();
                    if(keys != null && !keys.isEmpty()) {
                        targetList.addAll(keys);
                    }
                } while (!(cursor = scanResult.getStringCursor()).equals(SCAN_POINTER_START));
            }
            return targetList;
        }

        private <T> List<List<T>> splitSourceFiles(final List<T> sourceList,
                                                   int adviceNumber) {
            List<List<T>> splitedList = new ArrayList<List<T>>();
            int averageLength = sourceList.size() / adviceNumber;
            averageLength = averageLength == 0 ? 1 : averageLength;

            for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
                end = begin + averageLength;
                if (end > sourceList.size()) {
                    end = sourceList.size();
                }
                splitedList.add(sourceList.subList(begin, end));
            }
            return splitedList;
        }

        private Jedis getRedisClient() {
            Jedis redisClient = null;
            try {
                redisClient = Job.JEDIS_POOL.getResource();

                if (this.originConfig.getString(Key.AUTH_PWD) != null)
                    redisClient.auth(this.originConfig.getString(Key.AUTH_PWD));

            } catch (Exception e) {
                LOG.error("Can't create redis from pool", e);
                throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR
                        , "Can't create redis from pool");
            }
            return redisClient;
        }
    }

    public static class Task extends Reader.Task {
        private static Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private List<String> sourceKeys;
        private int readBatchSize;

        @Override
        public void init() {
            this.readerSliceConfig = this.getPluginJobConf();
            this.sourceKeys = this.readerSliceConfig.getList(
                    Constant.SOURCE_KEYS, String.class);

            this.readBatchSize = this.readerSliceConfig.getInt(
                    Key.READ_BATCH_SIZE, 5000);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {
        }

        private Jedis getRedisClient() {
            Jedis redisClient = null;
            try {
                redisClient = Job.JEDIS_POOL.getResource();

                if (readerSliceConfig.getString(Key.AUTH_PWD) != null)
                    redisClient.auth(readerSliceConfig.getString(Key.AUTH_PWD));

            } catch (Exception e) {
                LOG.error("Can't create redis from pool", e);
                throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR
                        , "Can't create redis from pool");
            }
            return redisClient;
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.debug("start read redis topic...");
            List<ColumnEntry> columns = JsonStorageReaderUtil.getListColumnEntry(readerSliceConfig, Key.COLUMN);
            TaskPluginCollector collector = super.getTaskPluginCollector();
            Jedis client = getRedisClient();
            String cursor = SCAN_POINTER_START;
            ScanParams sp = null;
            String keyType = null;
            ScanResult sr = null;
            for (String cacheKey : this.sourceKeys) {
                LOG.debug("reading redis cache key: [{}]", cacheKey);
                List<String> json = null;

                sp = new ScanParams();
                sp.count(readBatchSize);
                sp.match(cacheKey);

                cursor = SCAN_POINTER_START;

                try {
                    keyType = client.type(cacheKey);
                    do {
                        switch (keyType) {
                            case "hash":
                                sr = client.hscan(cacheKey, cursor, sp);
                                break;
                            default:
                                sr = client.sscan(cacheKey, cursor, sp);
                                break;
                        }
                        //读取头部批量数据

                        json = client.lrange(cacheKey, batch_start_pos, batch_start_pos + readBatchSize - 1);
                        for (String row : json) {
                            JsonStorageReaderUtil.transportOneRecord(recordSender, collector,
                                    columns, row);
                        }
                        i++;
                        if (i > 5) {//每一批次flush成功，则从redis删除本批次
                            recordSender.flush();
                            //仅保留从头部起 [batch*i, END] 之间的数据
                            client.ltrim(cacheKey, batch_start_pos, -1);
                            i = 0;
                        }
                    } while (!(cursor = sr.getStringCursor()).equals(SCAN_POINTER_START));

                    if (i > 0) {
                        recordSender.flush();
                        client.ltrim(cacheKey, readBatchSize * i, -1);
                    }
                } catch (Exception e) {
                    String message = String
                            .format("error reading redis : [%s]", cacheKey);
                    LOG.error(message, e);

                    throw DataXException.asDataXException(
                            CommonErrorCode.RUNTIME_ERROR, message);
                } finally {
                    client.close();
                }
            }
            LOG.debug("end read redis ...");
        }

    }
}
