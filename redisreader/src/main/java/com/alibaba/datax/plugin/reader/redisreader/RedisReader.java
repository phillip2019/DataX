package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;

/**
 *
 * @author xiaowei.song
 * @date 2020-12-28
 */
public class RedisReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        private String host = null;
        private String auth = null;
        private int port = 6897;
        private int readBatchCount = 5000;
        private int db = 0;
        private boolean testOnBorrow;

        private List<String> listKeys = null;

        private int connectionTimeout = -1;

        private int maxIdle = 10;
        private int maxTotal = 30;
        private int minIdle = 0;
        private boolean blockWhenExhausted;
        private boolean jmxEnabled;

        public static JedisPool JEDIS_POOL;

        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();

            host = this.originConfig.getString(Key.HOST);
            auth = this.originConfig.getString(Key.AUTH_PWD);
            port = this.originConfig.getInt(Key.PORT);
            db = this.originConfig.getInt(Key.DB, 0);
            testOnBorrow = this.originConfig.getBool(Key.TEST_ON_BORROW, false);

            listKeys = this.originConfig.getList(Key.ListKey, String.class);

            readBatchCount = this.originConfig.getInt(Key.READ_BATCH_SIZE, 5000);
            connectionTimeout = this.originConfig.getInt(Key.CONN_TIMEOUT);
            maxIdle = this.originConfig.getInt(Key.MAX_IDLE, 10);
            maxTotal = this.originConfig.getInt(Key.MAX_TOTAL, 30);
            minIdle = this.originConfig.getInt(Key.MIN_IDLE, 0);
            blockWhenExhausted = this.originConfig.getBool(Key.BLOCK_WHEN_EXHAUSTED, true);
            jmxEnabled = this.originConfig.getBool(Key.JMX_ENABLED, true);
        }

        @Override
        public void prepare() {
            LOG.debug("prepare() begin...");
            if (listKeys.isEmpty()) {
                throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR
                        , "No target Keys defined in config.");
            }
            // 生成连接池配置信息
            JedisPoolConfig config = new JedisPoolConfig();
            // 	资源池允许最大空闲的连接数， 默认8
            config.setMaxIdle(maxIdle);
            // 资源池中最大连接数, 默认8
            config.setMaxTotal(maxTotal);
            // 资源池确保最少空闲的连接数
            config.setMinIdle(minIdle);
            // 当资源池用尽后，调用者是否要等待。只有当为true时，下面的maxWaitMillis才会生效，默认true
            config.setBlockWhenExhausted(jmxEnabled);
            // 最大等待时间，毫秒，默认-1：表示永不超时
            config.setMaxWaitMillis(5 * 1000);
            // 开启空闲连接检测
            config.setTestWhileIdle(true);
            config.setTestOnReturn(true);
            config.setNumTestsPerEvictionRun(-1);
            //向资源池借用连接时是否做连接有效性检测(ping)，无效连接会被移除
            if (testOnBorrow) {
                config.setTestOnBorrow(true);
            }

            // 是否开启jmx监控，可用于监控
            config.setJmxEnabled(jmxEnabled);

            // 在应用初始化的时候生成连接池
            JEDIS_POOL = new JedisPool(config, host, port, connectionTimeout, auth, db);
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
            LOG.info("split() begin, adviceNumber: [{}] ...", adviceNumber);
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

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
            // 更新需要切分数据尺寸
            splitNumber = targetListKeys.size();

            List<List<String>> splitedSourceFiles = this.splitSourceFiles(
                    targetListKeys, adviceNumber);

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
            Jedis client = null;
            try {
                client = getRedisClient();
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
                    } while (!(cursor = scanResult.getCursor()).equals(SCAN_POINTER_START));
                }
            } catch (RuntimeException e) {
                LOG.error("source topic to target topic failed. source topic: {}", sourceList, e);
            } finally {
                if (client != null) {
                    client.close();
                }
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

        public synchronized Jedis getRedisClient() {
            Jedis redisClient = null;
            try {
                redisClient = Job.JEDIS_POOL.getResource();
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

        public synchronized Jedis getRedisClient() {
            Jedis redisClient = null;
            try {
                redisClient = Job.JEDIS_POOL.getResource();
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

            Jedis client = getRedisClient();
            List<ColumnEntry> columnMetas = JsonStorageReaderUtil.getListColumnEntry(readerSliceConfig, Key.COLUMN);
            LOG.debug("task columns: {}", columnMetas);
            TaskPluginCollector collector = super.getTaskPluginCollector();

            String cursor;
            ScanParams sp;
            String keyType = null;
            ScanResult sr = null;
            List ls;
            boolean raiseException = false;
            List<JSONObject> resultList = null;
            for (String cacheKey : this.sourceKeys) {
                LOG.debug("reading redis cache key: [{}]", cacheKey);
                List<String> json = null;

                sp = new ScanParams();
                sp.count(readBatchSize);

                cursor = SCAN_POINTER_START;
                boolean singleValue = false;
                boolean nonExistKey = false;
                resultList = new ArrayList<>();
                try {
                    keyType = client.type(cacheKey);
                    do {
                        switch (keyType) {
                            case "hash":
                                sr = client.hscan(cacheKey, cursor, sp);
                                ls = sr.getResult();
                                for (Object o : ls) {
                                    Map.Entry<String, String> ot = (Map.Entry<String, String>)o;
                                    JSONObject jo = new JSONObject();
                                    // 定制化解析BizCartQuery情况
                                    if (StringUtils.contains(ot.getKey(), "com.chinagoods.oms.biz.query.BizCartQuery")) {
                                        JSONObject jn = JSON.parseObject(ot.getKey());
                                        LOG.info("Redis cart value: [{}]", jn.toString());
                                        if (cacheKey != null && cacheKey.split("_").length >= 3) {
                                            jo.put("userId", cacheKey.split("_")[2]);
                                        }
                                        jo.put("goodsId", jn.getString("goodsId"));
                                        jo.put("goodsSkuId", jn.getString("goodsSkuId"));
                                    } else {
                                        jo.put("hKey", ot.getKey());
                                        jo.put("hValue", ot.getValue());
                                        jo.put("key", cacheKey);
                                    }
                                    resultList.add(jo);
                                }
                                break;
                            case "zset":
                                sr = client.zscan(cacheKey, cursor, sp);
                                ls = sr.getResult();
                                for (Object o : ls) {
                                    Tuple ot = (Tuple)o;
                                    JSONObject jo = new JSONObject();
                                    jo.put("element", ot.getElement());
                                    jo.put("score", ot.getScore());
                                    jo.put("key", cacheKey);
                                    resultList.add(jo);
                                }
                                break;
                            case "string":
                                String valStr = client.get(cacheKey);
                                JSONObject jo = new JSONObject();
                                jo.put("key", cacheKey);
                                // 针对性处理反序列化问题
                                if (StringUtils.contains(valStr, "java.lang.Double")) {
                                    byte[] valBytes = client.get(cacheKey.getBytes());
                                    Double valStr2 = 0.0D;
                                    // 处理NPE
                                    if (valBytes != null) {
                                        ByteArrayInputStream bais = new ByteArrayInputStream(valBytes);;
                                        ObjectInputStream ois = new ObjectInputStream(bais);
                                        valStr2 = (Double) ois.readObject();
                                    }
                                    // double 保留8位精度, 避免精度不一致，数据校验出错
                                    jo.put("value", String.format("%.8f", valStr2));
                                } else {
                                    jo.put("value", valStr);
                                }
                                singleValue = true;

                                resultList.add(jo);
                                break;
                            case "none":
                                // key不存在，直接结束遍历
                                nonExistKey = true;
                                break;
                            case "list":
                            case "set":
                            default:
                                sr = client.sscan(cacheKey, cursor, sp);
                                ls = sr.getResult();
                                for (Object o : ls) {
                                    String ot = (String)o;
                                    JSONObject jor = new JSONObject();
                                    jor.put("value", ot);
                                    jor.put("key", cacheKey);
                                    resultList.add(jor);
                                }
                                break;
                        }
                        LOG.debug("task key: [{}], task key type: [{}], result list: [{}]", cacheKey, keyType, resultList);
                        JsonStorageReaderUtil.transportOneRecord(recordSender, collector,
                                columnMetas, resultList);

                        // 若是单元素类型，则直接结束此key查询
                        if (singleValue || nonExistKey) {
                            break;
                        }
                    } while (!(cursor = sr.getCursor()).equals(SCAN_POINTER_START));
                } catch (Exception e) {
                    raiseException = true;
                    String message = String
                            .format("error reading redis key: [%s], type: [%s]", cacheKey, keyType);
                    LOG.error(message, e);
                    throw DataXException.asDataXException(
                            CommonErrorCode.RUNTIME_ERROR, message);
                } finally {
                    if (raiseException) {
                        LOG.error("raise exception, close connection!!!");
                        if (client != null) {
                            client.close();
                        }
                    }
                }
            }

            // 若还处于激活状态，则归还连接
            if (client != null) {
                try {
                    client.close();
                } catch (RuntimeException e) {
                    LOG.error("释放jedis资源出错，将要关闭jedis，异常信息：", e);
                    if (client != null) {
                        try {
                            // 2. 客户端主动关闭连接
                            client.disconnect();
                        } catch (Exception e1) {
                            LOG.error("disconnect jedis connection fail: " , e);
                        }
                    }
                }

            }
            LOG.debug("end read redis ...");
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String str = "{\"@class\":\"com.chinagoods.oms.biz.query.BizCartQuery\",\"goodsId\":2874228,\"goodsSkuId\":1435192700960288769}";
        JSONObject jo = JSON.parseObject(str);
        System.out.println(jo.getString("goodsId"));
        System.out.println(jo.getString("goodsSkuId"));
        System.out.println(jo);
    }
}
