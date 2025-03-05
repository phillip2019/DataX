package com.alibaba.datax.plugin.writer.rocketmqwriter;

public final class Key {

    /**
     * 此处声明插件用到的需要插件使用者提供的配置项
     */
    public static final String CONFIG_KEY_NAMESRV_ADDR = "namesrvAddr";
    public static final String CONFIG_KEY_TOPIC = "topic";
    public static final String CONFIG_KEY_TAG = "tag";
    public static final String CONFIG_KEY_PRODUCER_GROUP = "producerGroup";
    public static final String CONFIG_KEY_ACCESS_KEY = "accessKey";
    public static final String CONFIG_KEY_SECRET_KEY = "secretKey";
    public static final String CONFIG_KEY_MAX_RETRY_COUNT = "maxRetryCount";
    public static final String CONFIG_KEY_MAX_BATCH_SIZE = "maxBatchSize";
    public static final String CONFIG_KEY_MAX_BATCH_BYTE_SIZE = "maxBatchByteSize";
    public static final String CONFIG_KEY_SEND_TIMEOUT = "sendTimeoutMillis";
    public static final String CONFIG_KEY_WRITE_MODE = "mode";
    public static final String CONFIG_KEY_KEY_FIELD = "keyField";
    public static final String CONFIG_KEY_COLUMN = "column";
    public static final String CONFIG_KEY_MESSAGE_SERIALIZER = "messageSerializer";
    
    public static final String CONFIG_VALUE_MODE_NORMAL = "normal";
    public static final String CONFIG_VALUE_MODE_FIFO = "fifo";
    public static final String CONFIG_VALUE_MODE_DELAY = "delay";
    
    public static final String CONFIG_VALUE_SERIALIZER_JSON = "json";
    public static final String CONFIG_VALUE_SERIALIZER_STRING = "string";
    public static final String CONFIG_VALUE_SERIALIZER_BYTES = "bytes";
    
    public final static String RETRY_INTERVAL = "retryInterval";
}
