package com.alibaba.datax.plugin.reader.redisreader;

/**
 * Created by crabo yang
 */
public class Key {
	public static final String HOST = "host";
	public static final String PORT = "port";
	public static final String AUTH_PWD = "auth";
    public static final String DB = "db";
	public static final String TEST_ON_BORROW = "testOnBorrow";
	
    public static final String CONN_TIMEOUT = "connectionTimeout";
    public static final String ListKey = "listKey"; //read list by keys[]
    public static final String COLUMN = "column";
    
    public static final String READ_BATCH_SIZE = "readBatchSize";

    public static final String MAX_IDLE = "maxIdle";
    public static final String MAX_TOTAL = "maxTotal";
    public static final String MIN_IDLE = "minIdle";


    public static final String BLOCK_WHEN_EXHAUSTED = "blockWhenExhausted";
    public static final String JMX_ENABLED = "jmxEnabled";

}
