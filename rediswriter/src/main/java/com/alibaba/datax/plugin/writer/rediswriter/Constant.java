package com.alibaba.datax.plugin.writer.rediswriter;

/**
 * @description 常量
 * @author xiaowei.song
 * @date 2021-07-24 19:57
 * @version v1.0.0       
 **/
public final class Constant {

    public static final String STANDALONE = "singleton";
    public static final String CLUSTER = "cluster";

    /**
     * 支持的redis的四种数据类型
     **/
    public static final String WRITE_TYPE_STRING = "string";
    public static final String WRITE_TYPE_LIST= "list";
    public static final String WRITE_TYPE_HASH = "hash";
    public static final String WRITE_TYPE_SET = "set";

    /**
     * 两种redis的操作类型，delete和insert
     **/
    public static final String WRITE_MODE_DELETE = "delete";
    public static final String WRITE_MODE_INSERT = "insert";

    /**
     * 导入redis list的模式，有lpush，rpush，overwrite,默认overwrite
     **/
    public static final String LIST_PUSH_TYPE_OVERWRITE = "overwrite";
    public static final String LIST_PUSH_TYPE_LPUSH = "lpush";
    public static final String LIST_PUSH_TYPE_RPUSH = "rpush";

    public static final String HASH_KEY_DYNAMIC = "dynamic";
    public static final String HASH_KEY_STATIC = "static";

}
