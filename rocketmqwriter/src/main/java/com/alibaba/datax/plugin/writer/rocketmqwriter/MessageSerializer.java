package com.alibaba.datax.plugin.writer.rocketmqwriter;

import com.alibaba.datax.common.element.Record;

/**
 * 消息序列化接口，定义如何将DataX的Record转换成RocketMQ的消息格式
 */
public interface MessageSerializer {
    /**
     * 将DataX记录序列化为字节数组
     * @param record DataX的Record对象
     * @param columnMapping 列映射信息
     * @param columnNames 列映射名称
     * @return 序列化后的字节数组
     */
    byte[] serialize(Record record, int[] columnMapping, String[] columnNames);
}
