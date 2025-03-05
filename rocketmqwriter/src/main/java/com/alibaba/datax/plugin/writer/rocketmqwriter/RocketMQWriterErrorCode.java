package com.alibaba.datax.plugin.writer.rocketmqwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum RocketMQWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("RocketMQWriter-00", "缺失必需的值"),
    CONNECTION_ERROR("RocketMQWriter-01", "连接RocketMQ服务失败"),
    SEND_DATA_FAIL("RocketMQWriter-02", "向RocketMQ发送数据失败"),
    ILLEGAL_VALUE("RocketMQWriter-03", "值非法"),
    INIT_PRODUCER_ERROR("RocketMQWriter-04", "初始化RocketMQ生产者失败"),
    SHUTDOWN_PRODUCER_ERROR("RocketMQWriter-05", "关闭RocketMQ生产者失败"),
    CONVERT_DATA_FAILED("RocketMQWriter-06", "数据转换失败"),
    CONFIG_INVALID("RocketMQWriter-07", "配置项非法");

    private final String code;
    private final String description;

    RocketMQWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]", this.code,
                this.description);
    }
}
