package com.alibaba.datax.plugin.writer.rocketmqwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocketMQ客户端辅助类，负责创建和配置RocketMQ生产者
 */
public class RocketMQClientHelper {
    private static final Logger LOG = LoggerFactory.getLogger(RocketMQClientHelper.class);
    
    /**
     * 创建RocketMQ生产者
     * @param config 配置信息
     * @return RocketMQ生产者实例
     */
    public static MQProducer createProducer(Configuration config) {
        String namesrvAddr = config.getNecessaryValue(Key.CONFIG_KEY_NAMESRV_ADDR, 
                RocketMQWriterErrorCode.REQUIRED_VALUE);
        String producerGroup = config.getString(Key.CONFIG_KEY_PRODUCER_GROUP, "datax_rocketmq_producer");
        String accessKey = config.getString(Key.CONFIG_KEY_ACCESS_KEY, null);
        String secretKey = config.getString(Key.CONFIG_KEY_SECRET_KEY, null);
        int sendTimeout = config.getInt(Key.CONFIG_KEY_SEND_TIMEOUT, 3000);
        
        DefaultMQProducer producer;
        
        // 根据是否配置ACL凭证选择不同的初始化方式
        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
            try {
                // 动态加载ACL相关类并实例化
                Class<?> aclClientRPCHookClass = Class.forName("org.apache.rocketmq.acl.common.AclClientRPCHook");
                Class<?> sessionCredentialsClass = Class.forName("org.apache.rocketmq.acl.common.SessionCredentials");
                
                // 创建SessionCredentials实例
                Object credentials = sessionCredentialsClass.getDeclaredConstructor(String.class, String.class)
                        .newInstance(accessKey, secretKey);
                
                // 创建AclClientRPCHook实例
                Object aclClientRPCHook = aclClientRPCHookClass.getDeclaredConstructor(sessionCredentialsClass)
                        .newInstance(credentials);
                
                // 使用带有ACL的构造函数创建producer
                producer = DefaultMQProducer.class.getDeclaredConstructor(String.class, aclClientRPCHookClass.getInterfaces()[0])
                        .newInstance(producerGroup, aclClientRPCHook);
                
                LOG.info("Created RocketMQ producer with ACL enabled");
            } catch (Exception e) {
                LOG.warn("Failed to create RocketMQ producer with ACL. Falling back to non-ACL mode: " + e.getMessage());
                producer = new DefaultMQProducer(producerGroup);
            }
        } else {
            // 无ACL认证的常规初始化方式
            producer = new DefaultMQProducer(producerGroup);
        }
        
        producer.setNamesrvAddr(namesrvAddr);
        producer.setSendMsgTimeout(sendTimeout);
        producer.setVipChannelEnabled(false);
        
        try {
            LOG.info("Starting RocketMQ producer with namesrvAddr: {}, producerGroup: {}", 
                    namesrvAddr, producerGroup);
            producer.start();
            return producer;
        } catch (MQClientException e) {
            throw DataXException.asDataXException(
                    RocketMQWriterErrorCode.INIT_PRODUCER_ERROR,
                    "Failed to initialize RocketMQ producer: " + e.getMessage(), e);
        }
    }
    
    /**
     * 关闭RocketMQ生产者
     * @param producer RocketMQ生产者实例
     */
    public static void shutdownProducer(MQProducer producer) {
        if (producer != null) {
            try {
                producer.shutdown();
            } catch (Exception e) {
                LOG.warn("Error shutting down RocketMQ producer: " + e.getMessage(), e);
                // 不抛出异常，防止影响主流程
            }
        }
    }
}
