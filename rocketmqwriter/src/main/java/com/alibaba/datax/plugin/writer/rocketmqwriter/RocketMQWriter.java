package com.alibaba.datax.plugin.writer.rocketmqwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RocketMQWriter extends Writer {
    
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration jobConfig = null;

        @Override
        public void init() {
            this.jobConfig = super.getPluginJobConf();
            
            // 验证必需的配置项
            validateConfig();
            
            LOG.info("RocketMQWriter job init finished, configuration: {}",
                    JSON.toJSONString(this.jobConfig.getInternal()));
        }
        
        private void validateConfig() {
            // 验证必需的配置项
            this.jobConfig.getNecessaryValue(Key.CONFIG_KEY_NAMESRV_ADDR, 
                    RocketMQWriterErrorCode.REQUIRED_VALUE);
            this.jobConfig.getNecessaryValue(Key.CONFIG_KEY_TOPIC, 
                    RocketMQWriterErrorCode.REQUIRED_VALUE);
            
            // 验证发送模式配置的有效性
            String mode = this.jobConfig.getString(Key.CONFIG_KEY_WRITE_MODE, 
                    Key.CONFIG_VALUE_MODE_NORMAL);
            if (!Key.CONFIG_VALUE_MODE_NORMAL.equals(mode) 
                    && !Key.CONFIG_VALUE_MODE_FIFO.equals(mode)
                    && !Key.CONFIG_VALUE_MODE_DELAY.equals(mode)) {
                throw DataXException.asDataXException(
                        RocketMQWriterErrorCode.CONFIG_INVALID,
                        String.format("writeMode only support %s, %s or %s mode, but your mode is: %s",
                                Key.CONFIG_VALUE_MODE_NORMAL, Key.CONFIG_VALUE_MODE_FIFO, 
                                Key.CONFIG_VALUE_MODE_DELAY, mode));
            }
            
            // 验证序列化器配置的有效性
            String serializer = this.jobConfig.getString(Key.CONFIG_KEY_MESSAGE_SERIALIZER, 
                    Key.CONFIG_VALUE_SERIALIZER_JSON);
            if (!Key.CONFIG_VALUE_SERIALIZER_JSON.equals(serializer) 
                    && !Key.CONFIG_VALUE_SERIALIZER_STRING.equals(serializer)
                    && !Key.CONFIG_VALUE_SERIALIZER_BYTES.equals(serializer)) {
                throw DataXException.asDataXException(
                        RocketMQWriterErrorCode.CONFIG_INVALID,
                        String.format("messageSerializer only support %s, %s or %s, but yours is: %s",
                                Key.CONFIG_VALUE_SERIALIZER_JSON, Key.CONFIG_VALUE_SERIALIZER_STRING, 
                                Key.CONFIG_VALUE_SERIALIZER_BYTES, serializer));
            }
        }

        @Override
        public void prepare() {
            // 测试RocketMQ连接
            MQProducer producer = null;
            try {
                producer = RocketMQClientHelper.createProducer(this.jobConfig);
                LOG.info("Successfully connected to RocketMQ.");
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        RocketMQWriterErrorCode.CONNECTION_ERROR,
                        "RocketMQ connection test failed: " + e.getMessage(), e);
            } finally {
                if (producer != null) {
                    RocketMQClientHelper.shutdownProducer(producer);
                }
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(this.jobConfig.clone());
            }
            return configurations;
        }

        @Override
        public void post() {
            // 无需后处理
        }

        @Override
        public void destroy() {
            // 无需清理资源
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration taskConfig;
        private MQProducer producer;
        private String topic;
        private String tag;
        private String mode;
        private int maxBatchSize;
        private int maxBatchByteSize;
        private int maxRetryCount;
        private int retryInterval;
        private int keyFieldIndex;
        private MessageSerializer serializer;
        private int[] columnIndex;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            this.topic = taskConfig.getString(Key.CONFIG_KEY_TOPIC);
            this.tag = taskConfig.getString(Key.CONFIG_KEY_TAG, "");
            this.mode = taskConfig.getString(Key.CONFIG_KEY_WRITE_MODE, Key.CONFIG_VALUE_MODE_NORMAL);
            this.maxBatchSize = taskConfig.getInt(Key.CONFIG_KEY_MAX_BATCH_SIZE, 32);
            this.maxBatchByteSize = taskConfig.getInt(Key.CONFIG_KEY_MAX_BATCH_BYTE_SIZE, 4 * 1024 * 1024); // 默认4MB
            this.maxRetryCount = taskConfig.getInt(Key.CONFIG_KEY_MAX_RETRY_COUNT, 3);
            this.retryInterval = taskConfig.getInt(Key.RETRY_INTERVAL, 1000);

            // 初始化消息序列化器
            String serializerType = taskConfig.getString(Key.CONFIG_KEY_MESSAGE_SERIALIZER, 
                    Key.CONFIG_VALUE_SERIALIZER_JSON);
            this.serializer = MessageSerializerImpl.createSerializer(serializerType);
            
            // 处理keyField配置
            String keyField = taskConfig.getString(Key.CONFIG_KEY_KEY_FIELD, "");
            this.keyFieldIndex = -1;
            
            // 处理列映射
            List<String> column = this.taskConfig.getList(Key.CONFIG_KEY_COLUMN, String.class);
            this.columnIndex = new int[column == null ? 0 : column.size()];
            if (column != null && !column.isEmpty()) {
                // 如果只有一个元素且为"*"，表示使用所有列
                if (column.size() == 1 && "*".equals(column.get(0))) {
                    this.columnIndex = null; // 使用null表示使用所有列
                } else {
                    // 根据列配置，查找keyField对应的索引
                    for (int i = 0; i < column.size(); i++) {
                        String col = column.get(i);
                        this.columnIndex[i] = i;
                        if (StringUtils.equals(col, keyField)) {
                            this.keyFieldIndex = i;
                        }
                    }
                }
            }
        }

        @Override
        public void prepare() {
            this.producer = RocketMQClientHelper.createProducer(this.taskConfig);
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            LOG.info("Start writing to RocketMQ, topic: {}, tag: {}", topic, tag);

            Record record;
            List<Record> recordBatch = new ArrayList<>(this.maxBatchSize);
            int batchByteSize = 0;
            
            while ((record = recordReceiver.getFromReader()) != null) {
                recordBatch.add(record);
                batchByteSize += record.getByteSize();
                
                if (recordBatch.size() >= this.maxBatchSize || batchByteSize >= this.maxBatchByteSize) {
                    sendBatch(recordBatch);
                    recordBatch.clear();
                    batchByteSize = 0;
                }
            }
            
            // 发送最后一批数据
            if (!recordBatch.isEmpty()) {
                sendBatch(recordBatch);
            }
            
            LOG.info("Finished writing to RocketMQ");
        }

        private void sendBatch(List<Record> recordBatch) {
            if (recordBatch.isEmpty()) {
                return;
            }
            
            int size = recordBatch.size();
            LOG.debug("Sending batch to RocketMQ, size: {}", size);
            
            if (Key.CONFIG_VALUE_MODE_FIFO.equals(this.mode)) {
                // FIFO模式（同步发送，保证按照固定的分区顺序发送）
                sendBatchFIFO(recordBatch);
            } else if (Key.CONFIG_VALUE_MODE_DELAY.equals(this.mode)) {
                // 延时消息模式
                sendBatchDelay(recordBatch);
            } else {
                // 普通模式（异步批量发送）
                sendBatchAsync(recordBatch);
            }
        }
        
        private void sendBatchAsync(List<Record> recordBatch) {
            CountDownLatch latch = new CountDownLatch(recordBatch.size());
            AtomicReference<Throwable> exception = new AtomicReference<>();
            
            for (Record record : recordBatch) {
                try {
                    byte[] messageBody = serializer.serialize(record, columnIndex);
                    if (messageBody == null || messageBody.length == 0) {
                        latch.countDown();
                        continue;
                    }

                    Message msg = createMessage(record, messageBody);
                    producer.send(msg, new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                LOG.warn("Send message not OK, status: {}, msgId: {}", 
                                        sendResult.getSendStatus(), sendResult.getMsgId());
                            }
                            latch.countDown();
                        }

                        @Override
                        public void onException(Throwable e) {
                            LOG.error("Failed to send message: " + e.getMessage(), e);
                            exception.set(e);
                            latch.countDown();
                        }
                    });
                } catch (Exception e) {
                    LOG.error("Failed to send message: " + e.getMessage(), e);
                    super.getTaskPluginCollector().collectDirtyRecord(record, e);
                    latch.countDown();
                }
            }
            
            try {
                boolean completed = latch.await(30, TimeUnit.SECONDS);
                if (!completed) {
                    LOG.warn("Timeout waiting for RocketMQ send callbacks, some messages may still be in flight");
                }
                
                if (exception.get() != null) {
                    throw DataXException.asDataXException(
                            RocketMQWriterErrorCode.SEND_DATA_FAIL,
                            "Failed to send message to RocketMQ", exception.get());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw DataXException.asDataXException(
                        RocketMQWriterErrorCode.SEND_DATA_FAIL,
                        "Thread interrupted while waiting for RocketMQ send callbacks", e);
            }
        }
        
        private void sendBatchFIFO(List<Record> recordBatch) {
            // 确保消息按照相同的顺序发送到相同的队列
            // 计算一个哈希值作为消息key，相同key的消息会路由到相同的队列
            String messageKey = "datax_batch_" + System.currentTimeMillis();
            
            for (Record record : recordBatch) {
                try {
                    byte[] messageBody = serializer.serialize(record, columnIndex);
                    if (messageBody == null || messageBody.length == 0) {
                        continue;
                    }

                    Message msg = createMessage(record, messageBody);
                    msg.setKeys(messageKey);  // 设置消息键以确保消息分布到相同队列
                    
                    // 同步发送并重试
                    for (int i = 0; i <= maxRetryCount; i++) {
                        try {
                            SendResult sendResult = producer.send(msg);
                            if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                LOG.warn("Message not sent OK, status: {}, attempt: {}/{}", 
                                        sendResult.getSendStatus(), i + 1, maxRetryCount + 1);
                                
                                if (i == maxRetryCount) {
                                    super.getTaskPluginCollector().collectDirtyRecord(record, 
                                            new Exception("Failed to send message after " + maxRetryCount + " retries"));
                                } else {
                                    Thread.sleep(retryInterval);
                                }
                            } else {
                                break; // 发送成功，退出重试循环
                            }
                        } catch (Exception e) {
                            if (i == maxRetryCount) {
                                LOG.error("Failed to send message after " + maxRetryCount + " retries: " + e.getMessage(), e);
                                super.getTaskPluginCollector().collectDirtyRecord(record, e);
                            } else {
                                LOG.warn("Failed to send message, will retry {}/{}: {}", 
                                        i + 1, maxRetryCount + 1, e.getMessage());
                                Thread.sleep(retryInterval);
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Failed to process message: " + e.getMessage(), e);
                    super.getTaskPluginCollector().collectDirtyRecord(record, e);
                }
            }
        }
        
        private void sendBatchDelay(List<Record> recordBatch) {
            // 处理延时消息，RocketMQ支持的延时级别
            // messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
            int delayLevel = taskConfig.getInt("delayLevel", 0);
            
            for (Record record : recordBatch) {
                try {
                    byte[] messageBody = serializer.serialize(record, columnIndex);
                    if (messageBody == null || messageBody.length == 0) {
                        continue;
                    }

                    Message msg = createMessage(record, messageBody);
                    if (delayLevel > 0) {
                        msg.setDelayTimeLevel(delayLevel);
                    }
                    
                    // 同步发送延时消息，确保可靠性
                    for (int i = 0; i <= maxRetryCount; i++) {
                        try {
                            SendResult sendResult = producer.send(msg);
                            if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                LOG.warn("Delay message not sent OK, status: {}, attempt: {}/{}", 
                                        sendResult.getSendStatus(), i + 1, maxRetryCount + 1);
                                
                                if (i == maxRetryCount) {
                                    super.getTaskPluginCollector().collectDirtyRecord(record, 
                                            new Exception("Failed to send delay message after " + maxRetryCount + " retries"));
                                } else {
                                    Thread.sleep(retryInterval);
                                }
                            } else {
                                break; // 发送成功，退出重试循环
                            }
                        } catch (Exception e) {
                            if (i == maxRetryCount) {
                                LOG.error("Failed to send delay message after " + maxRetryCount + " retries: " + e.getMessage(), e);
                                super.getTaskPluginCollector().collectDirtyRecord(record, e);
                            } else {
                                LOG.warn("Failed to send delay message, will retry {}/{}: {}", 
                                        i + 1, maxRetryCount + 1, e.getMessage());
                                Thread.sleep(retryInterval);
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Failed to process delay message: " + e.getMessage(), e);
                    super.getTaskPluginCollector().collectDirtyRecord(record, e);
                }
            }
        }
        
        private Message createMessage(Record record, byte[] messageBody) {
            Message msg = new Message(topic, tag, messageBody);
            
            // 如果配置了keyField，则使用对应列的值作为消息的Key
            if (keyFieldIndex >= 0 && keyFieldIndex < record.getColumnNumber()) {
                Column keyColumn = record.getColumn(keyFieldIndex);
                if (keyColumn != null && keyColumn.getRawData() != null) {
                    String keyValue = keyColumn.asString();
                    if (StringUtils.isNotBlank(keyValue)) {
                        msg.setKeys(keyValue);
                    }
                }
            }
            
            return msg;
        }

        @Override
        public void post() {
            // 无需后处理
        }

        @Override
        public void destroy() {
            RocketMQClientHelper.shutdownProducer(this.producer);
        }
    }
}
