package com.alibaba.datax.plugin.writer.rocketmqwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.fastjson.JSON;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 消息序列化器实现类
 */
public class MessageSerializerImpl implements MessageSerializer {
    
    // 实现接口的方法
    @Override
    public byte[] serialize(Record record, int[] columnMapping, String[] columnNames) {
        // 默认使用JSON序列化
        return new JsonSerializer().serialize(record, columnMapping, columnNames);
    }

    public static class JsonSerializer implements MessageSerializer {
        @Override
        public byte[] serialize(Record record, int[] columnMapping, String[] columnNames) {
            try {
                Map<String, Object> jsonMap = new HashMap<>();
                for (int i = 0; i < columnMapping.length; i++) {
                    Column column = record.getColumn(i);
//                    String columnName = "column_" + columnMapping[i];
                    String columnName = columnNames[i];
                    
                    if (column.getRawData() == null) {
                        jsonMap.put(columnName, null);
                        continue;
                    }
                    
                    switch (column.getType()) {
                        case DATE:
                            jsonMap.put(columnName, column.asDate());
                            break;
                        case BOOL:
                            jsonMap.put(columnName, column.asBoolean());
                            break;
                        case BYTES:
                            jsonMap.put(columnName, column.asBytes());
                            break;
                        case DOUBLE:
                            jsonMap.put(columnName, column.asDouble());
                            break;
                        case LONG:
                            jsonMap.put(columnName, column.asLong());
                            break;
                        case STRING:
                        default:
                            jsonMap.put(columnName, column.asString());
                            break;
                    }
                }
                return JSON.toJSONString(jsonMap).getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        RocketMQWriterErrorCode.CONVERT_DATA_FAILED,
                        "JSON serialization failed", e);
            }
        }
    }
    
    public static class StringSerializer implements MessageSerializer {
        @Override
        public byte[] serialize(Record record, int[] columnMapping, String[] columnNames) {
            try {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < columnMapping.length; i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    Column column = record.getColumn(i);
                    if (column.getRawData() != null) {
                        sb.append(column.asString());
                    }
                }
                return sb.toString().getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        RocketMQWriterErrorCode.CONVERT_DATA_FAILED,
                        "String serialization failed", e);
            }
        }
    }
    
    public static class BytesSerializer implements MessageSerializer {
        @Override
        public byte[] serialize(Record record, int[] columnMapping, String[] columnNames) {
            try {
                // 只取第一列作为字节数组内容
                if (columnMapping.length > 0) {
                    Column column = record.getColumn(0);
                    if (column.getType() == Column.Type.BYTES) {
                        return column.asBytes();
                    } else {
                        return column.asString().getBytes(StandardCharsets.UTF_8);
                    }
                }
                return new byte[0];
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        RocketMQWriterErrorCode.CONVERT_DATA_FAILED,
                        "Bytes serialization failed", e);
            }
        }
    }
    
    /**
     * 创建消息序列化器
     * @param serializerType 序列化类型
     * @return 序列化器实例
     */
    public static MessageSerializer createSerializer(String serializerType) {
        switch (serializerType) {
            case Key.CONFIG_VALUE_SERIALIZER_JSON:
                return new JsonSerializer();
            case Key.CONFIG_VALUE_SERIALIZER_STRING:
                return new StringSerializer();
            case Key.CONFIG_VALUE_SERIALIZER_BYTES:
                return new BytesSerializer();
            default:
                return new JsonSerializer();
        }
    }
}
