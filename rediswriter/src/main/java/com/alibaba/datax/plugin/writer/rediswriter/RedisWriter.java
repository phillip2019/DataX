package com.alibaba.datax.plugin.writer.rediswriter;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswriter.writer.DeleteWriter;
import com.alibaba.datax.plugin.writer.rediswriter.writer.HashTypeWriter;
import com.alibaba.datax.plugin.writer.rediswriter.writer.ListTypeWriter;
import com.alibaba.datax.plugin.writer.rediswriter.writer.StringTypeWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RedisWriter extends Writer {
    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private static final Logger LOG = LoggerFactory.getLogger(RedisWriter.class);

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(originalConfig.clone());
                LOG.info("splited write part: {}", j);
            }
            LOG.info("end do split.");
            return splitResultConfigs;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            RedisWriterHelper.checkConnection(originalConfig);
        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration taskConfig;
        RedisWriteAbstract writer;

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            writer.addToPipLine(lineReceiver);
            writer.syncData();
        }



        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            String writeType = taskConfig.getString(Key.WRITE_TYPE);
            String writeMode = taskConfig.getString(Key.WRITE_MODE);

            LOG.info("当前写入模式为： {}", Key.WRITE_MODE);
            // 判断是delete还是insert
            if (Constant.WRITE_MODE_DELETE.equalsIgnoreCase(writeMode)) {
                writer = new DeleteWriter(taskConfig);
            } else {
                // 判断写redis的数据类型，string，list，hash
                switch (writeType) {
                    case Constant.WRITE_TYPE_HASH:
                        writer = new HashTypeWriter(taskConfig);
                        break;
                    case Constant.WRITE_TYPE_LIST:
                        writer = new ListTypeWriter(taskConfig);
                        break;
                    case Constant.WRITE_TYPE_STRING:
                        writer = new StringTypeWriter(taskConfig);
                        break;
                    default:
                        throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "redisWriter 不支持此数据类型:" + writeType);
                }

            }
            writer.checkAndGetParams();
            writer.initCommonParams();
            writer.deleteOldData();
        }

        @Override
        public void destroy() {
            writer.syncAllData();
            writer.close();
        }
    }
}
