package com.alibaba.datax.plugin.reader.mongodbregtblreader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.HostUtils;
import com.alibaba.datax.plugin.reader.mongodbregtblreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbregtblreader.util.MongoUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.alibaba.fastjson.parser.Feature;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class MongoDBReader extends Reader {

    private static final Logger log = LoggerFactory.getLogger(MongoDBReader.class);

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            String dbName = originalConfig.getString(KeyConstant.MONGO_DB_NAME, originalConfig.getString(KeyConstant.MONGO_DATABASE));
            String collName = originalConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            if(Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(collName) || mongoClient == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                        MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }

            List<Configuration> configurationList = new ArrayList<>();
            if (collName.endsWith("*")) {
                MongoDatabase database = mongoClient.getDatabase(dbName);
                String tempColl = collName.substring(0, collName.length() - 1);
                log.info("starts to get collection names...");
                // 获取所有表名
                for (String coll : database.listCollectionNames()) {
                    log.debug("mongoregtbldb get collection name: {}", coll);
                    if (coll.startsWith(tempColl)) {
                        log.debug("mongoregtbldb dbName: {}, collection name: {}", dbName, coll);
                        originalConfig.set(KeyConstant.MONGO_COLLECTION_NAME, coll);
                        List<Configuration> colCfgList = CollectionSplitUtil.doSplit(originalConfig, adviceNumber, mongoClient);
                        log.debug("mongoregtbldb coll: {} cfg size: {}", coll, colCfgList.size());
                        configurationList.addAll(colCfgList);
                    }
                }
            } else {
                configurationList = CollectionSplitUtil.doSplit(originalConfig, adviceNumber, mongoClient);
            }
            return configurationList;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME, originalConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD, originalConfig.getString(KeyConstant.MONGO_PASSWORD));
            String database =  originalConfig.getString(KeyConstant.MONGO_DB_NAME, originalConfig.getString(KeyConstant.MONGO_DATABASE));
            String authDb =  originalConfig.getString(KeyConstant.MONGO_AUTHDB, database);
            if(!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig,userName,password,authDb);
            } else {
                this.mongoClient = MongoUtil.initMongoClient(originalConfig);
            }
        }

        @Override
        public void destroy() {
            this.mongoClient.close();
        }
    }


    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        private String authDb = null;
        private String database = null;
        private String collection = null;

        private String query = null;

        private JSONArray mongodbColumnMeta = null;
        private Object lowerBound = null;
        private Object upperBound = null;
        private boolean isObjectId = true;

        @Override
        public void startRead(RecordSender recordSender) {

            if(lowerBound== null || upperBound == null ||
                mongoClient == null || database == null ||
                collection == null  || mongodbColumnMeta == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection col = db.getCollection(this.collection);

            MongoCursor<Document> dbCursor = null;
            Document filter = new Document();
            if (lowerBound.equals("min")) {
                if (!upperBound.equals("max")) {
                    filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
                }
            } else if (upperBound.equals("max")) {
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound));
            } else {
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound).append("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
            }
            if(!Strings.isNullOrEmpty(query)) {
                Document queryFilter = Document.parse(query);
                filter = new Document("$and", Arrays.asList(filter, queryFilter));
            }
            dbCursor = col.find(filter).iterator();
            while (dbCursor.hasNext()) {
                Document item = dbCursor.next();
                Record record = recordSender.createRecord();
                Iterator columnItera = mongodbColumnMeta.iterator();
                while (columnItera.hasNext()) {
                    JSONObject column = (JSONObject)columnItera.next();
                    Object tempCol = item.get(column.getString(KeyConstant.COLUMN_NAME));
                    if (tempCol == null) {
                        // 文档类型
                        if (KeyConstant.isDocumentType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            //列名，针对嵌套类型，parentKey.childKey
                            String[] name = column.getString(KeyConstant.COLUMN_NAME).split("\\.");
                            if (name.length > 1) {
                                Object obj;
                                Document nestedDocument = item;
                                for (String str : name) {
                                    obj = nestedDocument.get(str);
                                    if (obj instanceof Document) {
                                        nestedDocument = (Document) obj;
                                    }
                                }

                                if (null != nestedDocument) {
                                    Document doc = nestedDocument;
                                    tempCol = doc.get(name[name.length - 1]);
                                }
                            }
                        }
                    }
                    if (tempCol == null) {
                        //continue; 这个不能直接continue会导致record到目的端错位
                        record.addColumn(new StringColumn(null));
                    }else if (tempCol instanceof Double) {
                        //TODO deal with Double.isNaN()
                        record.addColumn(new DoubleColumn((Double) tempCol));
                    } else if (tempCol instanceof Boolean) {
                        record.addColumn(new BoolColumn((Boolean) tempCol));
                    } else if (tempCol instanceof Date) {
                        record.addColumn(new DateColumn((Date) tempCol));
                    } else if (tempCol instanceof Integer) {
                        record.addColumn(new LongColumn((Integer) tempCol));
                    }else if (tempCol instanceof Long) {
                        record.addColumn(new LongColumn((Long) tempCol));
                    } else {
                        if(KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            String splitter = column.getString(KeyConstant.COLUMN_SPLITTER);
                            if(Strings.isNullOrEmpty(splitter)) {
                                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
                            } else {
                                ArrayList array = (ArrayList)tempCol;
                                String tempArrayStr = Joiner.on(splitter).join(array);
                                record.addColumn(new StringColumn(tempArrayStr));
                            }
                        } else if (tempCol instanceof List || tempCol instanceof Map) {
                            record.addColumn(new StringColumn(JSON.toJSONString(tempCol)));
                        } else {
                            record.addColumn(new StringColumn(tempCol.toString()));
                        }
                    }
                }
                recordSender.sendToWriter(record);
            }
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME, readerSliceConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD, readerSliceConfig.getString(KeyConstant.MONGO_PASSWORD));
            this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME, readerSliceConfig.getString(KeyConstant.MONGO_DATABASE));
            this.authDb = readerSliceConfig.getString(KeyConstant.MONGO_AUTHDB, this.database);
            if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig,userName,password,authDb);
            } else {
                mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
            }

            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            // 修复读取配置无序，导致后续写入乱序问题
            this.mongodbColumnMeta = JSONArray.parseObject(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN).getBytes(), JSONArray.class, Feature.OrderedField);
            this.lowerBound = readerSliceConfig.get(KeyConstant.LOWER_BOUND);
            this.upperBound = readerSliceConfig.get(KeyConstant.UPPER_BOUND);
            this.isObjectId = readerSliceConfig.getBool(KeyConstant.IS_OBJECTID);
        }

        @Override
        public void destroy() {

        }

    }
}
