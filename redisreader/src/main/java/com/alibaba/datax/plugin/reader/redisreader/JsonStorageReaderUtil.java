package com.alibaba.datax.plugin.reader.redisreader;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class JsonStorageReaderUtil {

	public static void transportOneRecord(RecordSender recordSender,
			TaskPluginCollector taskPluginCollector,
			List<ColumnEntry> column,
			String json) {
		
		//if(column==null)
		//column = JsonStorageReaderUtil.getListColumnEntry(configuration, Key.COLUMN);
		
		if(json!=null){
			if(json.startsWith("[")){
				JSONArray multi = JSON.parseArray(json);
				for(Object data : multi){
					JSONObject row = (JSONObject)data;
					if(!row.isEmpty())
						transportOneRecord(recordSender, column, row, taskPluginCollector);
				}
			}else{
				JSONObject row = JSON.parseObject(json);
				if(!row.isEmpty())
					transportOneRecord(recordSender, column, row, taskPluginCollector);
			}
		}
	}

	public static Record transportOneRecord(RecordSender recordSender, List<ColumnEntry> columnConfigs,
			JSONObject row, TaskPluginCollector taskPluginCollector) {
		Record record = recordSender.createRecord();
		Column columnGenerated = null;

		try {
			for (ColumnEntry columnConfig : columnConfigs) {
				String columnType = columnConfig.getType();
				//Integer columnIndex = columnConfig.getIndex();
				String columnName = columnConfig.getValue();

				String columnValue = row.getString(columnName);

				ColumnType type = ColumnType.valueOf(columnType.toUpperCase());

				switch (type) {
				case STRING:
					columnGenerated = new StringColumn(columnValue);
					break;
				case LONG:
					try {
						columnGenerated = new LongColumn(columnValue);
					} catch (Exception e) {
						throw new IllegalArgumentException(
								String.format("类型转换错误, 无法将[%s] 转换为[%s]", columnValue, "LONG"));
					}
					break;
				case DOUBLE:
					try {
						columnGenerated = new DoubleColumn(columnValue);
					} catch (Exception e) {
						throw new IllegalArgumentException(
								String.format("类型转换错误, 无法将[%s] 转换为[%s]", columnValue, "DOUBLE"));
					}
					break;
				case BOOLEAN:
					try {
						columnGenerated = new BoolColumn(columnValue);
					} catch (Exception e) {
						throw new IllegalArgumentException(
								String.format("类型转换错误, 无法将[%s] 转换为[%s]", columnValue, "BOOLEAN"));
					}

					break;
				case DATE:
					try {
						if (columnValue == null) {
							Date date = null;
							columnGenerated = new DateColumn(date);
						} else {
							String formatString = columnConfig.getFormat();
							if (formatString!=null && !formatString.isEmpty()) {
								// 用户自己配置的格式转换, 脏数据行为出现变化
								DateFormat format = columnConfig.getDateFormat();
								columnGenerated = new DateColumn(format.parse(columnValue));
							} else {
								// 框架尝试转换
								columnGenerated = new DateColumn(new StringColumn(columnValue).asDate());
							}
						}
					} catch (Exception e) {
						throw new IllegalArgumentException(
								String.format("类型转换错误, 无法将[%s] 转换为[%s]", columnValue, "DATE"));
					}
					break;
				default:
					String errorMessage = String.format("您配置的列类型暂不支持 : [%s]", columnType);
					throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
							errorMessage);
				}

				record.addColumn(columnGenerated);

			}
			recordSender.sendToWriter(record);
		} catch (IllegalArgumentException iae) {
			taskPluginCollector.collectDirtyRecord(record, iae.getMessage());
		} catch (IndexOutOfBoundsException ioe) {
			taskPluginCollector.collectDirtyRecord(record, ioe.getMessage());
		} catch (Exception e) {
			if (e instanceof DataXException) {
				throw (DataXException) e;
			}
			// 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
			taskPluginCollector.collectDirtyRecord(record, e.getMessage());
		}

		return record;
	}

	public static List<ColumnEntry> getListColumnEntry(Configuration configuration, final String path) {
		List<JSONObject> lists = configuration.getList(path, JSONObject.class);
		if (lists == null) {
			return null;
		}
		List<ColumnEntry> result = new ArrayList<ColumnEntry>();
		for (final JSONObject object : lists) {
			result.add(JSON.parseObject(object.toJSONString(),
			ColumnEntry.class));
		}
		return result;
	}

	private enum ColumnType {
		STRING, LONG, BOOLEAN, DOUBLE, DATE, ;
	}

	/**
	 * redis key enum
	 **/
	public static enum RedisKeyTypeEnum {
		none, string, list, set, hash, ;
	}
}
