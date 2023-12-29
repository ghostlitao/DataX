package com.alibaba.datax.plugin.reader.mdyhttpreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.util.*;

/**
 * Created by ghostlitao on 2023-12-29.
 */
public class MdyHttpReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;


        private String host = null;
        private String appKey = null;
        private String sign = null;

        private String worksheetId = null;

        private int pageSize = 1000;

        @Override
        public void init() {
            // 加载配置
            this.originalConfig = super.getPluginJobConf();
            this.host = originalConfig.getString("host");
            this.appKey = originalConfig.getString("appKey");
            this.worksheetId = originalConfig.getString("worksheetId");
            this.sign = originalConfig.getString("sign");
            this.pageSize = originalConfig.getInt("pageSize", 1000);

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> confList = new ArrayList<>();
            // 获取表数据总量
            String countUrl = host + "/api/v2/open/worksheet/getFilterRowsTotalNum";
            String totalInfo = null;
            try {
                HashMap<String, Object> params = new HashMap<>();
                params.put("appKey", appKey);
                params.put("sign", sign);
                params.put("worksheetId", worksheetId);
                totalInfo = OkHttpUtils.post(countUrl, params);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            JSONObject totalObj = JSON.parseObject(totalInfo);
            int totalNum = totalObj.getIntValue("data");
            // 计算页数
            int pageNum = (int) Math.ceil(totalNum / pageSize);
            for (int i = 1; i <= pageNum; i++) {
                Configuration conf = originalConfig.clone();
                conf.set("pageIndex", i);
                conf.set("pageSize", pageSize);
                conf.set("host", host);
                conf.set("appKey", appKey);
                conf.set("sign", sign);
                confList.add(conf);
            }
            return confList;
        }


    }


    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;


        private String host = null;
        private String appKey = null;
        private String sign = null;
        private String worksheetId = null;
        private String viewId = null;

        private int pageSize = 1000;
        private int pageIndex = 1;

        private JSONArray columns = null;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.host = readerSliceConfig.getString("host");
            this.appKey = readerSliceConfig.getString("appKey");
            this.sign = readerSliceConfig.getString("sign");
            this.worksheetId = readerSliceConfig.getString("worksheetId");
            this.viewId = readerSliceConfig.getString("viewId");
            this.pageSize = readerSliceConfig.getInt("pageSize", 1000);
            this.pageIndex = readerSliceConfig.getInt("pageIndex", 1);
            this.columns = (JSONArray) readerSliceConfig.get("column");
        }

        @Override
        public void startRead(RecordSender recordSender) {

            if (host == null || appKey == null || sign == null) {
                throw DataXException.asDataXException("获取作业配置信息失败");
            }

            // 获取表数据
            String tableUrl = host + "/api/v2/open/worksheet/getFilterRows";
            String rowDataInfo = null;
            try {
                HashMap<String, Object> params = new HashMap<>();
                params.put("appKey", appKey);
                params.put("sign", sign);
                params.put("worksheetId", worksheetId);
                params.put("viewId", viewId);
                params.put("pageSize", pageSize);
                params.put("pageIndex", pageIndex);
                rowDataInfo = OkHttpUtils.post(tableUrl, params);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            JSONArray rowData = JSON.parseObject(rowDataInfo).getJSONObject("data").getJSONArray("rows");
            // 遍历 rowData，放入 record
            for (int i = 0; i < rowData.size(); i++) {
                Record record = recordSender.createRecord();
                JSONObject row = rowData.getJSONObject(i);
                // 获取配置文件中的表结构
                this.columns.stream().map(column -> (JSONObject) column).forEach(column -> {
                    String columnName = column.getString("name");
                    String columnType = column.getString("type");
                    // 根据类型转换
                    switch (columnType) {
                        case "string":
                            record.addColumn(new StringColumn(row.getString(columnName)));
                            break;
                        case "int":
                        case "number":
                        case "long":
                        case "double":
                        case "float":
                            record.addColumn(new DoubleColumn(row.getDouble(columnName)));
                        case "date":
                            record.addColumn(new DateColumn(row.getDate(columnName)));
                            break;
                        default:
                            record.addColumn(new StringColumn(row.getString(columnName)));
                            break;
                    }
                });
                recordSender.sendToWriter(record);
            }
        }


        @Override
        public void destroy() {

        }

    }
}
