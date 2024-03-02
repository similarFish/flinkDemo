package com.demo.flink.sink;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.UUID;

public class HBaseSinkBuilder extends RichSinkFunction<Tuple2<String, String>> {
    Connection conn = null;
    ParameterTool tool;

    public HBaseSinkBuilder(ParameterTool tool) {
        this.tool = tool;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, tool.get("hbase.zookeeper.quorum", ""));
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, tool.get("hbase.zookeeper.property.clientPort", ""));
        config.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, tool.get("hbase.client.operation.timeout", "30000"));
        config.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, tool.get("hbase.client.scanner.timeout.period", "30000"));
        conn = ConnectionFactory.createConnection(config);
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        String tableName = "ods:" + value.f0;
        String columnFamily = value.f0;
        String rowKey = UUID.randomUUID().toString();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        JSONObject jsonObject = JSONObject.parseObject(value.f1);
        String tableNameCol = (String) jsonObject.getOrDefault("table_name", "");
        String key1 = (String) jsonObject.getOrDefault("key1", "");
        String key2 = (String) jsonObject.getOrDefault("key2", "");
        String allData = value.f1;
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("table_name"), Bytes.toBytes(tableNameCol));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("key1"), Bytes.toBytes(key1));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("key2"), Bytes.toBytes(key2));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("all_data"), Bytes.toBytes(allData));
        table.put(put);
    }
}
