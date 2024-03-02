package com.demo.flink.starter;

import com.alibaba.fastjson2.JSONObject;
import com.demo.flink.common.LoggerUtil;
import com.demo.flink.core.ExecutionEnvBuilder;
import com.demo.flink.sink.DynamicTableKeyedSerializationSchema;
import com.demo.flink.sink.HBaseSinkBuilder;
import com.demo.flink.source.KafkaSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;

import java.util.Properties;

public class flinkDemo {
    private static final Logger LOG = LoggerUtil.getLogger();

    public static void main(String[] args) {
        try {
            LOG.info("start application flinkCDC");
            //1.流处理执行环境

            ParameterTool tool = ExecutionEnvBuilder.createParameterTool(args);

            final StreamExecutionEnvironment env = ExecutionEnvBuilder.buildEnvironment(tool);

            //2.获取不同topic的数据
            KafkaSource<String> topic1 = KafkaSourceBuilder.buildConsumer(tool, env, "topic1");
            KafkaSource<String> topic2 = KafkaSourceBuilder.buildConsumer(tool, env, "topic2");

            DataStreamSource<String> source1 = env.fromSource(topic1, WatermarkStrategy.noWatermarks(), "topic1");
            DataStreamSource<String> source2 = env.fromSource(topic2, WatermarkStrategy.noWatermarks(), "topic2");

            //3.获取数据中不同的表
            SingleOutputStreamOperator<Tuple2<String, String>> map1 = source1.map(line -> {
                JSONObject jsonObject = JSONObject.parseObject(line);
                String tableName = (String) jsonObject.getOrDefault("table_name", "");
                return new Tuple2<String, String>(tableName, line);
            }).returns(Types.TUPLE(Types.STRING, Types.STRING));

            SingleOutputStreamOperator<Tuple2<String, String>> map2 = source2.map(line -> {
                JSONObject jsonObject = JSONObject.parseObject(line);
                String tableName = (String) jsonObject.getOrDefault("table_name", "");
                return new Tuple2<String, String>(tableName, line);
            }).returns(Types.TUPLE(Types.STRING, Types.STRING));

            //4.发送到对应表名的kafka dwd topic
            Properties props = new Properties();
            props.setProperty("bootstrap.servers", tool.get("kafka.bootstrap.servers.sink", ""));
            map1.addSink(new FlinkKafkaProducer<>("", new DynamicTableKeyedSerializationSchema(), props));
            map2.addSink(new FlinkKafkaProducer<>("", new DynamicTableKeyedSerializationSchema(), props));

            //5.发送到HBASE
            map1.addSink(new HBaseSinkBuilder(tool));
            map2.addSink(new HBaseSinkBuilder(tool));

            //6.执行
            env.execute("flinkDemo");

        } catch (Exception e) {
            LOG.error("job failed.", e);
            System.out.println("job failed." + e.getMessage());
            e.printStackTrace();
        }
    }
}
