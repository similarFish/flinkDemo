package com.demo.flink.source;

import com.demo.flink.common.LoggerUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;

import java.util.Properties;

/**
 * @Author zhanggaoyu
 * @Date 2023/1/29
 * @Description
 */

public class KafkaSourceBuilder {
    private static final Logger LOG = LoggerUtil.getLogger();
    public static KafkaSource<String> buildConsumer(ParameterTool tool, StreamExecutionEnvironment env, String topicConf) throws Exception {
        LOG.info("start connect kafka brokers:{}", tool.get("kafka.bootstrap.servers." + topicConf));
        String offset = tool.get("kafka.offset." + topicConf);
        OffsetsInitializer offsetsInitializer = OffsetsInitializer.latest();
        if (offset != null) {
            if (offset.equalsIgnoreCase("earliest")) {
                offsetsInitializer = OffsetsInitializer.earliest();
            }
        }


        org.apache.flink.connector.kafka.source.KafkaSourceBuilder<String> builder = KafkaSource.<String>builder()
                .setBootstrapServers(tool.get("kafka.bootstrap.servers." + topicConf))
                .setTopics(tool.get("kafka.topic." + topicConf))
                .setGroupId(tool.get("kafka.group.id.") + topicConf)
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(new SimpleStringSchema());

        // set some kafka properties
        Properties properties = new Properties();
        properties.put("max.partition.fetch.bytes", tool.getInt("kafka.max.partition.fetch.bytes." + topicConf, (Integer.MAX_VALUE - 1)));
        properties.put("max.poll.records", tool.getInt("kafka.max.poll.records." + topicConf, 2000));
        properties.put("fetch.max.bytes", tool.getInt("kafka.fetch.message.max.bytes." + topicConf, 447392426));

        boolean enableKafkaAuth = tool.getBoolean("kerberos.flink.kafka.enable." + topicConf, false);
        if (enableKafkaAuth) {
            properties.setProperty("security.protocol", tool.get("kerberos.flink.kafka.security.protocol." + topicConf));
            properties.setProperty("sasl.mechanism", tool.get("kerberos.flink.kafka.sasl.mechanism." + topicConf));
            properties.setProperty("sasl.kerberos.service.name", tool.get("kerberos.flink.kafka.service.name." + topicConf));
        }

        boolean enableResetTimestamp = tool.getBoolean("kafka.nginx.etl.offset.timestamp.enable." + topicConf, false);
        if (enableResetTimestamp) {
            LOG.warn("Kafka开启默认重新消费,设置setStartFromTimestamp()策略!!!");
            // consumer.setStartFromTimestamp 表示数据写入kafka时间
            long resetTimestamp = tool.getLong("kafka.nginx.etl.offset.timestamp", 0L);
            builder.setStartingOffsets(OffsetsInitializer.timestamp(resetTimestamp));
        }

        boolean hasReset = tool.getBoolean("kafka.nginx.etl.offset.reset." + topicConf, false);
        if (hasReset) {
            LOG.warn("Kafka开启默认重新消费,设置setStartFromEarliest()策略!!!");
            builder.setStartingOffsets(OffsetsInitializer.earliest());
        }

        KafkaSource<String> source = builder.setProperties(properties).build();
        LOG.info("======= kafka source build success =======");
        return source;
    }
}
