package com.demo.flink.sink;

import com.demo.flink.common.LoggerUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.slf4j.Logger;

import java.util.Properties;

public class KafkaSinkBuilder {
    private static final Logger LOG = LoggerUtil.getLogger();
    public static KafkaSink<String> buildProducer(ParameterTool tool) throws Exception {
        //LOG.info("start connect kafka brokers:{}",tool.get("kafka.bootstrap.servers"));
        // set some kafka properties
        Properties properties = new Properties();
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "GSSAPI");
        properties.setProperty("sasl.kerberos.service.name", "kafka");
        properties.put("transaction.timeout.ms",15*60*1000);



        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(tool.get("kafka.bootstrap.servers")) //node14.test-tpl-hadoop-wh.com:9092
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(tool.get("kafka.topic"))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                // 设置 ts 的id前缀
                //.setTransactionalIdPrefix("ts")
                // 精确一次生产
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(properties)
                .build();




        LOG.info("======= kafka sink build success =======");
        return sink;
    }
}
