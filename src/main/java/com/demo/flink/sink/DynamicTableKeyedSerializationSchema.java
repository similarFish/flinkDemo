package com.demo.flink.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class DynamicTableKeyedSerializationSchema implements KeyedSerializationSchema<Tuple2<String, String>> {
    @Override
    public byte[] serializeKey(Tuple2<String, String> element) {
        return element.f0.getBytes();
    }

    @Override
    public byte[] serializeValue(Tuple2<String, String> element) {
        return element.f1.getBytes();
    }

    @Override
    public String getTargetTopic(Tuple2<String, String> element) {
        return "dwd-" + element.f0;
    }
}
