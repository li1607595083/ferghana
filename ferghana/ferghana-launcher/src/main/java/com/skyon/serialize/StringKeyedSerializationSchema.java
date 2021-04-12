package com.skyon.serialize;

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class StringKeyedSerializationSchema implements KeyedSerializationSchema<String>{

    @Override
    public byte[] serializeKey(String element) {
        return null;
    }

    @Override
    public byte[] serializeValue(String element) {
        return element.getBytes();
    }

    @Override
    public String getTargetTopic(String element) {
        return null;
    }
}
