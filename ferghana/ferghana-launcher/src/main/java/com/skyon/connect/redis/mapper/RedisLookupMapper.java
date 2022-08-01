package com.skyon.connect.redis.mapper;


import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;

import com.google.common.base.Joiner;


public class RedisLookupMapper extends AbstractDeserializationSchema<RowData> implements SerializationSchema<Object[]> {


    private DeserializationSchema<RowData> valueDeserializationSchema;

    public RedisLookupMapper(DeserializationSchema<RowData> valueDeserializationSchema) {
        this.valueDeserializationSchema = valueDeserializationSchema;

    }

    public RedisCommandDescription getCommandDescription(String additionalKey) {
        return additionalKey == null ? new RedisCommandDescription(RedisLookupCommand.GET) : new RedisCommandDescription(RedisLookupCommand.HGET, additionalKey);
    }

    public RowData deserialize(byte[] message, String fieldname, String fieldvalue) {
        try {
            return this.valueDeserializationSchema.deserialize(message, fieldname, fieldvalue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public byte[] serialize(Object[] element) {
        return Joiner.on(":").join(element).getBytes();
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try {
            return  this.valueDeserializationSchema.deserialize(message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
