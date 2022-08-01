package com.skyon.connect.redis.deserializ;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;


/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/11/11
 */
public class RedisStringDeserializer implements DeserializationSchema<RowData> {

    private final List<LogicalType> parsingTypes;
    private final DynamicTableSource.DataStructureConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;

    public RedisStringDeserializer(
            List<LogicalType> parsingTypes,
            DynamicTableSource.DataStructureConverter converter,
            TypeInformation<RowData> producedTypeInfo) {
        this.parsingTypes = parsingTypes;
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        String value = message == null ? null : new String(message);
        final Row row = new Row(parsingTypes.size());
        row.setField(0, null);
        row.setField(1, value);
        return (RowData)converter.toInternal(row);
    }

    @Override
    public RowData deserialize(byte[] message, String fieldname, String fieldvalue) throws IOException {
        String value = message == null ? null : new String(message);
        final Row row = new Row(parsingTypes.size());
        row.setField(0, fieldvalue);
        row.setField(1, value);
        return (RowData)converter.toInternal(row);
    }

    @Override
    public boolean isEndOfStream(RowData var1) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        converter.open(RuntimeConverter.Context.create(RedisStringDeserializer.class.getClassLoader()));
    }
}
