package com.skyon.connect.redis.deserializ;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;

import static java.sql.Types.VARCHAR;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/11/11
 */
public class RedisStringFormat implements DecodingFormat<DeserializationSchema<RowData>> {
    @Override
    @SuppressWarnings("unchecked")
    public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
        // create type information for the DeserializationSchema
        final TypeInformation<RowData> producedTypeInfo = (TypeInformation<RowData>) context.createTypeInformation(
                producedDataType);
        validateParsingTypes(producedDataType);
        // most of the code in DeserializationSchema will not work on internal data structures
        // create a converter for conversion at the end
        final DynamicTableSource.DataStructureConverter converter = context.createDataStructureConverter(producedDataType);

        // use logical types during runtime for parsing
        final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();

        // create runtime class
        return new RedisStringDeserializer(parsingTypes, converter,producedTypeInfo);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return null;
    }

    private void validateParsingTypes(DataType producedDataType){
        int size = producedDataType.getChildren().size();
        if (size != 2){
            throw new IllegalArgumentException("When value is in default format, only two fields can be defined in the table.");
        }
        for (DataType dataType : producedDataType.getChildren()) {
            if (!dataType.toString().startsWith("STRING"))
             throw new IllegalArgumentException("When value is in default format, When value is in default format, the field type can only be String.");
        }

    }

}
