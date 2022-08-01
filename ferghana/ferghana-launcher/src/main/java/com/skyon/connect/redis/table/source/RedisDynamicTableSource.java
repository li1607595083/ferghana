package com.skyon.connect.redis.table.source;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

import javax.annotation.Nullable;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import com.skyon.connect.redis.mapper.RedisLookupMapper;
import com.skyon.connect.redis.options.RedisLookupOptions;

import java.util.*;
import java.util.stream.IntStream;


public class RedisDynamicTableSource implements LookupTableSource {

    /*Data type to configure the formats.*/
    private final DataType physicalDataType;
    /*Optional format for decoding keys from redis.*/
    private final @Nullable DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final FlinkJedisConfigBase flinkJedisConfigBase;
    private final RedisLookupOptions redisLookupOptions;
    private final String primarykey;
    private final String hashName;

    public RedisDynamicTableSource(
            DataType physicalDataType
            , DecodingFormat<DeserializationSchema<RowData>> decodingFormat
            , FlinkJedisConfigBase flinkJedisConfigBase
            , RedisLookupOptions redisLookupOptions
            , String primarykey
            , String hashName
            ) {

        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.decodingFormat = decodingFormat;
        this.flinkJedisConfigBase = flinkJedisConfigBase;
        this.redisLookupOptions = redisLookupOptions;
        this.primarykey = primarykey;
        this.hashName = hashName;
    }


    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        RedisLookupMapper redisLookupMapper = new RedisLookupMapper(
                this.createDeserialization(context, this.decodingFormat, createValueFormatProjection(this.physicalDataType)));

        return TableFunctionProvider.of(new RedisRowDataLookupFunction(
                flinkJedisConfigBase
                ,redisLookupMapper
                ,this.redisLookupOptions
                ,hashName
                ,primarykey
                ));
    }

    private @Nullable DeserializationSchema<RowData> createDeserialization(
            Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType = projectRow(this.physicalDataType, projection);
        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }


    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(this.physicalDataType, this.decodingFormat, this.flinkJedisConfigBase, this.redisLookupOptions, this.primarykey, this.hashName);
    }

    @Override
    public String asSummaryString() {
        return "redis look up" ;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof RedisDynamicTableSource)) {
            return false;
        } else {
            RedisDynamicTableSource that = (RedisDynamicTableSource)o;
            return Objects.equals(this.physicalDataType, that.physicalDataType) && Objects.equals(this.decodingFormat, that.decodingFormat) && Objects.equals(this.flinkJedisConfigBase, that.flinkJedisConfigBase) && Objects.equals(this.redisLookupOptions, that.redisLookupOptions) && Objects.equals(this.primarykey, that.primarykey) && Objects.equals(that.hashName, that.hashName);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.physicalDataType, this.decodingFormat, this.flinkJedisConfigBase, this.redisLookupOptions, this.hashName);
    }

    private static  DataType projectRow(DataType dataType, int[] indices){
        final int[][] indexPaths =
                IntStream.of(indices).mapToObj(i -> new int[] {i}).toArray(int[][]::new);
        return projectRow(dataType, indexPaths);
    }
    private static DataType projectRow(DataType dataType, int[][] indexPaths) {
        final List<RowType.RowField> updatedFields = new ArrayList<>();
        final List<DataType> updatedChildren = new ArrayList<>();
        Set<String> nameDomain = new HashSet<>();
        int duplicateCount = 0;
        for (int[] indexPath : indexPaths) {
            DataType fieldType = dataType.getChildren().get(indexPath[0]);
            LogicalType fieldLogicalType = fieldType.getLogicalType();
            StringBuilder builder =
                    new StringBuilder(
                            ((RowType) dataType.getLogicalType())
                                    .getFieldNames()
                                    .get(indexPath[0]));
            for (int index = 1; index < indexPath.length; index++) {
                Preconditions.checkArgument(
                        hasRoot(fieldLogicalType, LogicalTypeRoot.ROW), "Row data type expected.");
                RowType rowtype = ((RowType) fieldLogicalType);
                builder.append("_").append(rowtype.getFieldNames().get(indexPath[index]));
                fieldLogicalType = rowtype.getFields().get(indexPath[index]).getType();
                fieldType = fieldType.getChildren().get(indexPath[index]);
            }
            String path = builder.toString();
            while (nameDomain.contains(path)) {
                path = builder.append("_$").append(duplicateCount++).toString();
            }
            updatedFields.add(new RowType.RowField(path, fieldLogicalType));
            updatedChildren.add(fieldType);
            nameDomain.add(path);
        }
        return new FieldsDataType(
                new RowType(dataType.getLogicalType().isNullable(), updatedFields),
                dataType.getConversionClass(),
                updatedChildren);
    }

    public static int[] createValueFormatProjection(
            DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                hasRoot(physicalType, LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);

        return physicalFields.toArray();
    }
}
