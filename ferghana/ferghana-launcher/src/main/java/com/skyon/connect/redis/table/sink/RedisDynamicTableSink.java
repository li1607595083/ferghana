package com.skyon.connect.redis.table.sink;

import javax.annotation.Nullable;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import com.skyon.connect.redis.mapper.RedisSetMapper;
import com.skyon.connect.redis.options.RedisWriteOptions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/sourcessinks/
 *
 * https://www.alibabacloud.com/help/zh/faq-detail/118038.htm?spm=a2c63.q38357.a3.16.48fa711fo1gVUd
 */
public class RedisDynamicTableSink implements DynamicTableSink {

    /**
     * Data type to configure the formats.
     */
    protected final DataType physicalDataType;

    protected final RedisWriteOptions redisWriteOptions;

    public RedisDynamicTableSink(
            DataType physicalDataType
            , RedisWriteOptions redisWriteOptions) {

        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.redisWriteOptions = redisWriteOptions;
    }

    private @Nullable
    SerializationSchema<RowData> createSerialization(
            Context context,
            @Nullable EncodingFormat<SerializationSchema<RowData>> format,
            int[] projection) {
        if (format == null) {
            return null;
        }
//        DataType physicalFormatDataType =
//                DataTypeUtils.projectRow(this.physicalDataType, projection);
        DataType physicalFormatDataType = projectRow(this.physicalDataType, projection);
        return format.createRuntimeEncoder(context, physicalFormatDataType);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // UPSERT mode
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        FlinkJedisConfigBase flinkJedisConfigBase = new FlinkJedisPoolConfig.Builder()
                .setHost(this.redisWriteOptions.getHostname())
                .setPort(this.redisWriteOptions.getPort())
                .build();

        RedisMapper<RowData> redisMapper = null;

        switch (this.redisWriteOptions.getWriteMode()) {
            case "string":
                redisMapper = new RedisSetMapper();
                break;
            default:
                throw new RuntimeException("其他类型 write mode 请自定义实现");
        }

        return SinkFunctionProvider.of(new RedisSink<>(
                flinkJedisConfigBase
                , redisMapper));
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return "redis";
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
}
