package com.skyon.connect.redis.table;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import com.skyon.connect.redis.options.*;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import com.skyon.connect.redis.table.source.RedisDynamicTableSource;

import static org.apache.flink.configuration.RestOptions.PORT;


public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue().withDescription("Optional password for connect to redis");
    public static final ConfigOption<String> MODE = ConfigOptions.key("mode").stringType().noDefaultValue().withDescription("Optional mode for connect to redis");
    public static final ConfigOption<String> SINGLENODE = ConfigOptions.key("single-node").stringType().noDefaultValue().withDescription("Optional node for connect to redis");
    public static final ConfigOption<String> CLUSTERNODES = ConfigOptions.key("cluster-nodes").stringType().noDefaultValue().withDescription("Optional nodes for connect to redis cluster");
    public static final ConfigOption<Integer> DATABASE = ConfigOptions.key("database").intType().defaultValue(0).withDescription("Optional database for connect to redis");
    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS = ConfigOptions.key("lookup.cache.max-rows").longType().defaultValue(-1L).withDescription("the max number of rows of lookup cache, over this value, the oldest rows will be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is specified. Cache is not enabled as default.");
    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL = ConfigOptions.key("lookup.cache.ttl").durationType().defaultValue(Duration.ofSeconds(0)).withDescription("the cache time to live.");
    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions.key("lookup.max-retries").intType().defaultValue(3).withDescription("the max retry times if lookup database failed.");
    public static final ConfigOption<String> HASHNAME = ConfigOptions.key("hashname").stringType().defaultValue(null).withDescription("the hash key name in hash mode.");
    public static final ConfigOption<String> VALUE_FORMAT = ConfigOptions.key("value.format").stringType().defaultValue("string").withDescription("Defines the format identifier for encoding value data. The identifier is used to discover a suitable format factory.");

    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MODE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SINGLENODE);
        options.add(CLUSTERNODES);
        options.add(DATABASE);
        options.add(PASSWORD);
        options.add(HASHNAME);
        options.add(LOOKUP_CACHE_MAX_ROWS);
        options.add(LOOKUP_CACHE_TTL);
        options.add(LOOKUP_MAX_RETRIES);
        options.add(VALUE_FORMAT);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // get the validated options
        final ReadableConfig options = helper.getOptions();
        // discover a suitable decoding format
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(DeserializationFormatFactory.class, VALUE_FORMAT);
        // validate all options
        helper.validate();
        TableSchema schema = context.getCatalogTable().getSchema();
        validatePrimaryKey(schema);
        validateMode(options);
        String primarykey = schema.getPrimaryKey().get().getColumns().get(0);
        return new RedisDynamicTableSource(schema.toPhysicalRowDataType(), decodingFormat, getFlinkJedisConfigBase(options), getRedisLookupOptions(options), primarykey, options.get(HASHNAME));
    }


    private void validateMode(ReadableConfig readableConfig) {
        Pattern pattern = Pattern.compile( "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}:[0-9]{1,5} " );
        if (readableConfig.getOptional(SINGLENODE).isPresent() && readableConfig.getOptional(CLUSTERNODES).isPresent()){
            throw new IllegalArgumentException("Single mode and cluster mode cannot coexist");
        }
        if (!readableConfig.getOptional(SINGLENODE).isPresent() && !readableConfig.getOptional(CLUSTERNODES).isPresent()){
            throw new IllegalArgumentException("Connection mode cannot be empty.");
        }
        if (readableConfig.get(MODE).equals("single")){
            if (readableConfig.getOptional(CLUSTERNODES).isPresent()){
                throw new IllegalArgumentException("single mode should correspond to single-node.");
            }else {
                String hostnameAndPort = readableConfig.get(SINGLENODE);
                if (hostnameAndPort.split(",").length != 1 || !pattern.matcher(hostnameAndPort + " ").find()){
                    throw new IllegalArgumentException("The value of single-node is incorrect: " + hostnameAndPort);
                }
            }
        } else if (readableConfig.get(MODE).equals("cluster")){
            if (readableConfig.getOptional(SINGLENODE).isPresent()){
                throw new IllegalArgumentException("cluster mode should correspond to cluster-nodes.");
            } else {
                String hostnameAndPort = readableConfig.get(CLUSTERNODES);
                for (String s : hostnameAndPort.split(",")) {
                    if (!pattern.matcher(s + " ").find()){
                        throw new IllegalArgumentException("The value of cluster-node is incorrect: " + hostnameAndPort);
                    }
                }
            }
        } else {
            throw new IllegalArgumentException("Currently, only the single or cluster mode is supported.");
        }

    }

    private void validatePrimaryKey(TableSchema schema) {
        Optional<UniqueConstraint> primaryKey = schema.getPrimaryKey();
        if (!primaryKey.isPresent()){
            throw new IllegalArgumentException("The primary key cannot be empty.");
        } else {
            List<String> columns = primaryKey.get().getColumns();
            if (columns.size() != 1){
                throw new IllegalArgumentException("The number of primary keys can be only one.");
            }
            if (!schema.getFieldDataType(columns.get(0)).get().toString().startsWith("STRING")){
                throw new IllegalArgumentException("The primary key type can only be STRING.");
            }
        }
    }

    private FlinkJedisConfigBase getFlinkJedisConfigBase(ReadableConfig readableConfig) {
        if (readableConfig.get(MODE).equals("single")){
            String singnode = readableConfig.get(SINGLENODE);
            String[] hostnameAndPort = singnode.split(":");
            FlinkJedisPoolConfig.Builder builder = new FlinkJedisPoolConfig.Builder()
                    .setHost(hostnameAndPort[0])
                    .setPort(Integer.parseInt(hostnameAndPort[1]))
                    .setDatabase(readableConfig.get(DATABASE));
            readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
            return builder.build();
        } else {
            Set<InetSocketAddress> nodeSet = new HashSet<>();
            String clusterNodes = readableConfig.get(CLUSTERNODES);
            for (String node : clusterNodes.split(",")) {
                String[] hostnameAndPort = node.split(":");
                InetSocketAddress inetSocketAddress = new InetSocketAddress(hostnameAndPort[0], Integer.parseInt(hostnameAndPort[1]));
                nodeSet.add(inetSocketAddress);
            }
            return new FlinkJedisClusterConfig.Builder()
                    .setNodes(nodeSet)
                    .build();
        }
    }


    private RedisLookupOptions getRedisLookupOptions(ReadableConfig readableConfig) {
        return new RedisLookupOptions((Long)readableConfig.get(LOOKUP_CACHE_MAX_ROWS),((Duration)readableConfig.get(LOOKUP_CACHE_TTL)).toMillis(), (Integer)readableConfig.get(LOOKUP_MAX_RETRIES));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
//
//        // either implement your custom validation logic here ...
//        // or use the provided helper utility
//        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
//
//        // discover a suitable decoding format
////        final EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
////                SerializationFormatFactory.class,
////                FactoryUtil.FORMAT);
//
//        // validate all options
//        helper.validate();
//
//        // get the validated options
//        final ReadableConfig options = helper.getOptions();
//
//        final RedisWriteOptions redisWriteOptions = RedisOptions2.getRedisWriteOptions(options);
//
//        TableSchema schema = context.getCatalogTable().getSchema();
//
//        return new RedisDynamicTableSink(schema.toPhysicalRowDataType()
//                , redisWriteOptions);
        return  null;
    }
}