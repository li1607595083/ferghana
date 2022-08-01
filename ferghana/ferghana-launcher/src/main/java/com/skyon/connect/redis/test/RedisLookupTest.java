package com.skyon.connect.redis.test;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.HashMap;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.concat;


public class RedisLookupTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode().build();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Row> r = env.addSource(new UserDefinedSource()).setParallelism(1);

        Table sourceTable = tEnv.fromDataStream(r.map(v -> v).returns(new RowTypeInfo(Types.STRING,Types.STRING, Types.LONG)), $("f_1"),$("f_2"),$("f_3"),$("proctime").proctime());

        tEnv.createTemporaryView("leftTable",sourceTable);

        String sql = "CREATE TABLE dimTable (\n"
                + "    id STRING,\n"
                + "    name STRING,\n"
                + "    name1 STRING,\n"
                + "    score BIGINT, \n"
                +"     PRIMARY KEY (id) NOT ENFORCED "
                + ") WITH (\n"
                + "  'connector' = 'redis',\n"
                + "  'mode' = 'cluster',\n"
                + "  'single-node' = '192.168.4.95:6379',\n"
                + "  'cluster-nodes' = '192.168.30.97:6379,192.168.30.98:6379,192.168.30.99:6379',\n"
                + "  'value.format' = 'json',\n"
                + "  'lookup.cache.max-rows' = '500',\n"
                + "  'lookup.cache.ttl' = '3600',\n"
                + "  'lookup.max-retries' = '3',\n"
                + "  'hashname' = 'b'\n"
                + ")";

//        , c.score, c.name1
        String joinSql = "SELECT o.f_1, o.f_2, o.f_3, c.id,c.name , c.score, c.name1\n"
                + "FROM leftTable AS o \n"
                + "LEFT JOIN dimTable FOR SYSTEM_TIME AS OF o.proctime AS c \n"
                + "ON o.f_1 = c.id";

        tEnv.executeSql(sql);

        Table t = tEnv.sqlQuery(joinSql);

        // Table t = tEnv.sqlQuery("select * from leftTable");
        DataStream<Row> rowDataStream = tEnv.toAppendStream(t, Row.class);
        rowDataStream.print("c").setParallelism(1);
        env.execute();
    }


    private static class UserDefinedSource implements SourceFunction<Row>, ResultTypeQueryable<Row> {

        private volatile boolean isCancel;

        @Override
        public void run(SourceContext<Row> sourceContext) throws Exception {

            while (!this.isCancel) {
                sourceContext.collect(Row.of("tom001", "b", 1L));
                Thread.sleep(10L);
            }

        }


        @Override
        public void cancel() {
            this.isCancel = true;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            return new RowTypeInfo(TypeInformation.of(String.class), TypeInformation.of(String.class),
                    TypeInformation.of(Long.class));
        }
    }

}
