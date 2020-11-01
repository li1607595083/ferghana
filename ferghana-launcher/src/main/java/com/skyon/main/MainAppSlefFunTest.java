package com.skyon.main;

import com.skyon.udf.NullForObject;
import com.skyon.utils.FlinkUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class MainAppSlefFunTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();
        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv);

        dbTableEnv.executeSql("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS  ifFalseSetNull  AS 'com.skyon.selfudf.TestNiceForObject' LANGUAGE JAVA");
//        dbTableEnv.createTemporarySystemFunction("ifFalseSetNull", new NullForObject());

        Row row = new Row(5);
        row.setField(0, "001");
        row.setField(1, "fqewrgr24535");
        row.setField(2, "大宝");
        row.setField(3, "10.99");
        row.setField(4, "2020-08-04 08:37:32.581");
        DataStream<Row> source = dbEnv.fromElements(row);

        Expression[] expressions = new Expression[5];
        expressions[0] = $("uid");
        expressions[1] = $("aid");
        expressions[2] = $("name");
        expressions[3] = $("price");
        expressions[4] = $("trade_time");
        dbTableEnv.createTemporaryView("shrq_test", source, expressions);

        Table table = dbTableEnv.sqlQuery("SELECT "
                + "CAST(uid AS STRING) AS uid, "
                + "CAST(aid AS STRING) AS aid, "
                + "CAST(name AS STRING) AS name, "
                + "IF(CAST(price AS DOUBLE) < 10, price, ifFalseSetNull()) AS price, "
                + "CAST(trade_time AS TIMESTAMP) AS trade_time "
                + "FROM shrq_test");

        table.printSchema();

        dbTableEnv.toAppendStream(table, Row.class).print();

        dbEnv.execute();
    }

}
