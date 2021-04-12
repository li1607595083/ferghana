import com.skyon.udf.NullForObject;
import com.skyon.utils.FlinkUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.planner.expressions.Null;
import org.apache.flink.table.planner.expressions.Null$;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.nullOf;

public class TableTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();
        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv);

        Row row = new Row(5);
        row.setField(0, "001");
        row.setField(1,"2344");
//        nullOf(DataTypes.STRING()).asSummaryString()
        row.setField(2, "null");
        row.setField(3, "9.99");
        row.setField(4, "2020-08-04 8:37:32.581");
        DataStream<Row> source = dbEnv.fromElements(row);

        Expression[] expressions = new Expression[5];
        expressions[0] = $("uid");
        expressions[1] = $("aid");
        expressions[2] = $("name");
        expressions[3] = $("price");
        expressions[4] = $("trade_time");
        dbTableEnv.createTemporaryView("shrq_test", source, expressions);
        Table table3 = dbTableEnv.sqlQuery("SELECT * FROM shrq_test");
        table3.printSchema();
        dbTableEnv.toAppendStream(table3,Row.class).print();


        Table table = dbTableEnv.sqlQuery("SELECT "
                + "CAST(uid AS STRING) AS uid, "
                + "CAST(aid AS STRING) AS aid, "
                + "CAST(name AS STRING) AS name, "
                + "CAST(price AS DOUBLE) AS price, "
                + "CAST(trade_time AS TIMESTAMP) AS trade_time "
                + "FROM shrq_test");
//        table.printSchema();
//        dbTableEnv.toAppendStream(table, Row.class).print();
        dbTableEnv.createTemporaryView("fqr", table);
        dbTableEnv.createTemporarySystemFunction("ifFalseSetNull", new NullForObject());

//        Table table2 = dbTableEnv.sqlQuery("SELECT if(name='null', ifFalseSetNull(), 'wcr') as jishu FROM fqr");
        Table table2 = dbTableEnv.sqlQuery("SELECT if(ifFalseSetNull() IS NULL, true, false) as jishu FROM fqr");
        dbTableEnv.toAppendStream(table2, Row.class).print();
//        Table table1 = dbTableEnv.sqlQuery("SELECT * FROM (SELECT if(uid = '001', uid, CAST(ifFalseSetNull() AS STRING)) AS uid, name, price FROM fqr) AS tmp");
//        dbTableEnv.toAppendStream(table1, Row.class).print();
        dbEnv.execute();
    }

}
