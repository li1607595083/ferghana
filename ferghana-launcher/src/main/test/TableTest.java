import com.skyon.function.FunMapJsonForPars;
import com.skyon.utils.FlinkUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;

import java.util.HashMap;

import static org.apache.flink.table.api.Expressions.$;

public class TableTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();
        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv);

        Row row = new Row(5);
        row.setField(0, "001");
        row.setField(1, "fqewrgr24535");
        row.setField(2, "大宝");
        row.setField(3, "9.99");
        row.setField(4, "2020-08-04T08:37:32.581Z");
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
                + "CAST(price AS DOUBLE) AS price, "
                + "CAST(trade_time AS TIMESTAMP) AS trade_time "
                + "FROM shrq_test");
        table.printSchema();
        dbTableEnv.toAppendStream(table, Row.class).print();
        dbEnv.execute();
    }

}
