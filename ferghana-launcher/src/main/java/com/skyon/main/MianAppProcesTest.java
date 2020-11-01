package com.skyon.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.skyon.app.AppDealOperation;
import com.skyon.app.AppRegisFunction;
import com.skyon.function.FunMapJsonForPars;
import com.skyon.sink.KafkaSink;
import com.skyon.sink.StoreSink;
import com.skyon.utils.FlinkUtils;
import com.skyon.utils.KafkaUtils;
import kafka.utils.ZkUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;
import java.util.*;
import static org.apache.flink.table.api.Expressions.$;

public class MianAppProcesTest {

    /**
     * Parse the JSON and return properties
     * @param meta
     * @return
     */
    public static Properties getProperties(String meta){
        JSONObject jsonObject = JSON.parseObject(meta);
        Iterator<Map.Entry<String, Object>> iterator = jsonObject.entrySet().iterator();
        Properties properties = new Properties();
        while (iterator.hasNext()){
            Map.Entry<String, Object> kv = iterator.next();
            properties.put(kv.getKey(), kv.getValue().toString().trim().replaceAll("'\\s*,\\s*'", "','"));
        }
        return properties;
    }

    public static void main(String[] args) throws Exception {

        // 加密参数
        byte[] decoded = Base64.getDecoder().decode(args[0]);

        // 传入参数信息, JSON格式
        String meta = new String(decoded);
        Properties properties = getProperties(meta);

        // Flink datastream 运行环境
        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();

        // 指定事件类型
        dbEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置并发度
        if ("01".equals(properties.getProperty("runMode"))){
            dbEnv.setParallelism(1);
        }

        // Flink table 运行环境
        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv);

        // 注册条件函数
        AppRegisFunction.of().registerFunction(dbTableEnv);

        // 创建一个AppIndexOperation实例
        AppDealOperation appOneStreamOver = AppDealOperation.of(properties);

        // 创建数据原表，sourceTableSql为创建Kafka数据原表语句
        appOneStreamOver.createSource(dbTableEnv);

        // 执行数据维维表创建语句
        appOneStreamOver.createDimTabl(dbTableEnv);

        // 指定数据原表与维表拼接语句
        if (properties.getProperty("joinSql") != null){
            dbTableEnv.executeSql(properties.getProperty("joinSql"));
        }

        String deSqlSet = getSqlSet(properties, dbTableEnv, appOneStreamOver);

        // 变量的sql语句，并进行合并成一个DataStream
        DataStream<String> sqlQueryAndUnion = appOneStreamOver.sqlQueryAndUnion(dbTableEnv, deSqlSet);
        // 基础变量和衍生变量拼接
        SingleOutputStreamOperator<String> indicators_merger = appOneStreamOver.mergerIndicators(sqlQueryAndUnion, Integer.parseInt(properties.getProperty("fieldOutNum")));
        // 创建中间topic, 用于存储中间计算结果
        ZkUtils zkUtils = KafkaUtils.getZkUtils(properties.getProperty("kafkaZK"));
        if ("01".equals(properties.getProperty("runMode"))){
            KafkaUtils.deleteKafkaTopic(zkUtils, appOneStreamOver.middle_table);
            KafkaUtils.createKafkaTopic(zkUtils, appOneStreamOver.middle_table);
        } else if ("02".equals(properties.getProperty("runMode"))){
            KafkaUtils.createKafkaTopic(zkUtils, appOneStreamOver.middle_table, 1, 1);
        }
        KafkaUtils.clostZkUtils(zkUtils);

        // sink 到中间 topic
        indicators_merger.addSink(KafkaSink.of(appOneStreamOver.middle_table, properties.getProperty("kafkaZK").replace("2181", "9092")))
                .name("sink_kafka");
        if (properties.getProperty("sinkSql") != null){
            appOneStreamOver.schemaConcat(appOneStreamOver.singleFieldTypeHashMap);
            // 使用中间topic创建表
            appOneStreamOver.createMinddleTable(appOneStreamOver.middle_schema, dbTableEnv);
            // 输出到外部存储系统
            StoreSink.sink(dbTableEnv, properties);
        }
        dbEnv.execute();
    }

    private static String getSqlSet(Properties properties, StreamTableEnvironment dbTableEnv, AppDealOperation appOneStreamOver) {
        String deSqlSet = "";
        if (properties.getProperty("variableSqls") != null){
            deSqlSet = deSqlSet + properties.getProperty("variableSqls") + ";";
        }
        if (properties.getProperty("deVariableSqls") != null){
            for (String deVariableSqls : properties.getProperty("deVariableSqls").split("[|]")) {
                String[] split = deVariableSqls.split("@");
                // 执行派生变量的sql语句，并进行合并成一个DataStream
                deSqlSet = deSqlSet + split[1];
                DataStream<String> deSqlQueryAndUnion = appOneStreamOver.sqlQueryAndUnion(dbTableEnv, split[0]);
                LinkedHashMap<String, String> singleFieldTypeHashMap = appOneStreamOver.singleFieldTypeHashMap;
                TableSchema tableSchema = new TableSchema(singleFieldTypeHashMap).invoke();
                Expression[] expressions = tableSchema.getExpressions();
                String sch = tableSchema.getSch();
                RowTypeInfo rowTypeInfo = tableSchema.getRowTypeInfo();
                // 派生变量拼接
                SingleOutputStreamOperator<String> singleDeVarSplic = appOneStreamOver.mergerIndicators(deSqlQueryAndUnion, Integer.parseInt(split[3]));
                SingleOutputStreamOperator<Row> mapSingleOutputStreamOperator = singleDeVarSplic.map(new FunMapJsonForPars(singleFieldTypeHashMap)).returns(rowTypeInfo);
                dbTableEnv.createTemporaryView("mid_" + split[2], mapSingleOutputStreamOperator, expressions);
                Table table = dbTableEnv.sqlQuery("SELECT " + sch + " FROM mid_" + split[2]);
                dbTableEnv.createTemporaryView(split[2], table);
            }
        }
        return deSqlSet;
    }

    static class TableSchema {
        private LinkedHashMap<String, String> singleFieldTypeHashMap;
        private Expression[] expressions;
        private String sch;
        private RowTypeInfo rowTypeInfo;

        public TableSchema(LinkedHashMap<String, String> singleFieldTypeHashMap) {
            this.singleFieldTypeHashMap = singleFieldTypeHashMap;
        }

        public Expression[] getExpressions() {
            return expressions;
        }

        public String getSch() {
            return sch;
        }

        public RowTypeInfo getRowTypeInfo() {
            return rowTypeInfo;
        }

        public TableSchema invoke() {
            expressions = new Expression[singleFieldTypeHashMap.size()];
            Iterator<Map.Entry<String, String>> iterator = singleFieldTypeHashMap.entrySet().iterator();
            int i = 0;
            sch = "";
            TypeInformation[] typeArr = new TypeInformation[singleFieldTypeHashMap.size()];
            String[] nameArr = new String[singleFieldTypeHashMap.size()];
            while (iterator.hasNext()){
                Map.Entry<String, String> next = iterator.next();
                String name = next.getKey();
                String type = next.getValue();
                nameArr[i] = name;
                typeArr[i] = Types.STRING;
                expressions[i] = $(name);
                sch = sch + "CAST(" + name + " AS " +  type + ") AS " + name + ", ";
                i++;
            }
            sch  = sch.trim();
            sch = sch.substring(0, sch.length() - 1);
            rowTypeInfo = new RowTypeInfo(typeArr, nameArr);
            return this;
        }
    }
}
