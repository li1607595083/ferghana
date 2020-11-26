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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.*;
import static org.apache.flink.table.api.Expressions.$;

public class MianAppProcesTest {

    public static void main(String[] args) throws Exception {
        // 获取配置参数
        Properties properties = getProperties(args[0], args[1]);
        // Flink datastream 运行环境
        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv(properties);
        // Flink table 运行环境
        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv);
        // 注册条件函数
         AppRegisFunction.of().registerFunction(dbTableEnv, properties);
        // 创建一个AppIndexOperation实例
        AppDealOperation appOneStreamOver = AppDealOperation.of(properties);
        // 创建数据原表，sourceTableSql为创建Kafka数据原表语句
        appOneStreamOver.createSource(dbTableEnv);
        // 执行数据维维表创建语句
        appOneStreamOver.createDimTabl(dbTableEnv);
        // 指定数据原表与维表拼接语句，使用的是left join, 并将查询结果组成成一张表
        soureAndDimConcat(properties, dbTableEnv, appOneStreamOver);
        // 将衍生SQL与基础SQL进行拼接返回,如果存在衍生SQL,需要对衍生SQL进行表注册
        String deSqlSet = getSqlSet(properties, dbTableEnv, appOneStreamOver);
        // 将衍生SQL和基础SQL合并成一个流
        DataStream<String> sqlQueryAndUnion = appOneStreamOver.sqlQueryAndUnion(dbTableEnv,  deSqlSet);
        // 将计算结果值进行拼接
        SingleOutputStreamOperator<String> singleDeVarSplic = appOneStreamOver.mergerIndicators(sqlQueryAndUnion, Integer.parseInt(properties.getProperty("fieldOutNum")));
        // 创建测试使用的topic, 用于存储计算结果，以便前端读取
        testMOde(properties, dbTableEnv, appOneStreamOver, singleDeVarSplic);
        // 非测试模式
        runMode(properties, dbTableEnv, appOneStreamOver, singleDeVarSplic);
        // 程序执行
        dbEnv.execute();
    }

    private static void runMode(Properties properties, StreamTableEnvironment dbTableEnv, AppDealOperation appOneStreamOver, SingleOutputStreamOperator<String> singleDeVarSplic) throws Exception {
        if ("02".equals(properties.getProperty("runMode")) && properties.getProperty("sinkSql") != null){
            // 将拼接后的值再注册成一张表，用于后续的决策引擎使用
            registerMidTable(dbTableEnv,singleDeVarSplic,appOneStreamOver,appOneStreamOver.middle_table);
            sink(dbTableEnv, properties);
        }
    }

    private static void testMOde(Properties properties, StreamTableEnvironment dbTableEnv, AppDealOperation appOneStreamOver, SingleOutputStreamOperator<String> singleDeVarSplic) throws Exception {
        if ("01".equals(properties.getProperty("runMode"))){
            ZkUtils zkUtils = KafkaUtils.getZkUtils(properties.getProperty("testZK"));
            KafkaUtils.deleteKafkaTopic(zkUtils, appOneStreamOver.middle_table);
            KafkaUtils.createKafkaTopic(zkUtils, appOneStreamOver.middle_table);
            KafkaUtils.clostZkUtils(zkUtils);
            final OutputTag<String> outputTag = new OutputTag<String>("side-output", Types.STRING){};
            if (properties.getProperty("sinkSql") != null){
                SingleOutputStreamOperator<String> singleOutputStreamOperator = singleDeVarSplic.process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value);
                        ctx.output(outputTag, value);
                    }
                });
                DataStream<String> sideOutput = singleOutputStreamOperator.getSideOutput(outputTag);
                sideOutput.addSink(KafkaSink.untransaction(appOneStreamOver.middle_table, properties.getProperty("testBrokeList")));
                registerMidTable(dbTableEnv,singleOutputStreamOperator,appOneStreamOver,appOneStreamOver.middle_table);
                sink(dbTableEnv, properties);
            } else {
                // sink 到中间 topic
                singleDeVarSplic.addSink(KafkaSink.untransaction(appOneStreamOver.middle_table, properties.getProperty("testBrokeList")));
            }

        }
    }

    private static void soureAndDimConcat(Properties properties, StreamTableEnvironment dbTableEnv, AppDealOperation appOneStreamOver) {
        if (properties.getProperty("joinSql") != null){
            String joinSql = properties.getProperty("joinSql").trim();
            String[] split = joinSql.split("\\(", 2);
            String viewName = split[0].split("\\s+")[2];
            String querySql = split[1].substring(0, split[1].length() - 1);
            appOneStreamOver.queryRegisterView(dbTableEnv,querySql, viewName);
        }
    }

    /**
     * Parse the JSON and return properties
     * @param str
     * @return
     */
    public static Properties getProperties(String str, String str2) throws IOException {
        // 对加密参数进行解密
        byte[] decoded = Base64.getDecoder().decode(str);
        // 直接转换为字符串,转换后为JSON格式
        String meta = new String(decoded);
        JSONObject jsonObject = JSON.parseObject(meta);
        Iterator<Map.Entry<String, Object>> iterator = jsonObject.entrySet().iterator();
        Properties properties = new Properties();
        while (iterator.hasNext()){
            Map.Entry<String, Object> kv = iterator.next();
            // 替换操作,主要是对于创建表语句的连接信息(eg:"'connector' = 'hbase-1.4', 'table-name' = 'test:ordeproduct')进行格式上的统一
            // 以便后续通过("','"),进行拆分
            properties.put(kv.getKey(), kv.getValue().toString().trim().replaceAll("'\\s*,\\s*'", "','"));
        }
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(str2);
        Properties parameterToolProperties = parameterTool.getProperties();
        Set<Object> keySet = parameterToolProperties.keySet();
        for (Object key : keySet) {
            Object values = parameterToolProperties.get(key);
            properties.put(key.toString(), values.toString());
        }
        return properties;
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
                DataStream<String> deSqlQueryAndUnion = appOneStreamOver.sqlQueryAndUnion(dbTableEnv,  split[0]);
                SingleOutputStreamOperator<String> singleDeVarSplic = appOneStreamOver.mergerIndicators(deSqlQueryAndUnion, Integer.parseInt(split[3]));
                registerMidTable(dbTableEnv, singleDeVarSplic,appOneStreamOver, split[2]);
            }
        }
        return deSqlSet;
    }

    /**
     * The spliced value is then registered into a table
     * @param dbTableEnv
     * @param appOneStreamOver
     * @param tableName
     */
    public static void registerMidTable(StreamTableEnvironment dbTableEnv, DataStream<String> singleDeVarSplic, AppDealOperation appOneStreamOver, String tableName) {
        LinkedHashMap<String, String> singleFieldTypeHashMap = appOneStreamOver.singleFieldTypeHashMap;
        TableSchema tableSchema = new TableSchema(singleFieldTypeHashMap).invoke();
        Expression[] expressions = tableSchema.getExpressions();
        String sch = tableSchema.getSch();
        RowTypeInfo rowTypeInfo = tableSchema.getRowTypeInfo();
        SingleOutputStreamOperator<Row> mapSingleOutputStreamOperator = singleDeVarSplic.map(new FunMapJsonForPars(singleFieldTypeHashMap)).returns(rowTypeInfo);
        dbTableEnv.createTemporaryView(tableName + "_sky", mapSingleOutputStreamOperator, expressions);
        Table table = dbTableEnv.sqlQuery("SELECT " + sch + " FROM " + tableName + "_sky");
        dbTableEnv.createTemporaryView("`" + tableName + "`", table);
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
                nameArr[i] = "`" + name + "`";
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

    /**
     * The data base(sink)
     * @param dbTableEnv
     * @param properties
     * @throws Exception
     */
    public static void sink(StreamTableEnvironment dbTableEnv, Properties properties) throws Exception {
        StoreSink storeSink = new StoreSink(dbTableEnv, properties);
        storeSink.sinkTable();
    }
}
