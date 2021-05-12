package com.skyon.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.skyon.app.AppDealOperation;
import com.skyon.app.AppRegisFunction;
import com.skyon.function.FunMapCdcAddType;
import com.skyon.function.FunMapCdcConcatNameAndValue;
import com.skyon.function.FunMapJsonForPars;
import com.skyon.sink.KafkaSink;
import com.skyon.sink.StoreSink;
import com.skyon.type.TypeTrans;
import com.skyon.utils.FlinkUtils;
import com.skyon.utils.KafkaUtils;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import kafka.utils.ZkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import java.io.IOException;
import java.util.*;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.map;
import static org.apache.flink.table.api.Expressions.randInteger;


public class MianAppProcesTest {

    public static void main(String[] args) throws Exception {
        // 获取配置参数
        Properties properties = getProperties(args[0], args[1]);
        // 访问Oracle时,如果包含TimeStamp类型字段时，需要在提交作业时设置jvm参数参数
        // System.setProperty("oracle.jdbc.J2EE13Compliant", "true");
        // Flink datastream 运行环境
        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv(properties);
        int parallelism = dbEnv.getParallelism();
        properties.put("kafkaProducersPoolSize", parallelism+"");
        // Flink table 运行环境
        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv);
        // 设置状态的最小空闲时间和最大的空闲时间
        dbTableEnv.getConfig().setIdleStateRetentionTime(Time.hours(0), Time.hours(0));
        // 注册条件函数
        AppRegisFunction.of().registerFunction(dbTableEnv, properties);
        // 创建一个AppIndexOperation实例
        AppDealOperation appOneStreamOver = AppDealOperation.of(properties);
        // 创建数据原表，以及双流join注册成新的表，sourceTableSql为创建Kafka数据原表语句
        appOneStreamOver.createSource(dbTableEnv, dbEnv);
        // 执行数据维维表创建语句
        appOneStreamOver.createDimTabl(dbTableEnv);
        // 指定数据原表与维表拼接语句，使用的是left join, 并将查询结果组成成一张表
        soureAndDimConcat(properties, dbTableEnv, appOneStreamOver);
        // 用于计算部分
        if (!appOneStreamOver.checkComputerSql()){
            // 将衍生SQL与基础SQL,以及原始字段SQL语句,进行拼接返回,如果存在衍生SQL,需要对衍生SQL进行表注册,以及通过测流输出获取key和waterMark
            String deSqlSet = getSqlSet(properties, dbTableEnv, appOneStreamOver);
            // 将衍生SQL和基础SQL,以及数据源字段sql合并成一个流
            int couns_where = 0;
            HashMap<Integer, Boolean> isWhere = new HashMap<>();
            for (String s : deSqlSet.split(";")) {
                if (s.toUpperCase().contains("\\s+WHERE\\s+")){
                    couns_where += 1;
                    isWhere.put(couns_where, true);
                }
            }
            DataStream<Tuple2<String, String>> sqlQueryAndUnion = appOneStreamOver.indexUnion(dbTableEnv,  deSqlSet, "deSqlSet", isWhere);
            // 将计算结果值进行拼接
            SingleOutputStreamOperator<String> singleDeVarSplic = appOneStreamOver.mergerIndicators(sqlQueryAndUnion, Integer.parseInt(properties.getProperty("fieldOutNum")), "keyed-uid", appOneStreamOver.fieldSet);
            // 创建测试使用的topic, 用于存储计算结果，以便前端读取
            testMOde(properties, dbTableEnv, appOneStreamOver, singleDeVarSplic);
            // 非测试模式
            runMode(properties, dbTableEnv, appOneStreamOver, singleDeVarSplic);
        } else {
            cdcMySqlAsyncResult(properties, dbTableEnv, appOneStreamOver);
        }
        // 程序执行
        dbEnv.execute(properties.getProperty("variablePackEn", "test"));
    }

    private static void cdcMySqlAsyncResult(Properties properties, StreamTableEnvironment dbTableEnv, AppDealOperation appOneStreamOver) throws Exception {
        String cdcSyncSql = "SELECT * FROM " + appOneStreamOver.middle_table;
        String sinkSql = properties.getProperty("sinkSql");
        if ("01".equals(properties.getProperty("runMode"))){
            if (sinkSql == null){
                Table table = dbTableEnv.sqlQuery(cdcSyncSql);
                List<String> arrFieldName = Arrays.asList(table.getSchema().getFieldNames());
                SingleOutputStreamOperator<String> result = dbTableEnv.toRetractStream(table, Row.class)
                        .map(value -> value.f1)
                        .map(FunMapCdcConcatNameAndValue.of(arrFieldName));
                result.addSink(KafkaSink.untransaction(properties.getProperty("testTopicName"), properties.getProperty("testBrokeList")));
            } else {
                sink(dbTableEnv, properties, new HashMap<>(),true);
            }

        }   else if ("02".equals(properties.getProperty("runMode"))){
            sink(dbTableEnv, properties, new HashMap<>(),false);
        }
    }


    private static void runMode(Properties properties, StreamTableEnvironment dbTableEnv, AppDealOperation appOneStreamOver, SingleOutputStreamOperator<String> singleDeVarSplic) throws Exception {
        if ("02".equals(properties.getProperty("runMode")) && properties.getProperty("sinkSql") != null){
            // 将拼接后的值再注册成一张表，用于后续的决策引擎使用
            registerMidTable(dbTableEnv,singleDeVarSplic,appOneStreamOver,appOneStreamOver.middle_table, true);
            sink(dbTableEnv, properties, appOneStreamOver.indexfieldNameAndType,false);
        }
    }


    private static void testMOde(Properties properties, StreamTableEnvironment dbTableEnv, AppDealOperation appOneStreamOver, SingleOutputStreamOperator<String> singleDeVarSplic) throws Exception {
        String testTopicName = properties.getProperty("testTopicName");
        if ("01".equals(properties.getProperty("runMode"))){
            ZkUtils zkUtils = KafkaUtils.getZkUtils(properties.getProperty("testZK"));
            KafkaUtils.deleteKafkaTopic(zkUtils, testTopicName);
            KafkaUtils.createKafkaTopic(zkUtils, testTopicName);
            KafkaUtils.clostZkUtils(zkUtils);
            if (properties.getProperty("sinkSql") != null){
                registerMidTable(dbTableEnv,singleDeVarSplic,appOneStreamOver,appOneStreamOver.middle_table, true);
                sink(dbTableEnv, properties, appOneStreamOver.indexfieldNameAndType,true);
            } else if (properties.getProperty("decisionSql") != null){
                registerMidTable(dbTableEnv,singleDeVarSplic,appOneStreamOver,appOneStreamOver.middle_table, true);
                AppDealOperation decisionOperation = AppDealOperation.of();
                SingleOutputStreamOperator<String> singleOutputStreamOperator = decisionOperation.resultToString(dbTableEnv, properties.getProperty("decisionSql"), "decisionSql");
                singleOutputStreamOperator.addSink(KafkaSink.untransaction(testTopicName, properties.getProperty("testBrokeList")));
            } else {
                SingleOutputStreamOperator<String> resultDeal = singleDeVarSplic.map(new testMapOuput());
                resultDeal.print("myql_cdc\t");
                resultDeal.addSink(KafkaSink.untransaction(testTopicName, properties.getProperty("testBrokeList")));
            }

        }
    }

    public static void soureAndDimConcat(Properties properties, StreamTableEnvironment dbTableEnv, AppDealOperation appOneStreamOver) {
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
    private static Properties getProperties(String str, String str2) throws IOException {
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
            properties.put(kv.getKey().trim(), kv.getValue().toString().trim().replaceAll("'\\s*,\\s*'", "','"));
        }
        String propFilePath = str2;
        if (str2.contains("@")){
            String[] split = str2.split("@", 2);
            propFilePath = split[0];
            properties.put("savepointPath", split[1].trim());
        }
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propFilePath);
        Properties parameterToolProperties = parameterTool.getProperties();
        Set<Object> keySet = parameterToolProperties.keySet();
        for (Object key : keySet) {
            Object values = parameterToolProperties.get(key);
            properties.put(key.toString().trim(), values.toString().trim());
        }
        if (properties.getProperty("sinkSql") != null){
            String sinkSql = properties.getProperty("sinkSql").trim();
            String[] sp_2 = sinkSql.split("\\(", 2);
            String[] sp_3 = sp_2[0].split("\\s+");
            String src = "SELECT " + properties.getProperty("sourcePrimaryKey");
            String[] fieldAndTable = sp_2[1].replaceAll("from", "FROM").split("FROM", 2);
            for (String s : fieldAndTable[0].trim().split("\\s+", 2)[1].split(",")) {
                if (!s.trim().equals(properties.getProperty("sourcePrimaryKey"))){
                    src = src + ", " + s.trim();
                }
            }
            if (properties.getProperty("runMode").equals("01")){
                sinkSql = sp_3[0] + " " + sp_3[1] + " " + "sink_" + sp_3[2] + "(" + src + " FROM " + fieldAndTable[1];
                if (properties.getProperty("connectorType").equals("01")){
                    properties.setProperty("kafkaTopic", "sink_" + properties.getProperty("testTopicName"));
                }
            } else {
                sinkSql = sp_3[0] + " " + sp_3[1] + " " + sp_3[2] + "(" + src + " FROM " + fieldAndTable[1];
            }
            properties.setProperty("sinkSql", sinkSql);
        }
        return properties;
    }

    private static String getSqlSet(Properties properties, StreamTableEnvironment dbTableEnv, AppDealOperation appOneStreamOver) {
        String deSqlSet = "";
        if (properties.getProperty("originalVariableSql") != null){
            String originalVariableSql = properties.getProperty("originalVariableSql").replace("from", "FROM");
            String variableSqls = properties.getProperty("variableSqls");
            if (variableSqls != null){
                variableSqls = variableSqls.replaceAll("ORDER BY".toLowerCase(), "ORDER BY");
            }
            String deVariableSqls = properties.getProperty("deVariableSqls");
            if (deVariableSqls != null){
                deVariableSqls = deVariableSqls.replaceAll("ORDER BY".toLowerCase(), "ORDER BY");
            }

            String event_time_field = "";
            if (variableSqls != null && variableSqls.contains("ORDER BY")){
                event_time_field = variableSqls.split("ORDER BY", 2)[1].trim().split("\\s+", 2)[0];
            } else if (deVariableSqls != null && deVariableSqls.contains("ORDER BY")){
                event_time_field = deVariableSqls.split("ORDER BY", 2)[1].trim().split("\\s+", 2)[0];
            }
            if (!event_time_field.equals("")){
                String prefix = "";
                String sufix = "";
                String[] sp_1 = originalVariableSql.split("FROM", 2);
                if (sp_1.length == 2){
                    prefix = sp_1[0];
                    sufix = sp_1[1];
                }
                //PARTITION BY "+ properties.getProperty("sourcePrimaryKey") +"
                prefix = prefix + ", " + "COUNT(" + properties.getProperty("sourcePrimaryKey") + ")" + " OVER( ORDER BY " + event_time_field + " RANGE BETWEEN INTERVAL '1' SECOND preceding AND CURRENT ROW) AS tof123445fot ";
                originalVariableSql = prefix + " FROM " + sufix;
            }
            deSqlSet = deSqlSet + originalVariableSql + ";";
        }
        if (properties.getProperty("variableSqls") != null){
            deSqlSet = deSqlSet + properties.getProperty("variableSqls") + ";";
        }
        if (properties.getProperty("deVariableSqls") != null){
            for (String deVariableSqls : properties.getProperty("deVariableSqls").split("[|]")) {
                Integer counts_where = 0;
                String uid = "deVariableSqls";
                String[] split = deVariableSqls.split("@");
                // 执行派生变量的sql语句，并进行合并成一个DataStream
                deSqlSet = deSqlSet + split[1] + ";";
                //INTERVAL '30' MINUTE
                HashMap<Integer, Boolean> isWhere = new HashMap<>();
                int sqlCounts = 0;
                for (String s : split[0].split(";")) {
                    sqlCounts += 1;
                    uid = AppDealOperation.getUid(uid, s);
                    if (s.toUpperCase().contains("\\s+WHERE\\s+")){
                        counts_where += 1;
                        isWhere.put(sqlCounts,true);
                    }
                }
                String[] arr_udi = uid.split("");
                Arrays.sort(arr_udi);
                DataStream<Tuple2<String, String>> deSqlQueryAndUnion = appOneStreamOver.sqlQueryAndUnion(dbTableEnv,  split[0], "deVariableSqls", isWhere);
                SingleOutputStreamOperator<String> singleDeVarSplic = appOneStreamOver.mergerIndicators(deSqlQueryAndUnion, Integer.parseInt(split[3]), Arrays.toString(arr_udi), appOneStreamOver.fieldSet);
                registerMidTable(dbTableEnv, singleDeVarSplic,appOneStreamOver, split[2], false);
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
    private static void registerMidTable(StreamTableEnvironment dbTableEnv, DataStream<String> singleDeVarSplic, AppDealOperation appOneStreamOver, String tableName, Boolean flag) {
        LinkedHashMap<String, String> singleFieldTypeHashMap = appOneStreamOver.singleFieldTypeHashMap;
        registerTable(dbTableEnv, singleDeVarSplic, tableName, flag, singleFieldTypeHashMap, null);
    }

    public static void registerTable(StreamTableEnvironment dbTableEnv, DataStream<String> singleDeVarSplic, String tableName, Boolean flag, LinkedHashMap<String, String> singleFieldTypeHashMap, String timeStampFiled) {
        TableSchema tableSchema = new TableSchema(singleFieldTypeHashMap, timeStampFiled).invoke();
        Expression[] expressions = tableSchema.getExpressions();
        String sch = tableSchema.getSch();
        RowTypeInfo rowTypeInfo = tableSchema.getRowTypeInfo();
        SingleOutputStreamOperator<Row> mapSingleOutputStreamOperator = singleDeVarSplic.map(FunMapJsonForPars.of(singleFieldTypeHashMap, timeStampFiled)).returns(rowTypeInfo);
        if (!flag){
            dbTableEnv.createTemporaryView(tableName + "_sky", mapSingleOutputStreamOperator, expressions);
            String sqlTrans  = null;
            if (timeStampFiled  == null){
                sqlTrans = "SELECT " + sch  + " FROM " + tableName + "_sky";
            } else {
                sqlTrans =  "SELECT " + sch + ", "  + timeStampFiled  + " FROM " + tableName + "_sky";
            }
            Table table = dbTableEnv.sqlQuery(sqlTrans);
            dbTableEnv.createTemporaryView("`" + tableName + "`", table);
        } else {
            dbTableEnv.createTemporaryView("`" + tableName + "`", mapSingleOutputStreamOperator, expressions);
        }
    }

    static class TableSchema {
        private LinkedHashMap<String, String> singleFieldTypeHashMap;
        private Expression[] expressions;
        private String sch;
        private RowTypeInfo rowTypeInfo;
        private String timeStampFiled;

        public TableSchema(LinkedHashMap<String, String> singleFieldTypeHashMap, String timeStampFiled) {
            this.singleFieldTypeHashMap = singleFieldTypeHashMap;
            this.timeStampFiled = timeStampFiled;
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
                if (timeStampFiled == null || !name.equals(timeStampFiled)){
                    typeArr[i] = Types.STRING;
                    expressions[i] = $(name);
                    sch = sch + "CAST(" + name + " AS " +  type + ") AS " + name + ", ";
                } else {
                    typeArr[i] = Types.SQL_TIMESTAMP;
                    expressions[i] = $(name).rowtime();
                }
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
    private static void sink(StreamTableEnvironment dbTableEnv, Properties properties, HashMap<String, String> indexfieldNameAndType, Boolean sideOut) throws Exception {
        StoreSink storeSink = new StoreSink(dbTableEnv, properties, indexfieldNameAndType);
        storeSink.sinkTable(sideOut);
        // 性能测试1
//        storeSink.sinkRedis();
    }


   static class testMapOuput implements MapFunction<String, String>{
        @Override
        public String map(String value) throws Exception {
            HashMap<String, String> hashMap = JSON.parseObject(value, HashMap.class);
            Iterator<Map.Entry<String, String>> iterator = hashMap.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, String> next = iterator.next();
                String ky = next.getKey();
                String vl = next.getValue();
                String[] split = vl.split("&", -1);
                if (split.length == 3){
                    vl = split[2];
                    if (vl.equals("null")){
                        vl = "0";
                    }
                }
                hashMap.put(ky, vl);
            }
            return JSONObject.toJSON(hashMap).toString();
        }
    }
}
