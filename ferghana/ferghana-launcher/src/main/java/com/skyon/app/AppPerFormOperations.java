package com.skyon.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import com.skyon.bean.*;
import com.skyon.function.*;
import com.skyon.sink.KafkaSink;
import com.skyon.sink.StoreSink;
import com.skyon.type.TypeTrans;
import com.skyon.utils.DataStreamToTable;
import com.skyon.utils.KafkaUtils;
import com.skyon.utils.ParameterUtils;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.elasticsearch.index.engine.Engine;
import org.jetbrains.annotations.NotNull;
import org.omg.CORBA.DATA_CONVERSION;

import javax.xml.crypto.Data;
import java.awt.*;
import java.text.ParseException;
import java.time.Duration;
import java.util.*;
import java.util.List;

import static com.skyon.app.AppInputTestData.inputDataToHbase;
import static com.skyon.app.AppInputTestData.inputDataToJdbc;
import static com.skyon.app.AppInputTestData.inputDataToRedis;
import static org.apache.flink.table.api.Expressions.*;

/**
 * @DESCRIBE 主应用程序，执行操作类；
 */
public class AppPerFormOperations {
    /*参数属性*/
    private Properties properties;
    private StreamTableEnvironment dbTableEnv;
    private StreamExecutionEnvironment dbEnv;
    // 初始化表
    private String initTableName = "";

    public AppPerFormOperations() {}

    /**
     * @desc 对成员变量赋值
     * @param properties
     */
    public AppPerFormOperations(Properties properties, StreamTableEnvironment dbTableEnv, StreamExecutionEnvironment dbEnv) {
        // 成员变量引用
        this.properties = properties;
        this.dbTableEnv = dbTableEnv;
        this.dbEnv = dbEnv;
    }

    public void queryRegisterView(String querySql, String name){
        Table table = dbTableEnv.sqlQuery(querySql);
        dbTableEnv.createTemporaryView(name, table);
        initTableName = name;
    }

    /**
     * @desc 创建维表，用以查询
     */
    public void createDimTabl() throws Exception {
        String dimtable = properties.getProperty(ParameterName.DIMENSION_TABLE);
        if (dimtable != null){
            for (Object obj : JSONObject.parseArray(dimtable).toArray()) {
                HashMap hashMap = JSON.parseObject(obj.toString(), HashMap.class);
                String sql = hashMap.get(ParameterName.DIMENSION_TABLE_SQL).toString();
                String type = hashMap.get(ParameterName.DIM_TYPE).toString();
                Object  data = hashMap.get(ParameterName.DIM_DATA);
                if (RunMode.TEST_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
                    if (DimensionType.DIM_MYSQL.equals(type) || DimensionType.DIM_ORACLE.equals(type)){
                        boolean isMysql = DimensionType.DIM_MYSQL.equals(type);
                        inputDataToJdbc(sql, data == null ? null : data.toString(), isMysql);
                    } else if (DimensionType.DIM_HBASE.equals(type)){
                        inputDataToHbase(sql, data == null ? null : data.toString());
                    } else if (DimensionType.DIM_REDIS.equals(type)){
                        String[] hostAndPort = properties.getProperty(ParameterName.TEST_REDIS_NODE).split(":", 2);
                        inputDataToRedis(properties.getProperty(ParameterConfigName.REDIS_HASHNAME), hostAndPort[0], hostAndPort[1] ,data == null ? null : data.toString());
                    }
                }
                dbTableEnv.executeSql(sql);
            }
        }
    }


    /**
     * @desc 数据源表和数据维表关联并注册成一张新表
     */
    public void soureAndDimConcat() {
        String joinSql = properties.getProperty(ParameterName.SOURCE_JOIN_DIM_SQL);
        if (joinSql != null){
            String[] split = joinSql.split("\\(", 2);
            String viewName = split[0].split("\\s+")[2];
            String querySql = split[1].substring(0, split[1].length() - 1);
            queryRegisterView(querySql, viewName);
        }
    }

    public void twoStreamConcat(){
        if (SourceType.TWO_STREAM_JOIN.equals(properties.getProperty(ParameterName.SOURCE_TYPE))){
            Table table = dbTableEnv.sqlQuery(properties.getProperty(ParameterName.TWO_STREAM_JOIN_SQL));
            dbTableEnv.createTemporaryView(properties.getProperty(ParameterName.TWO_STREAM_JOIN_REGISTER_TABLE_NAME),table);
            initTableName = properties.getProperty(ParameterName.TWO_STREAM_JOIN_REGISTER_TABLE_NAME);
        }
    }

    private int getSqlNumbers(){
        String aggPartitionSqls = properties.getProperty(ParameterName.AGG_PARTITION_SQL);
        String transitionSqls = properties.getProperty(ParameterName.TRANSITION_SQL);
        String aggNoPartitionSqls = properties.getProperty(ParameterName.AGG_NO_PARTITION_SQL);
        String deriveSqls = properties.getProperty(ParameterName.DERIVE_SQL);
        int  couts = 0;
        couts = couts + (aggPartitionSqls != null ? aggPartitionSqls.trim().split(";").length : 0);
        couts = couts + (transitionSqls != null ? transitionSqls.trim().split(";").length : 0);
        couts = couts + (aggNoPartitionSqls != null ? aggNoPartitionSqls.trim().split(";").length : 0);
        couts = couts + (deriveSqls != null ? deriveSqls.trim().split(";").length : 0);
        return couts;
    }

    private  ArrayList<String> getSqlSets(){
        ArrayList<String> strings = new ArrayList<>();
        String aggPartitionSqls = properties.getProperty(ParameterName.AGG_PARTITION_SQL);
        String transitionSqls = properties.getProperty(ParameterName.TRANSITION_SQL);
        String aggNoPartitionSqls = properties.getProperty(ParameterName.AGG_NO_PARTITION_SQL);
        String deriveSqls = properties.getProperty(ParameterName.DERIVE_SQL);
        if (aggPartitionSqls != null) strings.add(aggPartitionSqls.trim());
        if (transitionSqls != null) strings.add(transitionSqls.trim());
        if (aggNoPartitionSqls != null) strings.add(aggNoPartitionSqls.trim());
        if (deriveSqls != null) strings.add(deriveSqls.trim());
        return strings;
    }


    /**
     * @desc 执行变量
     */
    public Tuple2<SingleOutputStreamOperator<String>, Map<String, String>> variableExec() {
        Tuple2<SingleOutputStreamOperator<String>, Map<String, String>> result = null;
        boolean isStart = false;
        boolean isEnd;
        int tmpTanleNameNum = 0;
        int sqlCounts = getSqlNumbers();
        String execSql;
        for (String sqlSet : getSqlSets()) {
            for (String sql : sqlSet.split(";")) {
                execSql = getExecSql(isStart, tmpTanleNameNum, sql);
                isStart = true;
                sqlCounts -= 1;
                isEnd = (sqlCounts == 0);
                Tuple2<SingleOutputStreamOperator<Tuple2<Long, String>>, LinkedHashMap<String, String>> sqlExec = sqlExec(execSql, isEnd, sql);
                if (!isEnd){
                    registerTableView(tmpTanleNameNum, sqlExec);
                } else {
                    result = Tuple2.of(sqlExec.f0.map(value -> value.f1), sqlExec.f1);
                }
                tmpTanleNameNum += 1;
            }
        }
        return result;
    }

    private void registerTableView(int tmpTanleNameNum, Tuple2<SingleOutputStreamOperator<Tuple2<Long, String>>, LinkedHashMap<String, String>> sqlExec) {
        String registerTableName = ParameterValue.TMP_TABLE + "_" + tmpTanleNameNum;
        SingleOutputStreamOperator<String> map = sqlExec.f0
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forGenerator((WatermarkGeneratorSupplier<Tuple2<Long, String>>) context -> new WaterMarkGeneratorCounuser(1))
                                .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<Long, String>>) (element, recordTimestamp) -> element.f0)
                ).map(value -> value.f1).filter(Objects::nonNull);
        DataStreamToTable.registerTable(dbTableEnv, map, registerTableName, false, sqlExec.f1, properties.getProperty(ParameterName.WATERMARK).split("[|]")[0]);


    }

    @NotNull
    private String getExecSql(boolean isStart, int tmpTanleNameNum, String sql) {
        String execSql;
        if (!isStart){
            execSql  = sql.replaceFirst(ParameterValue.TMP_TABLE, initTableName);
        } else {
            execSql = sql.replaceFirst(ParameterValue.TMP_TABLE,(ParameterValue.TMP_TABLE + "_" + (tmpTanleNameNum - 1)));
        }
        return execSql;
    }

    public void testMOde(Tuple2<SingleOutputStreamOperator<String>, Map<String, String>> result) throws Exception {
        String testTopicName = properties.getProperty(ParameterName.TEST_TOPIC_NAME);
        if (RunMode.TEST_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            ZkUtils zkUtils = KafkaUtils.getZkUtils(properties.getProperty(ParameterName.TEST_ZK));
            KafkaUtils.deleteKafkaTopic(zkUtils, testTopicName);
            KafkaUtils.createKafkaTopic(zkUtils, testTopicName);
            KafkaUtils.clostZkUtils(zkUtils);
            if (properties.getProperty(ParameterName.SINK_SQL) != null){
                DataStreamToTable.registerTable(dbTableEnv,result.f0, properties.getProperty(ParameterName.MIDDLE_TABLE_NAME), true,result.f1, null);
                sink(result.f1);
            } else if (properties.getProperty(ParameterName.DECISION_SQL) != null){
                DataStreamToTable.registerTable(dbTableEnv,result.f0,properties.getProperty(ParameterName.MIDDLE_TABLE_NAME), true,result.f1, null);
                SingleOutputStreamOperator<String> singleOutputStreamOperator = resultToString(properties.getProperty(ParameterName.DECISION_SQL), ParameterName.DECISION_SQL);
                singleOutputStreamOperator.addSink(KafkaSink.untransaction(testTopicName, properties.getProperty(ParameterName.TEST_BROKER_LIST)));
            } else {
                SingleOutputStreamOperator<String> resultDeal = result.f0.map(new FunTestMapOutput());
                resultDeal.addSink(KafkaSink.untransaction(testTopicName, properties.getProperty(ParameterName.TEST_BROKER_LIST)));
            }

        }
    }


    public  void runMode(Tuple2<SingleOutputStreamOperator<String>, Map<String, String>> result) throws Exception {
        if (RunMode.START_MODE.equals(properties.getProperty(ParameterName.RUM_MODE)) && properties.getProperty(ParameterName.SINK_SQL) != null){
            // 将拼接后的值再注册成一张表，用于后续的决策引擎使用
            DataStreamToTable.registerTable(dbTableEnv,result.f0,properties.getProperty(ParameterName.MIDDLE_TABLE_NAME), true, result.f1, null);
            sink(result.f1);
        }
    }

    public  void cdcMySqlAsyncResult() throws Exception {
        // 获取注册表的所有数据
        String querySql = "SELECT * FROM " + properties.getProperty(ParameterName.CDC_SOURCE_TABLE_NAME);
        Table table = dbTableEnv.sqlQuery(querySql);
        LinkedHashMap<String, String> schema = getSchema(table, false);
        schema.put(ParameterValue.CDC_TYPE, "STRING");
        // 筛选所需要的同步类型数据(新增,更新，删除)，添加数据类型字段
        SingleOutputStreamOperator<String> data_deal = dbTableEnv.toRetractStream(table, Row.class)
                .map(FunMapCdcAddType.of(table.getSchema().getFieldNames(), properties.getProperty(ParameterName.CDC_ROW_KIND)))
                .filter(Objects::nonNull);
        DataStreamToTable.registerTable(dbTableEnv,data_deal,properties.getProperty(ParameterName.MIDDLE_TABLE_NAME),false,schema, null);
        sink(new HashMap<>());
    }

    /**
     * The data base(sink)
     * @throws Exception
     */
    private void sink(Map<String, String> indexfieldNameAndType) throws Exception {
        StoreSink storeSink = new StoreSink(dbTableEnv, properties, indexfieldNameAndType);
        storeSink.sinkTable();
    }



    /**
     * @desc 获取字段名和字段类型
     * @param table
     * @return
     */
    public static LinkedHashMap<String, String> getSchema(Table table, boolean isResult) {
        LinkedHashMap<String, String> fieldNamdType = new LinkedHashMap<>();
        org.apache.flink.table.api.TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();
        for (String fieldName : fieldNames) {
            Optional<DataType> fieldDataType = schema.getFieldDataType(fieldName);
            if (!isResult){
                fieldNamdType.put(fieldName, TypeTrans.getType(fieldDataType.get().toString()));
            } else {
                fieldNamdType.put(fieldName, "STRING");
            }
        }
        return fieldNamdType;
    }


    /**
     * @desc 创建初始数据源表;
     */
    public void createSource() throws Exception {
        // 数据源表创建语句,多个创建表语句之间使用分号;进行拼接
        String[] sourceSqlSets = properties.getProperty(ParameterName.SOURCE_TABLE_SQL).split(";");
        String sourcetype = properties.getProperty(ParameterName.SOURCE_TYPE);
        // 启动模式
        if (properties.getProperty(ParameterName.RUM_MODE).equals(RunMode.START_MODE)){
            // 单个 kafka 数据源
            if (sourcetype.equals(SourceType.ONE_STREAM)){
                dbTableEnv.executeSql(sourceSqlSets[0]);
                if (properties.getProperty(ParameterName.DIMENSION_TABLE) == null){
                    initTableName = ParameterUtils
                            .removeTailFirstSpecialSymbol(sourceSqlSets[0], "(", true)
                            .split("\\s+")[2].replaceAll("`", "");
                }
            // 双流 join
            } else if (sourcetype.equals(SourceType.TWO_STREAM_JOIN)){
                for (String source : sourceSqlSets) {
                    dbTableEnv.executeSql(source);
                }
            // cdc 同步
            } else if (sourcetype.equals(SourceType.MYSQL_CDC)){
                dbTableEnv.executeSql(sourceSqlSets[0]);
            }
        // 测试模式
        } else {
            // 加入测试数据
            AppInputTestData.inputDataToKafka(properties);
            // 单个 kafka 数据源
            if (sourcetype.equals(SourceType.ONE_STREAM)){
                dbTableEnv.executeSql(sourceSqlSets[0]);
                if (properties.getProperty(ParameterName.DIMENSION_TABLE) == null){
                    initTableName = ParameterUtils
                            .removeTailFirstSpecialSymbol(sourceSqlSets[0], "(", true)
                            .split("\\s+")[2].replaceAll("`", "");
                }
            // 双流 join
            } else if (sourcetype.equals(SourceType.TWO_STREAM_JOIN)){
                for (int i = 0; i < sourceSqlSets.length; i++) {
                  dbTableEnv.executeSql(sourceSqlSets[i]);
                }
            }
        }
    }

    /**
     * @param properties
     * @return 创建一个 AppPerFormOperations 实例，并返回；
     */
    public static AppPerFormOperations of(Properties properties,StreamTableEnvironment dbTableEnv,StreamExecutionEnvironment dbEnv){
        return new AppPerFormOperations(properties, dbTableEnv, dbEnv);
    }


    /**
     * @return SQL query collection
     */
    private ArrayList<String> sqlArrQuery(String[] sql_set) {
        ArrayList<String> arr_sql = new ArrayList<>();
        for (int i = 0; i < sql_set.length; i++){
            arr_sql.add(sql_set[i]);
        }
        return arr_sql;
    }

    /**
     * @desc 处理SQL语句，用于UID的设置
     * @param uid
     * @param s
     * @return
     */
    public  static String getUid(String uid, String s) {
        String sql = s.toUpperCase().trim().replaceAll("\\s+", "");
        return uid + sql;
    }


    public SingleOutputStreamOperator<String> resultToString(String sql, String uidPrefix) {
        Table table = dbTableEnv.sqlQuery(sql);
        String uid = uidPrefix;
        uid = getUid(uid, sql);
        SingleOutputStreamOperator<String> resut = dbTableEnv.toAppendStream(table, Row.class, uid)
                .map(FunMapValueMoveTypeAndFieldNmae.of(table.getSchema().getFieldNames()));
        return resut;
    }


    /**
     * Execute the SQL and convert it to DataStream and merge it into a single stream while stitching together field names and field types
     * @return DataStream<String>
     */
    public Tuple2<SingleOutputStreamOperator<Tuple2<Long, String>>, LinkedHashMap<String, String>> sqlExec(String sql, boolean flag, String uidSql) {
        String uid = getUid("",uidSql);
        String dealSql = getOverUnboundedString(sql);
        Table table = dbTableEnv.sqlQuery(dealSql);
        LinkedHashMap<String, String> schema = getSchema(table, flag);
        SingleOutputStreamOperator<Tuple2<Long, String>> process;
        if (!flag){
            process  = dbTableEnv.toAppendStream(table, Row.class, uid)
                    .process(FunMapValueAddFieldName.of(table.getSchema().getFieldNames(), dbEnv.getParallelism(), properties.getProperty(ParameterName.WATERMARK).split("[|]", 2)[0]))
                    .keyBy(value -> value.f0)
                    .process(FunKeyGetWaterMark.of());
        } else {
            process = dbTableEnv.toAppendStream(table, Row.class, uid)
                    .process(FunMapValueAddTypeAadFieldName.of(table.getSchema().getFieldNames(), schema));
        }

        return Tuple2.of(process, schema);
    }


    /**
     * @desc 对 SQL 特殊处理，对于字段为 null 的不参与计算，以避免影响计算结果;
     * @param s
     * @return
     */
    private String getOverUnboundedString(String s) {
        if (s.contains("@_@")) {
            String[] split = s.split("@_@");
            s = split[0].trim()+ " " + split[3].trim();
            Table table = dbTableEnv.sqlQuery(s);
            org.apache.flink.table.api.TableSchema schema = table.getSchema();
            for (String fieldName : schema.getFieldNames()) {
                String type = TypeTrans.getType(schema.getTableColumn(fieldName).get().getType().toString());
                s = s.replaceAll("\\s+from", " FROM").replaceAll("\\s+over", " OVER").replaceAll("\\s+as\\s+", " AS ");
                String[] sp_1 = s.split(" FROM", 2);
                String[] sp_2 = sp_1[0].trim().split(",", 2);
                String[] sp_3 = sp_2[1].trim().split(" OVER", 2);
                String statistics = sp_3[0].trim();
                String[] sp_n = statistics.replaceAll("\\)", "").split("\\(");
                String statistic_field = sp_n[sp_n.length - 1];
                String statistic_fun = "IF(" + statistic_field + " IS NULL, CAST(ifFalseSetNull() AS " + type + ")," + sp_3[0] + "OVER" + sp_3[1].split(" AS ", 2)[0].trim() + ")" + " AS " + sp_3[1].split(" AS ", 2)[1].trim() + " FROM";
                s = sp_2[0] + ", " + statistic_fun + sp_1[1];
            }
        }
        return s;
    }


    /**
     * Indicators to merge
     * @param db_init
     * @return
     */
    public SingleOutputStreamOperator<String> mergerIndicators(DataStream<Tuple2<String, String>> db_init, int counts, String uid, String watermark) {
        SingleOutputStreamOperator<String> keyedProcess_indicators_merge = db_init
                .keyBy(value -> value.f0)
                .process(FunKeyedProValCon.of(counts, watermark)).uid(uid);
        return keyedProcess_indicators_merge;
    }

//    private BroadcastStream<String> getBrocadcats() {
//        DataStreamSource<String> broadStream = dbEnv.fromElements("skyon");
//        // 一个 value descriptor，它描述了用于存储规则名称与规则本身的 map 存储结构
//        MapStateDescriptor<String, String> ruleStateDescriptor = new ValueStateDescriptor<>(
//                "RulesBroadcastState",
//                Types.STRING,
//                Types.STRING);
//
//        // 广播流，广播规则并且创建 broadcast state
//        return broadStream
//                .broadcast(ruleStateDescriptor);
//    }
}
