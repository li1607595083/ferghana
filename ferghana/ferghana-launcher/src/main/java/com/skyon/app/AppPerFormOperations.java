package com.skyon.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.skyon.bean.*;
import com.skyon.function.*;
import com.skyon.sink.KafkaSink;
import com.skyon.sink.StoreSink;
import com.skyon.type.TypeTrans;
import com.skyon.utils.DataStreamToTable;
import com.skyon.utils.KafkaUtils;
import kafka.utils.ZkUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.omg.CORBA.DATA_CONVERSION;

import java.util.*;
import static com.skyon.app.AppInputTestData.inputDataToHbase;
import static com.skyon.app.AppInputTestData.inputDataToJdbc;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @DESCRIBE 主应用程序，执行操作类；
 */
public class AppPerFormOperations {
    /*参数属性*/
    private Properties properties;
    private StreamTableEnvironment dbTableEnv;

    public AppPerFormOperations() {}

    /**
     * @desc 对成员变量赋值
     * @param properties
     */
    public AppPerFormOperations(Properties properties, StreamTableEnvironment dbTableEnv) {
        // 成员变量引用
        this.properties = properties;
        this.dbTableEnv = dbTableEnv;
    }

    public void queryRegisterView(String querySql, String name){
        Table table = dbTableEnv.sqlQuery(querySql);
        dbTableEnv.createTemporaryView(name, table);
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
                String  data = hashMap.get(ParameterName.DIM_DATA).toString();
                if (DimensionType.DIM_JDBC.equals(type)){
                    inputDataToJdbc(sql, data);
                } else if (DimensionType.DIM_HBASE.equals(type)){
                    inputDataToHbase(sql, data);
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
        }
    }


    /**
     * @desc 执行变量
     */
    public Tuple2<SingleOutputStreamOperator<String>, LinkedHashMap<String, String>> variableExec() {
        Tuple2<DataStream<Tuple2<String, String>>, LinkedHashMap<String, String>> union = indexUnion(properties.getProperty(ParameterName.SQL_SET), ParameterName.SQL_SET);
        SingleOutputStreamOperator<String> result = mergerIndicators(union.f0, Integer.parseInt(properties.getProperty(ParameterName.FIELD_OUT_NUMBER)), "keyed-uid", properties.getProperty(ParameterName.WATERMARK).split("[|]")[0]);
        return Tuple2.of(result, union.f1);
    }


    /**
     * @desc 执行派生变量
     */
    public void deVariableExec() {
        if (properties.getProperty(ParameterName.DEVARIABLE_SQLS) != null){
            for (String deVariableSqls : properties.getProperty(ParameterName.DEVARIABLE_SQLS).split("[|]")) {
                String[] split = deVariableSqls.split("@");
                String[] arr_udi = deVariableSqls.replaceAll("\\s.", "").split("");
                Arrays.sort(arr_udi);
                Tuple2<DataStream<Tuple2<String, String>>, LinkedHashMap<String, String>> dataStreamLinkedHashMapTuple2 = sqlQueryAndUnion(split[0], Arrays.toString(arr_udi));
                SingleOutputStreamOperator<String> singleDeVarSplic = mergerIndicators(dataStreamLinkedHashMapTuple2.f0, Integer.parseInt(split[3]), Arrays.toString(arr_udi), properties.getProperty(ParameterName.WATERMARK).split("[|]")[0]);
                DataStreamToTable.registerTable(dbTableEnv,singleDeVarSplic,  split[2], false, dataStreamLinkedHashMapTuple2.f1);
            }
        }
    }

    public void testMOde(Tuple2<SingleOutputStreamOperator<String>, LinkedHashMap<String, String>> result) throws Exception {
        String testTopicName = properties.getProperty(ParameterName.TEST_TOPIC_NAME);
        if (RunMode.TEST_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            ZkUtils zkUtils = KafkaUtils.getZkUtils(properties.getProperty(ParameterName.TEST_ZK));
            KafkaUtils.deleteKafkaTopic(zkUtils, testTopicName);
            KafkaUtils.createKafkaTopic(zkUtils, testTopicName);
            KafkaUtils.clostZkUtils(zkUtils);
            if (properties.getProperty(ParameterName.SINK_SQL) != null){
                DataStreamToTable.registerTable(dbTableEnv,result.f0, properties.getProperty(ParameterName.MIDDLE_TABLE_NAME), true,result.f1);
                sink(result.f1,true);
            } else if (properties.getProperty(ParameterName.DEVARIABLE_SQLS) != null){
                DataStreamToTable.registerTable(dbTableEnv,result.f0,properties.getProperty(ParameterName.MIDDLE_TABLE_NAME), true,result.f1);
                SingleOutputStreamOperator<String> singleOutputStreamOperator = resultToString(properties.getProperty(ParameterName.DECISION_SQL), ParameterName.DECISION_SQL);
                singleOutputStreamOperator.addSink(KafkaSink.untransaction(testTopicName, properties.getProperty(ParameterName.TEST_BROKER_LIST)))
                        .name("SINK_OUTPUT_RESUTL");
            } else {
                SingleOutputStreamOperator<String> resultDeal = result.f0.map(new FunTestMapOutput());
                resultDeal.addSink(KafkaSink.untransaction(testTopicName, properties.getProperty(ParameterName.TEST_BROKER_LIST)))
                        .name("SINK_OUTPUT_RESUTL");
            }

        }
    }


    public  void runMode(Tuple2<SingleOutputStreamOperator<String>, LinkedHashMap<String, String>> result) throws Exception {
        if (RunMode.START_MODE.equals(properties.getProperty(ParameterName.RUM_MODE)) && properties.getProperty(ParameterName.SINK_SQL) != null){
            // 将拼接后的值再注册成一张表，用于后续的决策引擎使用
            DataStreamToTable.registerTable(dbTableEnv,result.f0,properties.getProperty(ParameterName.MIDDLE_TABLE_NAME), true, result.f1);
            sink(result.f1,false);
        }
    }

    public  void cdcMySqlAsyncResult() throws Exception {
        // 获取注册表的所有数据
        String querySql = "SELECT * FROM " + properties.getProperty(ParameterName.CDC_SOURCE_TABLE_NAME);
        Table table = dbTableEnv.sqlQuery(querySql);
        LinkedHashMap<String, String> schema = getSchema(table);
        schema.put(ParameterValue.CDC_TYPE, "STRING");
        // 筛选所需要的同步类型数据(新增,更新，删除)，添加数据类型字段
        SingleOutputStreamOperator<String> data_deal = dbTableEnv.toRetractStream(table, Row.class)
                .map(FunMapCdcAddType.of((String[]) schema.keySet().toArray(), properties.getProperty(ParameterName.CDC_ROW_KIND)))
                .filter(Objects::nonNull);
        DataStreamToTable.registerTable(dbTableEnv,data_deal,properties.getProperty(ParameterName.MIDDLE_TABLE_NAME),false,schema);
        sink(new HashMap<>(),false);
    }

    /**
     * The data base(sink)
     * @throws Exception
     */
    private void sink(HashMap<String, String> indexfieldNameAndType, Boolean sideOut) throws Exception {
        StoreSink storeSink = new StoreSink(dbTableEnv, properties, indexfieldNameAndType);
        storeSink.sinkTable(sideOut);
    }



    /**
     * @desc 获取字段名和字段类型
     * @param table
     * @return
     */
    public static LinkedHashMap<String, String> getSchema(Table table) {
        LinkedHashMap<String, String> fieldNamdType = new LinkedHashMap<>();
        org.apache.flink.table.api.TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();
        for (String fieldName : fieldNames) {
            Optional<DataType> fieldDataType = schema.getFieldDataType(fieldName);
            fieldNamdType.put(fieldName, TypeTrans.getType(fieldDataType.get().toString()));
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
    public static AppPerFormOperations of(Properties properties,StreamTableEnvironment dbTableEnv){
        return new AppPerFormOperations(properties, dbTableEnv);
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


    public SingleOutputStreamOperator<String> resultToString(String sinkSql, String uidPrefix) {
        Table table = dbTableEnv.sqlQuery(sinkSql);
        org.apache.flink.table.api.TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();
        String uid = uidPrefix;
        uid = getUid(uid, sinkSql);
        SingleOutputStreamOperator<String> resut = dbTableEnv.toAppendStream(table, Row.class, uid)
                .map(FunMapValueMoveTypeAndFieldNmae.of(fieldNames));
        return resut;
    }


    /**
     * Execute the SQL and convert it to DataStream and merge it into a single stream while stitching together field names and field types
     * @return DataStream<String>
     */
    public Tuple2<DataStream<Tuple2<String, String>>, LinkedHashMap<String, String>> indexUnion(String sqlSet,String uidPrefix) {
        DataStream<Tuple2<String, String>> db_init = null;
        LinkedHashMap<String, String> fieldAndType= new LinkedHashMap<>();
        ArrayList<DataStream<Tuple2<String, String>>> arr_db = new ArrayList<>();
        for (String s : sqlArrQuery(sqlSet.split(";"))) {
            String uid = getUid(uidPrefix, s);
            String dealSql = getOverUnboundedString(s);
            dealSql = addTimeField(dealSql);
            Table table = dbTableEnv.sqlQuery(dealSql);
            fieldAndType = getSchema(table);
            if (db_init == null){
                db_init = dbTableEnv.toAppendStream(table, Row.class, uid)
                        .process(FunMapValueAddTypeAadFieldName.of(table.getSchema().getFieldNames(), fieldAndType,properties.getProperty(ParameterName.SOURCE_PRIMARY_KEY)));
            } else {
                arr_db.add(dbTableEnv.toAppendStream(table, Row.class, uid)
                        .process(FunMapValueAddTypeAadFieldName.of(table.getSchema().getFieldNames(), fieldAndType,properties.getProperty(ParameterName.SOURCE_PRIMARY_KEY))));
            }
        }
        for (DataStream<Tuple2<String, String>> dataStream : arr_db) {
            db_init = db_init.union(dataStream);
        }
        return Tuple2.of(db_init, fieldAndType);
    }


    /**
     * @desc 给每个计算指标添加上时间字段
     * @param s
     */
    private String addTimeField(String s){
        String[] split = s.trim().split("\\s+", 2);
        return split[0] + " " + properties.getProperty(ParameterName.WATERMARK).split("[|]")[0] + "," + split[1];
    }


    /**
     * @desc 执行派生变量所依赖的基础变量，并合并成为一个 DataStream
     * @return DataStream<String>
     */
    public Tuple2<DataStream<Tuple2<String, String>>, LinkedHashMap<String, String>> sqlQueryAndUnion(String sqlSet, String uidPrefix) {
        DataStream<Tuple2<String, String>> db_init = null;
        LinkedHashMap<String, String> fieldAndType = new LinkedHashMap<>();
        ArrayList<DataStream<Tuple2<String, String>>> arr_db = new ArrayList<>();
        for (String s : sqlArrQuery(sqlSet.split(";"))) {
            String uid = getUid(uidPrefix, s);
            String dealSql = getOverUnboundedString(s);
            dealSql = addTimeField(dealSql);
            Table table = dbTableEnv.sqlQuery(dealSql);
            fieldAndType  = getSchema(table);
            if (db_init == null){
                db_init = dbTableEnv.toAppendStream(table, Row.class, uid)
                        .process(FunMapGiveSchema.of(table.getSchema().getFieldNames(),properties.getProperty(ParameterName.SOURCE_PRIMARY_KEY)));
            } else {
                arr_db.add(dbTableEnv.toAppendStream(table, Row.class, uid)
                        .process(FunMapGiveSchema.of(table.getSchema().getFieldNames(),properties.getProperty(ParameterName.SOURCE_PRIMARY_KEY))));
            }
        }
        for (DataStream<Tuple2<String, String>> dataStream : arr_db) {
            db_init = db_init.union(dataStream);
        }
        return Tuple2.of(db_init, fieldAndType);
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
                if (!fieldName.equals(properties.getProperty(ParameterName.SOURCE_PRIMARY_KEY))) {
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
        }
        return s;
    }


    /**
     * Indicators to merge
     * @param db_init
     * @return
     */
    public SingleOutputStreamOperator<String> mergerIndicators(DataStream<Tuple2<String, String>> db_init, Integer fieldOutNum, String uid, String watermark) {
        SingleOutputStreamOperator<String> keyedProcess_indicators_merge = db_init
                .keyBy(value -> value.f0)
                .process(FunKeyedProValCon.of(fieldOutNum, watermark)).uid(uid);
        return keyedProcess_indicators_merge;
    }
}
