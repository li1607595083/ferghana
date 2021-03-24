package com.skyon.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skyon.function.*;
import com.skyon.type.TypeTrans;
import com.skyon.utils.KafkaUtils;
import com.skyon.utils.MySqlUtils;
import com.skyon.utils.StoreUtils;
import javafx.scene.control.Tab;
import kafka.Kafka;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import scala.collection.mutable.HashMap$;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;

public class AppDealOperation {

    /*Kafka topic among*/
    public String middle_table;
    /*Program run configuration*/
    private Properties properties;
    /*schema*/
    public LinkedHashMap<String, String> singleFieldTypeHashMap;
    public HashMap<String, String> indexfieldNameAndType;
    public String oracle_table = "";
    public HashSet<String> fieldSet;

    public AppDealOperation() {}

    public AppDealOperation(Properties properties) {
        if ("02".equals(properties.getProperty("runMode"))){
            this.middle_table = "tmp_" + properties.getProperty("variablePackEn");
            // && properties.getProperty("sinkSql") != null
        } else if ("01".equals(properties.getProperty("runMode"))) {
            this.middle_table = properties.getProperty("testTopicName");
        }
        indexfieldNameAndType = new HashMap<>();
        this.properties = properties;
    }

    public void queryRegisterView(StreamTableEnvironment dbTableEnv, String querySql, String name){
        Table table = dbTableEnv.sqlQuery(querySql);
        dbTableEnv.createTemporaryView(name, table);
    }

    /**
     * Create a dimension table
     * @param dbTableEnv
     */
    public void createDimTabl(StreamTableEnvironment dbTableEnv) throws Exception {
        String dimension = properties.getProperty("dimensionTable");
        if (dimension != null){
            int registerCount = 1;
            Object[] objects = JSON.parseArray(dimension).toArray();
            for (Object object : objects) {
                properties.setProperty("registerCount", registerCount + "");
                HashMap hashMap = JSON.parseObject(object.toString(), HashMap.class);
                String dimensionTableSql = hashMap.get("dimensionTableSql").toString();
                Object testDimType = hashMap.get("testDimType");
                String dealDimensionTableSql = getDealDimensionTableSql(dimensionTableSql, testDimType);
                Object testDimdata = hashMap.get("testDimdata");
                if ("02".equals(testDimType.toString())){
                    addDataToJdbc(dealDimensionTableSql, testDimdata);
                } else if ("03".equals(testDimType)){
                    addDataHbase(dealDimensionTableSql, testDimdata);
                }
                dbTableEnv.executeSql(dealDimensionTableSql);
                registerCount++;
            }
        }
    }


    /**
     * Add data to the Hbase dimension table
     * @param dealDimensionTableSql
     * @param testDimdata
     * @return
     * @throws IOException
     */
    private void addDataHbase(String dealDimensionTableSql, Object testDimdata) throws IOException {
        StoreUtils storeUtils = StoreUtils.of(dealDimensionTableSql);
        storeUtils.createHbaseTABLE();
        if (testDimdata != null){
            Object[] testDimdataArr = JSON.parseArray(testDimdata.toString()).toArray();
            org.apache.hadoop.hbase.client.Connection hbaseConnection = storeUtils.hbaseConnection();
            String fieldStr = storeUtils.fieldStr;
            HashMap<String, String> nameAndType = new HashMap<>();
            String fam = fieldStr.split(",", 2)[1].split("PRIMARY", 2)[0].trim();
            fam = fam.substring(0, fam.length() - 1).trim();
            String fenge = TypeTrans.getType(fam.split("\\s+", 2)[1]);
            String type = null;
            Boolean fer = true;
            while (fer){
                String[] strings = fenge.substring(0, type.length() - 1).split("<", 2);
                if (strings.length == 2){
                    type = strings[1];
                } else {
                    type = fenge.substring(0, type.length() - 1).split("\\(", 2)[1];
                }
                boolean flag = true;
                while (flag){
                    String[] split = type.split("\\s+", 2);
                    String key = split[0].replaceAll("`", "");
                    String ty = TypeTrans.getType(split[1]);
                    String values = TypeTrans.getTranKey(ty);
                    nameAndType.put(key, values);
                    String[] sp2 = split[1].replaceFirst(ty, "").split(",", 2);
                    if (sp2.length == 1){
                        flag = false;
                    } else if (sp2.length == 2){
                        type = sp2[1].trim();
                    }
                }
                String[] split = fam.split(fenge, 2);
                if (split.length == 2) {
                    fenge = TypeTrans.getType(split[1].replaceFirst(",", "").trim().split("\\s+", 2)[1]);
                } else if (split.length == 1){
                    fer = false;
                }
            }
            org.apache.hadoop.hbase.client.Table hbaseConnectionTable = hbaseConnection.getTable(TableName.valueOf(getSingMeta(dealDimensionTableSql, "table-name")));
            for (Object s : testDimdataArr) {
                JSONObject jsonObject = JSON.parseObject(s.toString());
                String rowkey = jsonObject.getString("rowkey");
                Put put = new Put(rowkey.getBytes());
                Iterator<Map.Entry<String, Object>> iterator = jsonObject.entrySet().iterator();
                while (iterator.hasNext()){
                    Map.Entry<String, Object> next = iterator.next();
                    String key = next.getKey();
                    String value = (String)next.getValue();
                    if (!key.toLowerCase().equals("rowkey")){
                        String[] famAndFile = key.split("\\.", 2);
                        put.addColumn(famAndFile[0].getBytes(), famAndFile[1].getBytes(), TypeTrans.hbaseByte(nameAndType.get(famAndFile[1]), value));
                    }
                }
                hbaseConnectionTable.put(put);
            }
            if (hbaseConnectionTable != null){
                hbaseConnectionTable.close();
            }
            if (hbaseConnection != null){
                hbaseConnection.close();
            }
        }
    }


    private String getDealDimensionTableSql(String dimensionTableSql, Object testDimType) throws Exception {
        String dealDimensionTableSql = dimensionTableSql;
        if ("01".equals(properties.getProperty("runMode"))){
            String tableNmae = getSingMeta(dimensionTableSql, "table-name");
            if ("02".equals(testDimType.toString())){
                    String url = getSingMeta(dimensionTableSql, "url");
                    String username = getSingMeta(dimensionTableSql, "username");
                    String password = getSingMeta(dimensionTableSql, "password");
                    String driver = getSingMeta(dimensionTableSql, "driver");
                    if (driver.startsWith("oracle.jdbc")){
                        dealDimensionTableSql = dimensionTableSql
                                .replace("'" + tableNmae + "'", "'" + properties.getProperty("testTopicName") + properties.getProperty("registerCount") + "'")
                                .replace("'" + url + "'", "'" + properties.getProperty("testOracleDimensionUrl") + "'")
                                .replace("'" + username + "'", "'" + properties.getProperty("testOracleUserName") + "'")
                                .replace("'" + password + "'", "'" + properties.getProperty("testOraclePassWord") + "'")
                                .replace("'" + driver + "'", "'" + properties.getProperty("testOracleDriver") + "'");
                    } else {
                        dealDimensionTableSql = dimensionTableSql
                                .replace("'" + tableNmae + "'", "'" + properties.getProperty("testTopicName") + properties.getProperty("registerCount") + "'")
                                .replace("'" + url + "'", "'" + properties.getProperty("testDimensionUrl") + "'")
                                .replace("'" + username + "'", "'" + properties.getProperty("testUserName") + "'")
                                .replace("'" + password + "'", "'" + properties.getProperty("testPassWord") + "'")
                                .replace("'" + driver + "'", "'" + properties.getProperty("testDriver") + "'");
                    }

            } else if ("03".equals(testDimType)){
                    String testHbaseTable = "TEST:" + properties.getProperty("testTopicName") + properties.getProperty("registerCount");
                    String zkquorum = getSingMeta(dimensionTableSql, "zookeeper.quorum");
                    dealDimensionTableSql = dimensionTableSql.replace("'" + tableNmae + "'", "'" + testHbaseTable + "'")
                            .replace("'" + zkquorum + "'", "'" + properties.getProperty("testZK") + "'");
            }
        }
        return dealDimensionTableSql;
    }
    /**
     * Add data to the JDBC dimension table
     * @param dealDimensionTableSql
     * @param testDimdata
     * @return
     * @throws Exception
     */
    private void addDataToJdbc(String dealDimensionTableSql, Object testDimdata) throws Exception {
        StoreUtils storeUtils = StoreUtils.of(dealDimensionTableSql);
        String type = "mysql";
        if (getSingMeta(dealDimensionTableSql, "driver").startsWith("oracle.jdbc")){
            type = "oracle";
        }
        storeUtils.createSqlTable(type);
        if (testDimdata != null) {
            Object[] testDimdataArr = JSON.parseArray(testDimdata.toString()).toArray();
            Connection connection = storeUtils.mySqlConnection();
            Statement statement = connection.createStatement();
            connection.setAutoCommit(false);
            ArrayList<String> msVacherFieldNmae = storeUtils.msVarcherFieldNmae;
            for (Object s : testDimdataArr) {
                String fieldName = "";
                String filedValue = "";
                String update = "";
                String pkValue = "";
                Iterator<Map.Entry<String, Object>> iterator = JSON.parseObject(s.toString()).entrySet().iterator();
                while (iterator.hasNext()){
                    Map.Entry<String, Object> next = iterator.next();
                    String key = next.getKey();
                    String value = next.getValue().toString();
                    if (msVacherFieldNmae.contains(key)) {
                        value = "'" + value + "'";
                    }
                    fieldName = fieldName + key + ",";
                    filedValue = filedValue + value + ",";
                    if (!key.toLowerCase().equals(storeUtils.jdbcPk.toLowerCase()) || type.equals("mysql")){
                        update = update + key + "=";
                        update = update + value + ",";
                    } else {
                        pkValue = value;
                    }
                }
                fieldName = fieldName.substring(0, fieldName.length() - 1);
                filedValue = filedValue.substring(0, filedValue.length() - 1);
                update = update.substring(0, update.length() - 1);
                String insertSql = "INSERT INTO " + getSingMeta(dealDimensionTableSql, "table-name") + "(" + fieldName + ")" + " VALUES (" + filedValue + ")" + " ON DUPLICATE KEY UPDATE " + update;
                if ("oracle".equals(type.toLowerCase())){
                    insertSql = "MERGE INTO " + getSingMeta(dealDimensionTableSql, "table-name") + " t USING DUAL "
                            + "ON (t." + storeUtils.jdbcPk + " = " + pkValue + ") WHEN NOT MATCHED THEN INSERT " + fieldName + " "
                            + "VALUES " + filedValue + " "
                            + "WHEN MATCHED THEN UPDATE SET "
                            + update;
                }
                statement.execute(insertSql);
            }
            connection.commit();

            if (statement != null){
                statement.close();
            }
            if (connection != null){
                connection.close();
            }
        }
    }

    public static String getSingMeta(String sql, String name) {
        String sqlDeal = StringUtils.reverse(StringUtils.reverse(sql).split("\\)", 2)[1]);
        return sqlDeal.split("'" + name + "'", 2)[1]
                .replaceFirst("=", "").split("','", 2)[0].replaceAll("'", "").trim();
    }

    private void inputTestSourceData(String topic, String brokeList){
        String testSourcedata = properties.getProperty("testSourcedata");
        JSONArray jsonArray = JSON.parseArray(testSourcedata);
        Object[] dataArr = jsonArray.toArray();
        createTopic(topic, properties.getProperty("testZK"));
        KafkaUtils.kafkaProducer(topic, dataArr, brokeList);
    }

    private void createTopic(String  topic, String zkAddAndPort){
        ZkUtils kafkaZK = KafkaUtils.getZkUtils(zkAddAndPort);
        KafkaUtils.deleteKafkaTopic(kafkaZK,topic);
        KafkaUtils.createKafkaTopic(kafkaZK,topic);
        KafkaUtils.clostZkUtils(kafkaZK);
    }

    /**
     * Create data source
     * @param dbTableEnv
     */
    public void createSource(StreamTableEnvironment dbTableEnv, StreamExecutionEnvironment env) throws Exception {
        String sourceTableSql = properties.getProperty("sourceTableSql").trim();
        String interval = sourceTableSql.toUpperCase().split("INTERVAL", 2)[1].trim().split("\\s+", 2)[0];
        sourceTableSql = sourceTableSql.substring(0, sourceTableSql.length() - 1) + "," + "'json.ignore-parse-errors' = '"+ properties.getProperty("json_ignore_parse_errors") + "'" + ")";
        Map<String, String> globaParm = new HashMap<>();
        globaParm.put("idleTimeout", 5*1000+"");
        globaParm.put("delayTime", Long.parseLong(interval.replaceAll("'", ""))*1000+"");
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(globaParm));
        if ("02".equals(properties.getProperty("runMode"))){
            if (properties.getProperty("savepointPath") != null){
                Connection connection = MySqlUtils.getConnection(properties.getProperty("savepointUrl"), properties.getProperty("testUserName"), properties.getProperty("testPassWord"), properties.getProperty("testDriver"));
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT offset FROM t_offset_record WHERE savepointpath = '" + properties.getProperty("savepointPath") + "'");
                while (resultSet.next()){
                    String offset = resultSet.getString(1);
                    String scanMode = getSingMeta(sourceTableSql, "scan.startup.mode");
                    sourceTableSql.replace("'" + scanMode + "'", "'specific-offsets'");
                    sourceTableSql = sourceTableSql.substring(0, sourceTableSql.length() - 1)
                            + ",'scan.startup.specific-offsets' = '" + offset.toLowerCase() +"')";
                }
                MySqlUtils.closeConnection(connection);
            }
            dbTableEnv.executeSql(sourceTableSql);
        } else if ("01".equals(properties.getProperty("runMode"))){
            String topic =  getSingMeta(sourceTableSql, "topic");
            String test_topic = "source_" + properties.getProperty("testTopicName");
            String testBrokeList = properties.getProperty("testBrokeList");
            inputTestSourceData(test_topic, testBrokeList);
            String scanMode = getSingMeta(sourceTableSql, "scan.startup.mode");
            String brokerList = getSingMeta(sourceTableSql,  "properties.bootstrap.servers");
            String testSourceTableSql = sourceTableSql
                    .replace("'" + topic + "'", "'" + test_topic +"'")
                    .replace("'" + scanMode + "'", "'earliest-offset'")
                    .replace("'" + brokerList + "'", "'" + testBrokeList +"'");
            dbTableEnv.executeSql(testSourceTableSql);
        }
    }

    /**
     * Create an instance for ApplicationOver
     * @param properties
     * @return
     */
    public static AppDealOperation of(Properties properties){
        return new AppDealOperation(properties);
    }

    public static AppDealOperation of(){
        return new AppDealOperation();
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
     * 处理SQL语句，用于UID的设置
     * @param uid
     * @param s
     * @return
     */
    public  static String getUid(String uid, String s) {
        String sql = s.toUpperCase().trim().replaceAll("\\s+", "");
        return uid + sql;
    }


    public SingleOutputStreamOperator<String> resultToString(StreamTableEnvironment dbTableEnv, String sinkSql, String uidPrefix) {
        Table table = dbTableEnv.sqlQuery(sinkSql);
        TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();
        String uid = uidPrefix;
        uid = getUid(uid, sinkSql);
        SingleOutputStreamOperator<String> resut = dbTableEnv.toAppendStream(table, Row.class, uid)
                .map(FunMapValueMoveTypeAndFieldNmae.of(fieldNames));
        return resut;
    }


    /**
     * Execute the SQL and convert it to DataStream and merge it into a single stream while stitching together field names and field types
     * @param dbTableEnv
     * @return DataStream<String>
     */
    public DataStream<Tuple2<String, String>> indexUnion(StreamTableEnvironment dbTableEnv, String sqlSet,String uidPrefix,HashMap<Integer, Boolean> isWhere) {
        DataStream<Tuple2<String, String>> db_init = null;
        int counts = 0;
        singleFieldTypeHashMap = new LinkedHashMap<>();
        fieldSet = new HashSet<>();
        ArrayList<DataStream<Tuple2<String, String>>> arr_db = new ArrayList<>();
        for (String s : sqlArrQuery(sqlSet.split(";"))) {
            s = getOverUnboundedString(dbTableEnv, s);
            Table table = dbTableEnv.sqlQuery(s);
            TableSchema schema = table.getSchema();
            String[] fieldNames = schema.getFieldNames();
            String uid = uidPrefix;
            uid = getUid(uid, s);
            for (String fieldName : fieldNames) {
                Optional<DataType> fieldDataType = schema.getFieldDataType(fieldName);
                String fieldType = TypeTrans.getType(fieldDataType.get().toString());
                singleFieldTypeHashMap.put(fieldName, fieldType);
                indexfieldNameAndType.put(fieldName,fieldType);
            }
            if (counts == 0){
                db_init = dbTableEnv.toAppendStream(table, Row.class, uid)
                        .process(FunMapValueAddTypeAadFieldName.of(fieldNames, singleFieldTypeHashMap,properties.getProperty("sourcePrimaryKey")));
                counts += 1;
            } else {
                arr_db.add(dbTableEnv.toAppendStream(table, Row.class, uid)
                        .process(FunMapValueAddTypeAadFieldName.of(fieldNames, singleFieldTypeHashMap,properties.getProperty("sourcePrimaryKey"))));
            }
            if (isWhere.get(counts) == null){
                for (String fieldName : fieldNames) {
                    fieldSet.add(fieldName);
                }
            }
        }

        for (DataStream<Tuple2<String, String>> dataStream : arr_db) {
            db_init = db_init.union(dataStream);
        }

        return db_init;
    }


    /**
     * Execute the SQL and convert it to DataStream and merge it into a single stream while stitching together field names and field types
     * @param dbTableEnv
     * @return DataStream<String>
     */
    public DataStream<Tuple2<String, String>> sqlQueryAndUnion(StreamTableEnvironment dbTableEnv, String sqlSet, String uidPrefix, HashMap<Integer, Boolean> isWhere) {
        DataStream<Tuple2<String, String>> db_init = null;
        int counts = 0;
        singleFieldTypeHashMap = new LinkedHashMap<>();
        fieldSet = new HashSet<>();
        ArrayList<DataStream<Tuple2<String, String>>> arr_db = new ArrayList<>();
        for (String s : sqlArrQuery(sqlSet.split(";"))) {
            s = getOverUnboundedString(dbTableEnv, s);
            Table table = dbTableEnv.sqlQuery(s);
            TableSchema schema = table.getSchema();
            String[] fieldNames = schema.getFieldNames();
            String uid = uidPrefix;
            uid = getUid(uid, s);
            for (String fieldName : fieldNames) {
                Optional<DataType> fieldDataType = schema.getFieldDataType(fieldName);
                singleFieldTypeHashMap.put(fieldName, TypeTrans.getType(fieldDataType.get().toString()));
            }
            if (counts == 0){
                db_init = dbTableEnv.toAppendStream(table, Row.class, uid)
                        .process(FunMapGiveSchema.of(fieldNames,properties.getProperty("sourcePrimaryKey")));
                counts += 1;
            } else {
                arr_db.add(dbTableEnv.toAppendStream(table, Row.class, uid)
                        .process(FunMapGiveSchema.of(fieldNames,properties.getProperty("sourcePrimaryKey"))));
                counts += 1;
            }
            if (isWhere.get(counts) == null){
                for (String fieldName : fieldNames) {
                    fieldSet.add(fieldName);
                }
            }
        }
        for (DataStream<Tuple2<String, String>> dataStream : arr_db) {
            db_init = db_init.union(dataStream);
        }

        return db_init;
    }

    private String getOverUnboundedString(StreamTableEnvironment dbTableEnv, String s) {
        if (s.contains("@_@")) {
            String[] split = s.split("@_@");
            s = split[0].trim()+ " " + split[3].trim();
            Table table = dbTableEnv.sqlQuery(s);
            TableSchema schema = table.getSchema();
            schema.getFieldDataType(properties.getProperty("sourcePrimaryKey")).get().toString();
            for (String fieldName : schema.getFieldNames()) {
                if (!fieldName.equals(properties.getProperty("sourcePrimaryKey"))) {
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
    public SingleOutputStreamOperator<String> mergerIndicators(DataStream<Tuple2<String, String>> db_init, Integer fieldOutNum, String uid, HashSet<String> fieldSet) {
        SingleOutputStreamOperator<String> keyedProcess_indicators_merge = db_init
                .keyBy(value -> value.f0)
                .process(FunKeyedProValCon.of(fieldOutNum, fieldSet)).uid(uid);
        return keyedProcess_indicators_merge;
    }

}
