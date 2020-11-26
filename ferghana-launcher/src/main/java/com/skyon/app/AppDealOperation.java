package com.skyon.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skyon.function.FunKeyedProValCon;
import com.skyon.function.FunMapGiveSchema;
import com.skyon.type.TypeTrans;
import com.skyon.utils.KafkaUtils;
import com.skyon.utils.StoreUtils;
import kafka.Kafka;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import scala.collection.mutable.HashMap$;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

public class AppDealOperation {

    /*Kafka topic among*/
    public String middle_table;
    /*Schema information for the intermediate Topic*/
//    public String middle_schema = "";
    /*Program run configuration*/
    private Properties properties;
    /*schema*/
//    public HashMap<String, String> fieldTypeHashMap = new HashMap<>();
    public LinkedHashMap<String, String> singleFieldTypeHashMap;

    public AppDealOperation() {}

    public AppDealOperation(Properties properties) {
        if ("02".equals(properties.getProperty("runMode"))){
            this.middle_table = "tmp_" + properties.getProperty("variablePackEn");
        } else if ("01".equals(properties.getProperty("runMode"))) {
            this.middle_table = "test_var_topic";
        }

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
            Object[] objects = JSON.parseArray(dimension).toArray();
            for (Object object : objects) {
                HashMap hashMap = JSON.parseObject(object.toString(), HashMap.class);
                Object dimensionTableSql = hashMap.get("dimensionTableSql");
                Object testDimType = hashMap.get("testDimType");
                Object testDimdata = hashMap.get("testDimdata");
                Object[] testDimdataArr = null;
                if (testDimdata != null){
                    testDimdataArr = JSON.parseArray(testDimdata.toString()).toArray();
                    String tableNmae = getSingMeta(dimensionTableSql.toString(), "table-name");
                    if ("02".equals(testDimType.toString())){
                        dimensionTableSql = addDataToJdbc(dimensionTableSql.toString(), testDimdataArr, tableNmae);
                    } else if ("03".equals(testDimType)){
                        dimensionTableSql = addDataHbase(dimensionTableSql.toString(), testDimdataArr, tableNmae);
                    }
                }
                dbTableEnv.executeSql(dimensionTableSql.toString());
            }
        }
    }

//    /**
//     * Add data to the Redis dimension table
//     * @param testDimdataArr
//     */
//    private void addDataRedis(Object[] testDimdataArr){
//        String redisHostPort = properties.getProperty("redisHostPort");
//        String[] hp = redisHostPort.split(":",2);
//        Jedis jedis = new Jedis(hp[0].trim(), Integer.parseInt(hp[1].trim()), 5000);
//        if (jedis.isConnected()){
//            jedis.connect();
//        }
//        Transaction multi = jedis.multi();
//        for (Object o : testDimdataArr) {
//
//        }
//    }


    /**
     * Add data to the Hbase dimension table
     * @param dimensionTableSql
     * @param testDimdataArr
     * @param tableNmae
     * @return
     * @throws IOException
     */
    private String addDataHbase(String dimensionTableSql, Object[] testDimdataArr, String tableNmae) throws IOException {
        String testHbaseTable = "TEST:" + tableNmae.split(":", 2)[1];
        String zkquorum = getSingMeta(dimensionTableSql, "zookeeper.quorum");
        String testZK = properties.getProperty("testZK");
        String testDimensionTableSql = dimensionTableSql.replace("'" + tableNmae + "'", "'" + testHbaseTable + "'")
                .replace("'" + zkquorum + "'", "'" + testZK + "'");
        StoreUtils storeUtils = StoreUtils.of(testDimensionTableSql);
        org.apache.hadoop.hbase.client.Connection hbaseConnection = storeUtils.createHbaseTABLE();
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
        org.apache.hadoop.hbase.client.Table hbaseConnectionTable = hbaseConnection.getTable(TableName.valueOf(testHbaseTable));
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
        return testDimensionTableSql;
    }

    /**
     * Add data to the JDBC dimension table
     * @param dimensionTableSql
     * @param testDimdataArr
     * @param tableNmae
     * @return
     * @throws Exception
     */
    private String addDataToJdbc(String dimensionTableSql, Object[] testDimdataArr, String tableNmae) throws Exception {
        String url = getSingMeta(dimensionTableSql, "url");
        String username = getSingMeta(dimensionTableSql, "username");
        String password = getSingMeta(dimensionTableSql, "password");
        String driver = getSingMeta(dimensionTableSql, "driver");
        String testDimensionTableSql = dimensionTableSql.replace("'" + tableNmae + "'", "'test_" + tableNmae + "'")
                .replace("'" + url + "'", "'" + properties.getProperty("testDimensionUrl") + "'")
                .replace("'" + username + "'", "'" + properties.getProperty("testUserName") + "'")
                .replace("'" + password + "'", "'" + properties.getProperty("testPassWord") + "'")
                .replace("'" + driver + "'", "'" + properties.getProperty("testDriver") + "'");
        StoreUtils storeUtils = StoreUtils.of(testDimensionTableSql);
        Connection connection = storeUtils.createMySqlTable();
        Statement statement = connection.createStatement();
        connection.setAutoCommit(false);
        ArrayList<String> msVacherFieldNmae = storeUtils.msVarcherFieldNmae;
        for (Object s : testDimdataArr) {
            String fieldName = "";
            String filedValue = "";
            String update = "";
            Iterator<Map.Entry<String, Object>> iterator = JSON.parseObject(s.toString()).entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, Object> next = iterator.next();
                String key = next.getKey();
                String value = next.getValue().toString();
                fieldName = fieldName + key + ",";
                update = update + key + "=";
                if (msVacherFieldNmae.contains(key)) {
                    value = "'" + value + "'";
                }
                filedValue = filedValue + value + ",";
                update = update + value + ",";
            }
            fieldName = fieldName.substring(0, fieldName.length() - 1);
            filedValue = filedValue.substring(0, filedValue.length() - 1);
            update = update.substring(0, update.length() - 1);
            String insertSql = "INSERT INTO test_" + tableNmae + "(" + fieldName + ")" + " VALUES (" + filedValue + ")" + " ON DUPLICATE KEY UPDATE " + update;
            statement.execute(insertSql);
        }
        connection.commit();

        if (statement != null){
            statement.close();
        }
        if (connection != null){
            connection.close();
        }
        return testDimensionTableSql;
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
//        String brokeList = getSingMeta(properties.getProperty("sourceTableSql"), "properties.bootstrap.servers");
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
    public void createSource(StreamTableEnvironment dbTableEnv){
        String sourceTableSql = properties.getProperty("sourceTableSql");
        if ("02".equals(properties.getProperty("runMode"))){
            dbTableEnv.executeSql(sourceTableSql);
        } else if ("01".equals(properties.getProperty("runMode"))){
            String topic =  getSingMeta(sourceTableSql, "topic");
            String test_topic = "test_" + topic;
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
     * Execute the SQL and convert it to DataStream and merge it into a single stream while stitching together field names and field types
     * @param dbTableEnv
     * @return DataStream<String>
     */
    public DataStream<String> sqlQueryAndUnion(StreamTableEnvironment dbTableEnv, String sqlSet) {
        String sourcePrimaryKey;
        if (properties != null){
            sourcePrimaryKey = properties.getProperty("sourcePrimaryKey");
        } else {
            sourcePrimaryKey = "NVL";
        }
        DataStream<String> db_init = null;
        int counts = 0;
        singleFieldTypeHashMap = new LinkedHashMap<>();
        ArrayList<DataStream<String>> arr_db = new ArrayList<>();
        for (String s : sqlArrQuery(sqlSet.split(";"))) {
            Table table = dbTableEnv.sqlQuery(s);
            TableSchema schema = table.getSchema();
            String[] fieldNames = schema.getFieldNames();
            for (String fieldName : fieldNames) {
                Optional<DataType> fieldDataType = schema.getFieldDataType(fieldName);
                // fieldTypeHashMap.put(fieldName, TypeTrans.getType(fieldDataType.get().toString()));
                singleFieldTypeHashMap.put(fieldName, TypeTrans.getType(fieldDataType.get().toString()));
            }
            if (counts == 0){
                db_init = dbTableEnv.toAppendStream(table, Row.class)
                        .map(FunMapGiveSchema.of(fieldNames, sourcePrimaryKey));
                counts += 1;
            } else {
                arr_db.add(dbTableEnv.toAppendStream(table, Row.class)
                        .map(FunMapGiveSchema.of(fieldNames, sourcePrimaryKey)));
            }

        }
        for (DataStream<String> dataStream : arr_db) {
            db_init = db_init.union(dataStream);
        }
//        schemaConcat(singleFieldTypeHashMap);
        return db_init;
    }


//    public void schemaConcat(HashMap<String, String> scheCon) {
//        Iterator<Map.Entry<String, String>> iter = scheCon.entrySet().iterator();
//        while (iter.hasNext()){
//            Map.Entry<String, String> next = iter.next();
//            String key = next.getKey();
//            String value = next.getValue();
//            middle_schema = middle_schema + key + " " + value + ",";
//        }
//        middle_schema = middle_schema.substring(0, middle_schema.length() - 1);
//    }

    /**
     * Indicators to merge
     * @param db_init
     * @return
     */
    public SingleOutputStreamOperator<String> mergerIndicators(DataStream<String> db_init, Integer fieldOutNum) {
        SingleOutputStreamOperator<String> keyedProcess_indicators_merge = db_init.map(x -> {
            String[] sp = x.split("\t", 2);
            String k = sp[0];
            String v = sp[1];
            return Tuple2.of(k, v);
        }).returns(Types.TUPLE(Types.STRING, Types.STRING))
                .keyBy(x -> x.f0)
                .process(FunKeyedProValCon.of(fieldOutNum));
        return keyedProcess_indicators_merge;
    }

//    /**
//     * Create an intermediate table
//     * @param middle_query
//     * @param dbTableEnv
//     */
//    public void createMinddleTable(String middle_query, StreamTableEnvironment dbTableEnv) {
//        String kafkaMeta = ") WITH ("
//                + "'connector' = 'kafka-0.11',"
//                + "'topic' = '" + middle_table + "',"
//                + "'properties.bootstrap.servers' = '" + properties.getProperty("kafkaZK").replace("2181", "9092") + "',"
//                + "'properties.group.id' = 'ts1',"
//                + "'format' = 'json')";
//        String createMinddleTableSql = "CREATE TABLE " + middle_table + "(" + middle_query + kafkaMeta;
//        System.out.println(createMinddleTableSql);
//        dbTableEnv.executeSql(createMinddleTableSql);
//    }

}
