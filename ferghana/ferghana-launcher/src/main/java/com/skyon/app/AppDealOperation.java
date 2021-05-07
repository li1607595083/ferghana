package com.skyon.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skyon.bean.WaterMarkGeneratorCounuser;
import com.skyon.function.*;
import com.skyon.main.MianAppProcesTest;
import com.skyon.type.TypeTrans;
import com.skyon.utils.KafkaUtils;
import com.skyon.utils.MySqlUtils;
import com.skyon.utils.StoreUtils;
import com.skyon.watermark.GeneratorWatermarkStrategy;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import static org.apache.flink.table.api.Expressions.$;

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
    public String source_tablename = "" ;
    public Boolean esflag = false;
    public long watermark = 0;


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
        String joinSql = properties.getProperty("joinSql");
        List<Map<String, String>> esFields = new ArrayList<>(); //需要返回的所有es字段，key是字段名称，value是字段类型
        List<Map<String, String>> hosts = new ArrayList<>();//多个es的连接信息
        List<Map<String, String>> namess = new ArrayList<>();//需要查询字段的相互匹配名称,key是源表字段名，value是匹配查询的es字段名
        List<Map<Integer, String>> searchValues = new ArrayList<>(); //key 是源表字段名的下标 ，value是匹配查询的es字段名
        List<String[]> returnnames = new ArrayList<>();//每个es需要拼接的所有字段
        int esNum;//es维表的个数
        int length = 0;//源表和es拼接后字段的个数
        String esMiddle = source_tablename + "tmp_es" ;
        String[] fieldNames = null;//源表的字段名称
        DataType[] types = null;//源表的字段类型
        String[] ess = null;//es和源表的关联关系
        int z = 0;//第几个es 和源表的关联关系
        int k = 0;//事件时间的下标
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
                } else if ("04".equals(testDimType) || "05".equals(testDimType)) {//04 指es_6.X;05 指es_7.X;
                    z++;
                    if (!esflag) {//当有多个es维表时，只需要处理一次
                        //处理掉joinSql中关于es部分，以便可以和其它类型维表正确join
                        String[] tests = joinSql.split("`");
                        tests[2] = tests[2].replace(source_tablename, esMiddle);
                        ess = tests[2].split(";");

                        tests[2] = ess[0];
                        String[] esnames = tests[1].split("_join_");
                        esNum = ess.length - 1;
                        for (int j = 0; j < esNum; j++) {//ess.length - 1 是因为ess[0]是join其它维表的句子
                            String name = esnames[esnames.length - 1 - j];
                            tests[2] = tests[2].replace("," + name + ".*", "");
                            tests[1] = tests[1].replace("_join_" + name, "");
                        }
                        joinSql = tests[0] + "`" + tests[1] + "`" + tests[2];
                        properties.setProperty("joinSql", joinSql);
                        Table source = dbTableEnv.from(source_tablename);
                        TableSchema schema = source.getSchema();
                        fieldNames = schema.getFieldNames();
                        types = schema.getFieldDataTypes();
                        length = length + fieldNames.length;
                    }
                    esflag = true;
                    StoreUtils esStore = StoreUtils.of(dimensionTableSql);
                    Map<String, String> host = esStore.getField(esStore.metate.split("\\)")[0].replaceAll("'", ""), "=");
                    hosts.add(host);
                    Map<String, String> esField = esStore.getField(esStore.fieldStr.replaceAll("`", ""), " ");
                    esFields.add(esField);

                    length = length + esField.size();

                    String[] fields = ess[z].split("and");
                    Map<String, String> names = new HashMap<>();//需要查询字段的相互匹配名称,key是源表字段名，value是匹配查询的es字段名
                    for (int i = 0; i < fields.length; i++) {
                        String[] keyValue = fields[i].split("=");
                        names.put(keyValue[0].trim(), keyValue[1].trim());
                    }
                    namess.add(names);

                }
                if (!"04".equals(testDimType)) {
                    dbTableEnv.executeSql(dealDimensionTableSql);
                }
                registerCount++;
            }
            if (esflag) { //执行es拼接
                TypeInformation[] typesInfo = new TypeInformation[length];
                Expression[] expressions = new Expression[length];

                for (int n = 0; n < namess.size(); n++) {
                    Map<String, String> names = namess.get(n);
                    Map<Integer, String> searchValue = new HashMap<>();
                    for (int i = 0; i < fieldNames.length; i++) {//将source中的字段放入
                        if (names.containsKey(fieldNames[i])) {
                            searchValue.put(i, names.get(fieldNames[i]));
                        }
                        if (fieldNames[i].equals("TRADE_TIME")) {
                            expressions[i] = $(fieldNames[i]).rowtime();
                            typesInfo[i] = Types.SQL_TIMESTAMP;
                            k = i;
                        } else {
                            expressions[i] = $(fieldNames[i]);
                            typesInfo[i] = TypeInformation.of(types[i].getConversionClass());
                        }
                    }
                }

                //处理要拼接的所有es字段
                int i = 0, index = fieldNames.length;
                String type = "" ;
                for (Map<String, String> esField : esFields){
                    for (Map.Entry<String, String> entry : esField.entrySet()) {
                        if (i < esField.size()) {
                            index += i;
                            expressions[index] = $(entry.getKey());
                            type = entry.getValue();
                            if (type.contains("STRING")) {
                                typesInfo[index] = Types.STRING;
                            } else if (type.contains("DOUBLE")) {
                                typesInfo[index] = Types.DOUBLE;
                            } else if (type.contains("TIMESTAMP")) {
                                typesInfo[index] = Types.SQL_TIMESTAMP;
                            }
                        }
                        i++;
                    }
                    Set<String> returnfield = esField.keySet();
                    String[] returnname = returnfield.toArray(new String[returnfield.size()]);
                    returnnames.add(returnname);
                }

                Table source = dbTableEnv.from(source_tablename);
                RowTypeInfo rowTypeInfo = new RowTypeInfo(typesInfo);
                DataStream<Row> ds = dbTableEnv.toAppendStream(source, Row.class)//将table转化为append流
//                        .map(new FunMapESjoin(host, searchValue, returnname, k))
//                        .returns(rowTypeInfo)
                        .assignTimestampsAndWatermarks(new GeneratorWatermarkStrategy(k, watermark));//关联es数据

                dbTableEnv.createTemporaryView(esMiddle, ds, expressions);// register the DataStream as View "tablename" with fields  expressions


                Table table = dbTableEnv.from(esMiddle);
                TableSchema sch = table.getSchema();
                String[] fields1 = sch.getFieldNames();
                DataType[] type1 = sch.getFieldDataTypes();
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

    private void inputTestSourceData(String topic, String brokeList, Integer testDataIndex){
        String[] split_testSourcedata = properties.getProperty("testSourcedata").split(";");
        for (int i1 = 0; i1 < split_testSourcedata.length; i1++) {
            String testSourcedata = split_testSourcedata[testDataIndex];
            JSONArray jsonArray = JSON.parseArray(testSourcedata);
            Object[] dataArr = jsonArray.toArray();
            createTopic(topic, properties.getProperty("testZK"));
            KafkaUtils.kafkaProducer(topic, dataArr, brokeList);
        }
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
        Map<String, String> globaParm = new HashMap<>();
        Boolean flag_cdc_mysql = false;
        String registerTableName = null;
        String watermarkFieldAndDealyTime = properties.get("watermark").toString();
        String[] sp_2 = watermarkFieldAndDealyTime.split("[|]");
        String waterMarkFiled = sp_2[0];
        Long watermarkDelayTime = Long.parseLong(sp_2[1]) * 1000;
        globaParm.put("delayTime", watermarkDelayTime + "");
        globaParm.put("idleTimeout", properties.getProperty("idleTimeout", -1 +""));
        String sourceTableSqlSet = properties.getProperty("sourceTableSql").trim();
        source_tablename = sourceTableSqlSet.split(" ", 3)[2].split("\\(", 2)[0].replaceAll("`", "");
        String[] spl_source = sourceTableSqlSet.split(";");
        for (int i = 0; i < spl_source.length; i++) {
            String sourceTableSql = spl_source[i];
            if (sourceTableSql.contains("|")){
                String[] sp = sourceTableSql.split("[|]");
                sourceTableSql = sp[0];
                if (getSingMeta(sourceTableSql,"connector").equals("mysql-cdc")){
                    flag_cdc_mysql = true;
                }
                globaParm.put("CDC_TYPE", sp[1]);
                registerTableName = sourceTableSql.split("\\(", 2)[0].split("\\s+")[2].replaceAll("`","");
                sourceTableSql = sourceTableSql.replaceFirst(registerTableName, "cdc_" + registerTableName);
            }else {
                sourceTableSql = sourceTableSql.substring(0, sourceTableSql.length() - 1) + "," + "'json.ignore-parse-errors' = '"+ properties.getProperty("json_ignore_parse_errors") + "'" + ")";
            }

            if ("02".equals(properties.getProperty("runMode"))){
                if (properties.getProperty("savepointPath") != null && !sourceTableSql.contains("|")){
                    String topic =  getSingMeta(sourceTableSql, "topic");
                    Connection connection = MySqlUtils.getConnection(properties.getProperty("savepointUrl"), properties.getProperty("testUserName"), properties.getProperty("testPassWord"), properties.getProperty("testDriver"));
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT offset FROM t_offset_record WHERE savepointpath = '" + properties.getProperty("savepointPath") + "_" + topic + "'");
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
                String testSourceTableSql = null;
                if (getSingMeta(sourceTableSql, "connector").startsWith("kafka")){
                    String topic =  getSingMeta(sourceTableSql, "topic");
                    String test_topic = "source_" + properties.getProperty("testTopicName") + i;
                    String testBrokeList = properties.getProperty("testBrokeList");
                    // 插入测试数据
                    inputTestSourceData(test_topic, testBrokeList, i);
                    String scanMode = getSingMeta(sourceTableSql, "scan.startup.mode");
                    String brokerList = getSingMeta(sourceTableSql,  "properties.bootstrap.servers");
                    testSourceTableSql = sourceTableSql
                            .replace("'" + topic + "'", "'" + test_topic +"'")
                            .replace("'" + scanMode + "'", "'earliest-offset'")
                            .replace("'" + brokerList + "'", "'" + testBrokeList +"'");
                } else if (getSingMeta(sourceTableSql, "connector").split("[|]")[0].equals("mysql-cdc")){
                        String hostname = getSingMeta(sourceTableSql, "hostname");
                        String port = getSingMeta(sourceTableSql, "port");
                        String username = getSingMeta(sourceTableSql, "username");
                        String password = getSingMeta(sourceTableSql, "password");
                        String database = getSingMeta(sourceTableSql, "database-name");
                        String table = getSingMeta(sourceTableSql, "table-name");
                        String test_table_name =  "source_" + properties.getProperty("testTopicName") + i;
                        testSourceTableSql = sourceTableSql.replace("'" + hostname + "'", "'" + properties.getProperty("testCdcMysqlHostname") + "'")
                                .replace("'" + port + "'",    "'" + properties.getProperty("testCdcMysqlPort") + "'")
                                .replace("'" +username + "'", "'" + properties.getProperty("testCdcMysqlUsername")  + "'")
                                .replace("'" +password + "'", "'" + properties.getProperty("testCdcMysqlPassword") + "'")
                                .replace("'" +database + "'", "'" + properties.getProperty("testCdcMysqlDatabase") + "'")
                                .replace("'" +table + "'",    "'" + test_table_name + "'");
                    String[] fieldAndMeate = testSourceTableSql.replaceFirst("with", "WITH").split("WITH");
                    String fieldName = fieldAndMeate[0].trim();
                    String createTableStr = fieldName.substring(0, fieldName.length() - 1) + ", " + "PRIMARY KEY (" + properties.getProperty("sourcePrimaryKey") + ") NOT ENFORCED" + ")"
                            + " WITH ("
                            + "'url'" + "=" + "'" + properties.getProperty("testCdcMysqlUrl") + "',"
                            + "'table-name'" + "=" + "'" + test_table_name + "',"
                            + "'driver'" + "=" + "'" + properties.getProperty("testCdcMysqlDriver") + "',"
                            + "'username'" + "=" + "'" + properties.getProperty("testCdcMysqlUsername") + "',"
                            + "'password'" + "=" + "'" + properties.getProperty("testCdcMysqlPassword") + "')";
                    StoreUtils.of(createTableStr).createSqlTable("mysql");
                }
                dbTableEnv.executeSql(testSourceTableSql);
             }
        }

        if (properties.getProperty("twoStreamJoinSqls") != null){
            // join的sql语句|注册表名(左右表名以下划线拼接_)|[-3,5](左边必须小于右边,数值必须带有单位符号)
            String twoStreamJoinSqls = properties.getProperty("twoStreamJoinSqls");
            String[] split = twoStreamJoinSqls.split("[|]");
            String[] leftAndRight = split[2].replaceFirst("\\[", "")
                    .replaceFirst("]", "")
                    .split(",", 2);
            int left = Integer.parseInt(leftAndRight[0].trim());
            int right = Integer.parseInt(leftAndRight[1].trim());
            // leftRelativeSize + (leftRelativeSize + rightRelativeSize) / 2 + 1
            int delayTime = ((-left * 1000 + (-left * 1000 + right * 1000) / 2 + 1)+ 1000);
            globaParm.put("TWO_STREAM_JOIN_DELAY_TIME", delayTime + "");
            Table table = dbTableEnv.sqlQuery(split[0]);
            dbTableEnv.createTemporaryView(split[1], table);
        }
        if (flag_cdc_mysql){
            String cdcMysqlQuerySql = "SELECT  * FROM " + "cdc_" + registerTableName;
            LinkedHashMap<String, String> fieldNamdType = new LinkedHashMap<>();
            ArrayList<String> fieldNameArr = new ArrayList<>();
            Table cdcTable = dbTableEnv.sqlQuery(cdcMysqlQuerySql);
            TableSchema schema = cdcTable.getSchema();
            String[] fieldNames = schema.getFieldNames();
            for (String fieldName : fieldNames) {
                Optional<DataType> fieldDataType = schema.getFieldDataType(fieldName);
                String fieldType = TypeTrans.getType(fieldDataType.get().toString());
                fieldNamdType.put(fieldName, fieldType);
                fieldNameArr.add(fieldName);
            }
            fieldNamdType.put("CDC_OP", "STRING");
            SingleOutputStreamOperator<Tuple2<Long, String>> data_deal = dbTableEnv.toRetractStream(cdcTable, Row.class)
                    .map(FunMapCdcAddType.of(fieldNameArr, waterMarkFiled))
                    .filter(value -> value != null);
            if (!checkComputerSql()){
            SingleOutputStreamOperator<String> assignWaterMarkAndTimeStamp;
                if (!waterMarkFiled.equals("proctime")){
                assignWaterMarkAndTimeStamp = data_deal
                        .assignTimestampsAndWatermarks(WatermarkStrategy
                        .forGenerator((WatermarkStrategy<Tuple2<Long, String>>) context -> new WaterMarkGeneratorCounuser(watermarkDelayTime))
                        .withTimestampAssigner((event, timestamp) -> event.f0))
                        .map(value -> value.f1);
            } else {
                assignWaterMarkAndTimeStamp = data_deal.assignTimestampsAndWatermarks(WatermarkStrategy
                        .forGenerator((WatermarkStrategy<Tuple2<Long, String>>) context -> new WaterMarkGeneratorCounuser(watermarkDelayTime))
                        .withTimestampAssigner((event, timestamp) -> timestamp))
                        .map(value -> value.f1);
                }
                MianAppProcesTest.registerTable(dbTableEnv, assignWaterMarkAndTimeStamp, registerTableName, false,fieldNamdType, waterMarkFiled);
            } else {
                MianAppProcesTest.registerTable(dbTableEnv, data_deal.map(value -> value.f1), middle_table, false,fieldNamdType, null);
                // 是不是为CDC_SYNC同步
                properties.setProperty("CDC_SYNC", "TRUE");
            }
        }
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(globaParm));
    }

    public boolean checkComputerSql() {
        return properties.getProperty("originalVariableSql") == null && properties.getProperty("variableSqls") == null && properties.getProperty("deVariableSqls") == null;
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
                counts +=1;
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
                db_init = dbTableEnv.toAppendStream(table, Row.class)
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
