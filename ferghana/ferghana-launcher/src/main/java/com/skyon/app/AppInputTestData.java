package com.skyon.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.skyon.bean.*;
import com.skyon.type.TypeTrans;
import com.skyon.utils.KafkaUtils;
import com.skyon.utils.MySqlUtils;
import com.skyon.utils.RedisUtils;
import com.skyon.utils.StoreUtils;
import kafka.utils.ZkUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

import static com.skyon.utils.ParameterUtils.getSingMeta;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/6/24
 */
public class AppInputTestData {

        public static void inputDataToRedis(String hashname, String host ,String port,String testDimdata){
            Jedis jedis = RedisUtils.getJedis(host, Integer.parseInt(port));
            if (testDimdata != null){
                RedisUtils.addDataStr(jedis, hashname, testDimdata);
            }
            RedisUtils.closeJedis(jedis);
        }

    /**
     * @desc 往 hbase 插入数据
     * @param dealDimensionTableSql
     * @param testDimdata
     * @return
     * @throws IOException
     */
    public static void inputDataToHbase(String dealDimensionTableSql, String testDimdata) throws Exception {
        StoreUtils storeUtils = StoreUtils.of(dealDimensionTableSql);
        // 创建或者更新 habse 表
        storeUtils.createOrUpdateHbaseTABLE();
        if (testDimdata != null){
            Object[] testDimdataArr = JSON.parseArray(testDimdata).toArray();
            org.apache.hadoop.hbase.client.Connection hbaseConnection = storeUtils.hbaseConnection(storeUtils.getMeta());
            String fieldStr = storeUtils.fieldStr;
            HashMap<String, String> nameAndType = new HashMap<>();
            String fam = fieldStr.split(",", 2)[1].split("PRIMARY", 2)[0].trim();
            fam = fam.substring(0, fam.length() - 1).trim();
            String fenge = TypeTrans.getType(fam.split("\\s+", 2)[1]);
            String type = "";
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

    /**
     * @desc 创建测试 topic
     * @param topic
     * @param zkAddAndPort
     */
    private static void createTopic(String  topic, String zkAddAndPort){
        ZkUtils kafkaZK = KafkaUtils.getZkUtils(zkAddAndPort);
        KafkaUtils.deleteKafkaTopic(kafkaZK,topic);
        KafkaUtils.createKafkaTopic(kafkaZK,topic);
        KafkaUtils.clostZkUtils(kafkaZK);
    }


    /**
     * @desc  写入测试数据
     * @param topic
     * @param brokeList
     * @param zk
     * @param data
     */
    private static void inputKafka(String topic, String brokeList,String zk ,String data){
        if (data != null){
            Object[] dataArr = JSON.parseArray(data).toArray();
            createTopic(topic, zk);
            KafkaUtils.kafkaProducer(topic, dataArr, brokeList);
        }
    }

    /**
     * @desc 往kafka 加入测试数据
     * @param properties
     */
    public static void inputDataToKafka(Properties properties) {
        String[] topic = properties.getProperty(ParameterName.TEST_SOUCE_TABLE_NAME).split("\t");
        String brokerList = properties.getProperty(ParameterName.TEST_BROKER_LIST);
        String zk = properties.getProperty(ParameterName.TEST_ZK);
        // 单个 kafka 数据源
        String sourcetype = properties.getProperty(ParameterName.SOURCE_TYPE);
        if (sourcetype.equals(SourceType.ONE_STREAM)) {
            inputKafka(topic[0], brokerList, zk, properties.getProperty(ParameterName.TEST_SOURCE_DATA));
            // 双流 join
        } else if (sourcetype.equals(SourceType.TWO_STREAM_JOIN)) {
            inputKafka(topic[0], brokerList, zk, properties.getProperty(ParameterName.TEST_SOURCE_DATA));
            inputKafka(topic[1], brokerList, zk, properties.getProperty(ParameterName.TEST_TWO_SOURCE_DATA));
        }
    }


    /**
     * @desc 往 jdbc 添加测试数据
     * @param dealDimensionTableSql
     * @param testDimdata
     * @return
     * @throws Exception
     */
    public static void inputDataToJdbc(String dealDimensionTableSql, String testDimdata, boolean isMysql) throws Exception {
        StoreUtils storeUtils = StoreUtils.of(dealDimensionTableSql);
        // uple3.of(字段名和字段类型, 写入数据时需要特殊处理的字段, 主键)
        Tuple3<HashMap<String, String>, ArrayList<String>, String> tableSchema = storeUtils.getSchema(isMysql);
        // 如果表不存在就创建
        storeUtils.createOrUpdateJdbcTable(storeUtils.getMeta().get(ParameterConfigName.TABLE_NAME), Tuple2.of(tableSchema.f0, tableSchema.f2),isMysql, true);
        if (testDimdata != null) {
            Object[] testDimdataArr = JSON.parseArray(testDimdata).toArray();
            Connection connection = storeUtils.jdbcConnection();
            Statement statement = connection.createStatement();
            connection.setAutoCommit(false);
            for (Object s : testDimdataArr) {
                String insertSql = getInsertSql(dealDimensionTableSql, isMysql, tableSchema, s);
                statement.execute(insertSql);
            }
            connection.commit();
            MySqlUtils.colseStatement(statement);
            MySqlUtils.closeConnection(connection);
        }
    }

    @NotNull
    private static String getInsertSql(String dealDimensionTableSql, boolean isMysql, Tuple3<HashMap<String, String>, ArrayList<String>, String> tableSchema, Object s) {
        String fieldName = "";
        String filedValue = "";
        String updateValue = "";
        Iterator<Map.Entry<String, Object>> iterator = JSON.parseObject(s.toString()).entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            String key = next.getKey();
            String value = next.getValue().toString();
            if (tableSchema.f1.contains(key)) {
                value = "'" + value + "'";
            }
            fieldName =  fieldName + key + ",";
            filedValue = filedValue + value + ",";
//            if (!(key.equals(tableSchema.f2)) || isMysql){
//                updateValue = updateValue + key + "=";
//                updateValue = updateValue + value + ",";
//            }
        }
        fieldName = fieldName.substring(0, fieldName.length() - 1);
        filedValue = filedValue.substring(0, filedValue.length() - 1);
//        updateValue = updateValue.substring(0, updateValue.length() - 1);
        String insertSql = "INSERT INTO " + getSingMeta(dealDimensionTableSql, ParameterConfigName.TABLE_NAME)
                + "(" + fieldName + ")" + " VALUES (" + filedValue + ")";
//                + " ON DUPLICATE KEY UPDATE " + updateValue;
//        if (!isMysql){
//            insertSql = "MERGE INTO " + getSingMeta(dealDimensionTableSql, ParameterConfigName.TABLE_NAME) + " A USING "
//                    + "(SELECT COUNT(1) CT FROM " + getSingMeta(dealDimensionTableSql, ParameterConfigName.TABLE_NAME) + " GROUP BY " + tableSchema.f2 +") B "
//                    + "ON ( CT > 0) "
//                    + "WHEN MATCHED THEN UPDATE SET "
//                    + updateValue
//                    + "WHEN NOT MATCHED THEN INSERT (" + fieldName + ") "
//                    + "VALUES (" + filedValue + ") ";
//        }
        System.out.println(insertSql);
        return insertSql;
    }

}
