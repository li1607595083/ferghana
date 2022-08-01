package com.skyon.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.skyon.app.AppPerFormOperations;
import com.skyon.utils.MySqlUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/1/9
 */
public class MainAppReadSavepoint {

    public static void main(String[] args) throws Exception {

        // 获取加密SQL语句
        String uidSet = getUidSet(args[0]);
        // 读取MySQL的连接参数
        Properties mySQLProperties = getMySQLProperties(args[1]);
        // 构建运行环境
        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        bEnv.setParallelism(1);
        // 读取savepoint文件
        ExistingSavepoint savepoint = Savepoint.load(bEnv, args[2], new MemoryStateBackend());
        // 读取偏移量信息
        HashMap<String, Integer> stringStringHashMap = getOffset(uidSet, savepoint);
        // 偏移量拼接
        Map<String, String> partitionAndOffset = offsetContact(stringStringHashMap);
        // 偏移量输出到MySQL中
        outputOffsetToMySQL(args[2], mySQLProperties, partitionAndOffset);

    }

    private static Map<String, String> offsetContact(HashMap<String, Integer> stringStringHashMap) {
        Map<String, String> topicAndConsumerRecord = new HashMap<>();
        Iterator<Map.Entry<String, Integer>> iterator = stringStringHashMap.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Integer> next = iterator.next();
            String topicAndPartition = next.getKey();
            Integer offsets = next.getValue();
            String[] split = topicAndPartition.split("\t");
            String topicName = split[0];
            String partitionNum = split[1];
            if (topicAndConsumerRecord.get(topicName) == null){
                topicAndConsumerRecord.put(topicName, "partition:" + partitionNum + ",offset:" + (offsets + 1) + ";");
            } else {
                topicAndConsumerRecord.put(topicName, topicAndConsumerRecord.get(topicName) + "partition:" + partitionNum + ",offset:" + (offsets + 1) + ";");
            }
        }
        if (topicAndConsumerRecord.size() > 0){
            Iterator<Map.Entry<String, String>> resultDeal = topicAndConsumerRecord.entrySet().iterator();
            while (resultDeal.hasNext()){
                Map.Entry<String, String> kv = resultDeal.next();
                String topicName = kv.getKey();
                String consumerRecord = kv.getValue();
                topicAndConsumerRecord.put(topicName, consumerRecord.substring(0, consumerRecord.length() - 1));
            }
            return topicAndConsumerRecord;
        } else {
            return topicAndConsumerRecord;
        }

    }

    private static HashMap<String, Integer> getOffset(String uidSet, ExistingSavepoint savepoint) throws Exception {
        HashMap<String, Integer> stringStringHashMap = new HashMap<>();
        DataSet<String> initDataSet = null;
        int sourceCounts = 2;
        for (String uid : uidSet.split(";")) {
            for (int i = 0; i < sourceCounts; i++) {
                try {
                    DataSet<String> dataSet  = savepoint.readListState(
                            "TableSourceScan" + "-" + sourceCounts + "-" + uid,
                            "topic_partition_offset_state_copy",
                            Types.STRING);
                    if (initDataSet == null){
                        initDataSet = dataSet;
                    } else {
                        initDataSet = initDataSet.union(dataSet);
                    }

                } catch (Exception e){
                    String message = e.getMessage();
                    if (!message.startsWith("Savepoint does not contain state with operator uid TableSourceScan-")){
                        throw e;
                    }
                }
            }

        }
        List<String> collect_1 = initDataSet.collect();
        for (String s : collect_1) {
            String[] topicDesc = s.split("\t");
            String topic = topicDesc[0];
            String partition = topicDesc[1];
            Integer offset = Integer.parseInt(topicDesc[2]);
            Integer oldOffset = stringStringHashMap.get(topic + "\t" + partition);
            if (oldOffset == null){
                stringStringHashMap.put(topic + "\t" + partition, offset);
            } else if (offset > oldOffset){
                stringStringHashMap.put(topic + "\t" + partition, offset);
            }
        }
        return stringStringHashMap;
    }

    private static void outputOffsetToMySQL(String savepointPath, Properties mySQLProperties, Map<String, String> topicAndConsumerRecord) throws Exception {
        if (topicAndConsumerRecord.size() > 0){
            Connection connection = MySqlUtils.getConnection(mySQLProperties.getProperty("savepointUrl"), mySQLProperties.getProperty("testUserName"), mySQLProperties.getProperty("testPassWord"), mySQLProperties.getProperty("testDriver"));
            PreparedStatement preparedStatement = connection.prepareStatement("INSERT  INTO t_offset_record(savepointpath, offset) VALUES (?, ?) ON DUPLICATE KEY UPDATE  savepointpath=?,  offset=?");
            Iterator<Map.Entry<String, String>> iterator = topicAndConsumerRecord.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, String> kv = iterator.next();
                String topicName = kv.getKey();
                String consumerRecord = kv.getValue();
                preparedStatement.setString(1, savepointPath + "_" + topicName);
                preparedStatement.setString(2, consumerRecord);
                preparedStatement.setString(3, savepointPath + "_" + topicName);
                preparedStatement.setString(4, consumerRecord);
            }
            preparedStatement.execute();
            MySqlUtils.closeConnection(connection);
        }
    }

    private static Properties getMySQLProperties(String arg) throws IOException {
        Properties properties = new Properties();
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(arg);
        Properties parameterToolProperties = parameterTool.getProperties();
        Set<Object> keySet = parameterToolProperties.keySet();
        for (Object key : keySet) {
            Object values = parameterToolProperties.get(key);
            properties.put(key.toString(), values.toString());
        }
        return properties;
    }


    private static String getUidSet(String str) throws IOException {
        // 对加密参数进行解密
        byte[] decoded = Base64.getDecoder().decode(str);
        // 直接转换为字符串,转换后为JSON格式
        String meta = new String(decoded);
        JSONObject jsonObject = JSON.parseObject(meta);
        Iterator<Map.Entry<String, Object>> iterator = jsonObject.entrySet().iterator();
        String uidSet = "";
        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            String k = next.getKey().trim();
            if (k.equals("variableSqls") || k.equals("originalVariableSql")){
                for (String sql_1 : next.getValue().toString().split(";")) {
                    uidSet = uidSet + AppPerFormOperations.getUid("deSqlSet",sql_1) + ";";
                }
            } else if (k.equals("deVariableSqls")){
                for (String sql_2 : next.getValue().toString().split("[|]")) {
                    for (String sql_3 : sql_2.split("@")[0].split(";")) {
                        uidSet = uidSet + AppPerFormOperations.getUid("deVariableSqls", sql_3) + ";";
                    }
                }

            }
        }
        return uidSet;
    }

}

