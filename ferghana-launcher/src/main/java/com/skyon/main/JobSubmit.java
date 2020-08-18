package com.skyon.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.skyon.domain.TDataResultSource;
import com.skyon.domain.TDataSource;
import com.skyon.domain.TDimensionTable;
import com.skyon.domain.TSelfFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class JobSubmit {
    private static final Logger LOG = LoggerFactory.getLogger(JobSubmit.class);

    public static void main(String[] args) throws Exception {
        //shell脚本传过来的5个参数
        String sourceTableId = args[0];
        String dimensionTableId = args[1];
        String sinkTableIds = args[2];
        String sqls = args[3].replaceAll("@_@", " ");
        String jobName = args[4];
        String testRun = args[5];

        // 根据 mybatis-config.xml 配置的信息得到 sqlSessionFactory
        String resource = "mybatis/mybatis-config.xml";
        InputStream inputStream = null;
        try {
            inputStream = Resources.getResourceAsStream(resource);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        // 然后根据 sqlSessionFactory 得到 session
        SqlSession session = sqlSessionFactory.openSession();

        //flink运行环境设置
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);


        //-----------------------------------------CreateSourceTable Start-----------------------------------------
        TDataSource sourceTable = session.selectOne("selectTDataSourceById", Long.valueOf(sourceTableId));
        String sourceTableSchema = schemaTransform(sourceTable.getSchemaDefine());

        //01代表kafka连接器
        if ("01".equals(sourceTable.getConnectorType())) {
            String consumerMode;
            String topicName;
            if ("1".equals(testRun)) {
                topicName = "upwisdom_test_" + sourceTable.getTopicName();
                consumerMode = "earliest-offset";
            } else {
                if ("01".equals(sourceTable.getConsumerMode())) {
                    consumerMode = "latest-offset";
                } else {
                    consumerMode = "earliest-offset";
                }
                topicName = sourceTable.getTopicName();
            }
            String createSourceTableSql = "CREATE TABLE " + sourceTable.getTableName() + "(" + sourceTableSchema + ") WITH ('connector.type' = 'kafka','connector.version' = '0.11','connector.topic' = '" + topicName + "','connector.properties.zookeeper.connect' = '" + sourceTable.getZookeeperAddress() + "','connector.properties.bootstrap.servers' = '" + sourceTable.getKafkaAddress() + "','connector.properties.group.id' = '" + sourceTable.getConsumerGroup() + "','connector.startup-mode' = '" + consumerMode + "','format.type' = 'json')";
            LOG.info("createSourceTableSql:" + createSourceTableSql);
            bsTableEnv.executeSql(createSourceTableSql);
            LOG.info("success");
        }
        //-----------------------------------------CreateSourceTable End-----------------------------------------

        //-----------------------------------------CreateDimensionTable Start-----------------------------------------
        TDimensionTable dimensionTable = session.selectOne("selectTDimensionTableById", Long.valueOf(dimensionTableId));
        String dimensionTableSchema = schemaTransform(dimensionTable.getSchemaDefine());


        //02代表jdbc连接器
        String createDimensionTableSql = null;
        if ("02".equals(dimensionTable.getConnectorType())) {
            createDimensionTableSql = "CREATE TABLE " + dimensionTable.getDimensionName() + "(" + dimensionTableSchema + ") WITH ('connector.type' = 'jdbc','connector.url' = '" + dimensionTable.getJdbcUrlAddress() + "','connector.table' = '" + dimensionTable.getDimensionName() + "','connector.driver' = '" + dimensionTable.getJdbcDrive() + "','connector.username' = '" + dimensionTable.getJdbcUserName() + "','connector.password' = '" + dimensionTable.getJdbcUserPwd() + "','connector.write.flush.max-rows' = '5000','connector.write.flush.interval' = '2s')";
        } else if ("03".equals(dimensionTable.getConnectorType())) {
            String hbaseSchemaDefine = schemaTransform(dimensionTable.getHbaseSchemaDefine());
            createDimensionTableSql = "CREATE TABLE " + dimensionTable.getDimensionName() + "(" + hbaseSchemaDefine + ") WITH ('connector' = 'hbase-1.4','table-name' = '" + dimensionTable.getHbaseTableName() + "','zookeeper.quorum' = '" + dimensionTable.getZookeeperAddress() + "')";
        }
        bsTableEnv.executeSql(createDimensionTableSql);
        LOG.info("createDimensionTableSql:" + createDimensionTableSql);
        //-----------------------------------------CreateDimensionTable End-----------------------------------------

        //-----------------------------------------CreateSinkTable Start-----------------------------------------
        String[] sinkTableIdSplits = sinkTableIds.split(",");
        for (String sinkTableIdSplit : sinkTableIdSplits) {
            TDataResultSource sinkTable = session.selectOne("selectTDataResultSourceById", Long.valueOf(sinkTableIdSplit));
            String schemaDefine = schemaTransform(sinkTable.getSchemaDefine());
            String topicName;
            if ("1".equals(testRun)) {
                topicName = "upwisdom_test_" + sinkTable.getTopicName();
            } else {
                topicName = sinkTable.getTopicName();
            }
            if ("01".equals(sinkTable.getConnectorType())) {
                String createSinkTableSql = "CREATE TABLE " + sinkTable.getTableName() + "(" + schemaDefine + ") WITH ('connector.type' = 'kafka','connector.version' = '0.11','connector.topic' = '" + topicName + "','connector.properties.zookeeper.connect' = '" + sinkTable.getZookeeperAddress() + "','connector.properties.bootstrap.servers' = '" + sinkTable.getKafkaAddress() + "','connector.sink-partitioner' = 'round-robin','format.type' = 'json')";
                bsTableEnv.executeSql(createSinkTableSql);
                LOG.info("createSinkTableSql:" + createSinkTableSql);
            }
        }
        //-----------------------------------------CreateSinkTable End-----------------------------------------

        //-----------------------------------------Execute SQL start-----------------------------------------
        List<TSelfFunction> selectTSelfFunctionList = session.selectList("selectTSelfFunctionList", new TSelfFunction());
        for (TSelfFunction tSelfFunction : selectTSelfFunctionList) {
            bsTableEnv.executeSql("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS " + tSelfFunction.getFunctionName() + " AS '" + tSelfFunction.getFunctionPackagePath() + "' LANGUAGE JAVA");
        }
        String[] sqlKeyWords = {"insert", "update", "delete", "create", "alter", "drop", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP"};
        String[] sqlSplits = sqls.split(";");
        for (int i = 0; i < sqlSplits.length; i++) {
            String sql = sqlSplits[i];
            boolean isSelect = true;
            for (int j = 0; j < sqlKeyWords.length; j++) {
                if (sql.contains(sqlKeyWords[j])) {
                    isSelect = false;
                    break;
                }
            }
            LOG.info("sql:" + sql);
            if (isSelect) {
                bsTableEnv.sqlQuery(sql);
            } else {
                bsTableEnv.executeSql(sql);
            }
        }
        //-----------------------------------------Execute SQL end-----------------------------------------

//        bsEnv.execute(jobName);

    }

    public static String schemaTransform(String schemaDefine) {
        schemaDefine = StringUtils.strip(schemaDefine, "[");
        schemaDefine = StringUtils.strip(schemaDefine, "]");

        StringBuilder sb = new StringBuilder();
        String[] split = schemaDefine.split(",");
        for (int i = 0; i < split.length; i++) {
            JSONObject jsonObj = JSON.parseObject(split[i]);
            for (Map.Entry<String, Object> entry : jsonObj.entrySet()) {
                sb.append(entry.getKey()).append(" ").append(entry.getValue()).append(",");
            }
        }
        return sb.substring(0, sb.length() - 1);
    }
}
