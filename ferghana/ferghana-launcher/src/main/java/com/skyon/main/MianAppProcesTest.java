package com.skyon.main;

import com.skyon.app.AppPerFormOperations;
import com.skyon.app.AppRegisFunction;
import com.skyon.bean.ParameterName;
import com.skyon.bean.RunMode;
import com.skyon.bean.SourceType;
import com.skyon.utils.FlinkUtils;
import com.skyon.utils.ParameterUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.*;

import static com.skyon.utils.ParameterUtils.parseStrToProperties;

/**
 * Flink 程序主类
 */
public class MianAppProcesTest {

    public static void main(String[] args) throws Exception {
        // 获取参数
        Properties properties = getParameterProperties(args[0], args[1]);
        // 访问Oracle时,如果包含TimeStamp类型字段时，需要在提交作业时设置jvm参数参数
        // System.setProperty("oracle.jdbc.J2EE13Compliant", "true");
        // 获取 Flink datastream 运行环境
        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv(properties);
        // 输出到 kafka 的时候，如果 topic 不存在，则创建一个分区数等于并行度的 topic
        properties.put(ParameterName.KAFKA_PARTITION, dbEnv.getParallelism() + "");
        // 获取 Flink table 运行环境
        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv, properties);
        // 注册条件函数
        AppRegisFunction.registerFunction(dbTableEnv, properties);
        // 设置全局变量
        Map gloabParameter = parseStrToProperties(properties.getProperty(ParameterName.GLOAB_PARAMETER));
        dbEnv.getConfig().setGlobalJobParameters(ParameterTool.fromMap(gloabParameter));
        // 创建一个 AppPerFormOperations 实例
        AppPerFormOperations appOneStreamOver = AppPerFormOperations.of(properties, dbTableEnv);
        // 创建数据源表，包括单个数据源(kafka,mysql,oracle),双流join(两个topic)
        appOneStreamOver.createSource();
        // 执行数据维维表创建语句
        appOneStreamOver.createDimTabl();
        // 指定数据原表与维表拼接语句，使用的是left join, 并将查询结果组成成一张表
        appOneStreamOver.soureAndDimConcat();
        // 双流 join 将两个数据流连接起来
        appOneStreamOver.twoStreamConcat();
        // 用于计算部分
        if (properties.getProperty(ParameterName.SOURCE_TYPE).equals(SourceType.ONE_STREAM) || properties.getProperty(ParameterName.SOURCE_TYPE).equals(SourceType.TWO_STREAM_JOIN)) {
            appOneStreamOver.deVariableExec();
            Tuple2<SingleOutputStreamOperator<String>, LinkedHashMap<String, String>> resutl = appOneStreamOver.variableExec();
            // 创建测试使用的topic, 用于存储计算结果，以便前端读取
            appOneStreamOver.testMOde(resutl);
            // 非测试模式
            appOneStreamOver.runMode(resutl);
        } else if (properties.getProperty(ParameterName.SOURCE_TYPE).equals(SourceType.MYSQL_CDC)){
            appOneStreamOver.cdcMySqlAsyncResult();
        }
        if (properties.getProperty(ParameterName.SINK_SQL) == null && RunMode.TEST_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            dbEnv.execute("test");
        }
    }


    /**
     * @param arg1 加密参数；
     * @param arg2 参数配置文件路径，此配置文件是在提交应用程序的机器上，如果应用程序是从
     *             SavePoint 进行恢复的话，此路径还会拼接上 SavePoint 的路径(@ 拼接符号);
     * @return 解析加密参数和读取配置文件的参数，以 Properties 的形式返回；
     */
    private static Properties getParameterProperties(String arg1, String arg2) throws Exception {
        // 创建一个 Properties 对象，用以存放参数信息
        Properties properties = new Properties();
        // 解析加密参数
        ParameterUtils.parseEncryptionParameter(arg1, properties);
        // 处理配置文件路径
        String configFilePath = ParameterUtils.dealconfigFilePath(arg2, properties);
        // 读取配置文件的配置参数
        ParameterUtils.readerConfigurationParameters(configFilePath, properties);
        // 参数进一步处理
        ParameterUtils.parameterDeal(properties);
        // 返回参数配置文件
        return properties;
    }
}