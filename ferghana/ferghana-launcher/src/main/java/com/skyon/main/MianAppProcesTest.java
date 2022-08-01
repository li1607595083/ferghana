package com.skyon.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.SpeedConfig;
import com.dtstack.flinkx.kafka.writer.KafkaWriter;
import com.dtstack.flinkx.oraclelogminer.reader.OraclelogminerReader;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.writer.BaseDataWriter;
import com.skyon.app.AppPerFormOperations;
import com.skyon.app.AppRegisFunction;
import com.skyon.bean.ParameterName;
import com.skyon.bean.RunMode;
import com.skyon.bean.SourceType;
import com.skyon.utils.FlinkUtils;
import com.skyon.utils.ParameterUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static com.skyon.utils.ParameterUtils.parseStrToProperties;

/**
 * Main Application
 */
public class MianAppProcesTest {

    public static void main(String[] args) throws Exception {
        // 对加密(Base64)参数进行解密，应用程序运行时所需要的配置参数都放在这里面
        byte[] decoded = Base64.getDecoder().decode(args[0]);
        // 直接转换为字符串,转换后为JSON格式
        String meta = new String(decoded);
        if (meta.startsWith("{\"sinkSql\"")) {
            JSONObject jsonObject = JSON.parseObject(meta);
            JSONObject job = (JSONObject) jsonObject.get("sinkSql");
            runJob(job.toString(), null);
        } else {
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
            AppPerFormOperations appOneStreamOver = AppPerFormOperations.of(properties, dbTableEnv, dbEnv);
            // 创建数据源表，包括单个数据源(kafka,mysql,oracle),双流join(两个topic)
            appOneStreamOver.createSource();
            // 双流 join 将两个数据流连接起来
            appOneStreamOver.twoStreamConcat();
            // 执行数据维维表创建语句
            appOneStreamOver.createDimTabl();
            // 指定数据原表与维表拼接语句，使用的是left join, 并将查询结果组成成一张表
            appOneStreamOver.soureAndDimConcat();
            // 用于计算部分
            if (properties.getProperty(ParameterName.SOURCE_TYPE).equals(SourceType.ONE_STREAM) || properties.getProperty(ParameterName.SOURCE_TYPE).equals(SourceType.TWO_STREAM_JOIN)) {
                Tuple2<SingleOutputStreamOperator<String>, Map<String, String>> result = appOneStreamOver.variableExec();
                // 创建测试使用的topic, 用于存储计算结果，以便前端读取
                appOneStreamOver.testMOde(result);
                // 非测试模式
                appOneStreamOver.runMode(result);
            } else if (properties.getProperty(ParameterName.SOURCE_TYPE).equals(SourceType.MYSQL_CDC)){
                appOneStreamOver.cdcMySqlAsyncResult();
            }
            if (RunMode.TEST_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
                dbEnv.execute("test");
            }
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
        // 读取配置文件的配置参数
        ParameterUtils.readerConfigurationParameters(arg2, properties);
        // 参数进一步处理
        ParameterUtils.parameterDeal(properties);
        // 返回参数配置文件
        return properties;
    }

    public static JobExecutionResult runJob(String job,String savepointPath) throws Exception{
        DataTransferConfig config = DataTransferConfig.parse(job);
        Configuration conf = new Configuration();
//        conf.setString("akka.ask.timeout", "180 s");
//        conf.setString("web.timeout", String.valueOf(100000));

//        MyLocalStreamEnvironment env = new MyLocalStreamEnvironment(conf);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        HashMap<String, String> stringStringHashMap = new HashMap<>();
        ParameterTool parameterTool = ParameterTool.fromMap(stringStringHashMap);
        env.getConfig().setGlobalJobParameters(parameterTool);

        env.setParallelism(config.getJob().getSetting().getSpeed().getChannel());

        if (needRestart(config)) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                    10,
                    Time.of(10, TimeUnit.SECONDS)
            ));
        }
        SpeedConfig speedConfig = config.getJob().getSetting().getSpeed();
        BaseDataReader reader = new OraclelogminerReader(config, env);
        DataStream<Row> dataStream = reader.readData();
        if(speedConfig.getReaderChannel() > 0){
            dataStream = ((DataStreamSource<Row>) dataStream).setParallelism(speedConfig.getReaderChannel());
        }

        if (speedConfig.isRebalance()) {
            dataStream = dataStream.rebalance();
        }

        BaseDataWriter dataWriter = new KafkaWriter(config);
        DataStreamSink<?> dataStreamSink = dataWriter.writeData(dataStream);
        if(speedConfig.getWriterChannel() > 0){
            dataStreamSink.setParallelism(speedConfig.getWriterChannel());
        }

//        if(StringUtils.isNotEmpty(savepointPath)){
//            env.setSettings(SavepointRestoreSettings.forPath(savepointPath));
//        }

        return env.execute();
    }

    private static boolean needRestart(DataTransferConfig config){
        return config.getJob().getSetting().getRestoreConfig().isRestore();
    }

}