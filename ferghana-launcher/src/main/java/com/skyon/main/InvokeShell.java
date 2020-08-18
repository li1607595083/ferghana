package com.skyon.main;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class InvokeShell {
    private static final Logger LOG = LoggerFactory.getLogger(JobSubmit.class);

    /**
     * 启动作业
     *
     * @param sourceTableId 通常情况下就一个数据源表
     * @param sinkTableIds  可以有多个数据结果表，多个数据结果表用逗号拼接
     * @param sqls          可以有多个sql,多个sql用分号拼接
     * @param jobName       job名称
     * @param testRun       1代表调试运行，0代表正式运行
     */
    public void jobSubmit(String sourceTableId, String sinkTableIds, String sqls, String jobName, String testRun) {
        //第一个变量是sh命令，第二个变量是需要执行的脚本路径，从第三个变量开始是我们要传到脚本里的参数。
        String[] path = new String[]{"sh", "/Users/liqiang/Install/upwisdom/bin/job_submit.sh", sourceTableId, sinkTableIds, sqls, jobName, "0"};
        try {
            Runtime runtime = Runtime.getRuntime();
            Process pro = runtime.exec(path);
            int status = pro.waitFor();
            if (status != 0) {
                LOG.error("作业启动失败!");
                System.exit(0);
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            StringBuffer strbr = new StringBuffer();
            String line;
            String jobId;
            String applicationId;
            while ((line = br.readLine()) != null) {
                strbr.append(line).append("\n");
                if (line.contains("YARN application has been deployed successfully")) {
                    if (line.contains("Found Web Interface")) {
                        String[] words = line.split(" ");
                        applicationId = words[words.length - 1];
                        LOG.info("applicationId为：" + applicationId);
                        System.out.println("applicationId为：" + applicationId);
                    }
                    if (line.contains("Job has been submitted with JobID")) {
                        String[] words = line.split(" ");
                        jobId = words[words.length - 1];
                        LOG.info("作业启动成功！JobID为：" + jobId);
                        System.out.println("作业启动成功！JobID为：" + jobId);
                    }
                }
            }
        } catch (IOException | InterruptedException ec) {
            ec.printStackTrace();
        }
    }

    /**
     * 停止作业
     *
     * @param applicationId
     */
    public void jobKill(String applicationId) {
        String[] path = new String[]{"sh", "/Users/liqiang/Install/upwisdom/bin/job_kill.sh", applicationId};
        try {
            Runtime runtime = Runtime.getRuntime();
            Process pro = runtime.exec(path);
            int status = pro.waitFor();
            System.out.println(applicationId);
            if (status != 0) {
                LOG.error("作业停止失败!");
                System.out.println("作业停止失败!");
                System.exit(0);
            } else {
                LOG.info("作业停止成功!");
                System.out.println("作业停止成功!");
            }
        } catch (IOException | InterruptedException ec) {
            ec.printStackTrace();
        }
    }

    /**
     * @param sourceTableId 通常情况下就一个数据源表
     * @param sinkTableIds  可以有多个数据结果表，多个数据结果表用逗号拼接
     * @param sqls          可以有多个sql,多个sql用分号拼接
     * @param jobName       job名称
     * @param testRun       1代表调试运行，0代表正式运行
     * @param param         调试输入参数，多条参数用逗号拼接
     */
    public String testRun(String sourceTableId, String sinkTableIds, String sqls, String jobName, String testRun, String param) {
        //第一个变量是sh命令，第二个变量是需要执行的脚本路径，从第三个变量开始是我们要传到脚本里的参数。
        String[] path = new String[]{"sh", "/Users/liqiang/Install/upwisdom/bin/job_submit.sh", sourceTableId, sinkTableIds, sqls, jobName, "1"};
        try {
            Runtime runtime = Runtime.getRuntime();
            Process pro = runtime.exec(path);
            int status = pro.waitFor();
            if (status != 0) {
                LOG.error("作业启动失败!");
                System.exit(0);
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            StringBuffer strbr = new StringBuffer();
            String line;
            String jobId;
            String applicationId;
            while ((line = br.readLine()) != null) {
                strbr.append(line).append("\n");
                if (line.contains("YARN application has been deployed successfully")) {
                    if (line.contains("Found Web Interface")) {
                        String[] words = line.split(" ");
                        applicationId = words[words.length - 1];
                        LOG.info("applicationId为：" + applicationId);
                    }
                    if (line.contains("Job has been submitted with JobID")) {
                        String[] words = line.split(" ");
                        jobId = words[words.length - 1];
                        LOG.info("作业启动成功！JobID为：" + jobId);
                        kafkaMessageSend("", "", "topicName", param);
                        return kafkaMessageGet("", "", "topicName");
                    }
                } else {
                    return strbr.toString();
                }
            }
        } catch (IOException | InterruptedException ec) {
            ec.printStackTrace();
        }
        return null;
    }

    public void kafkaMessageSend(String zookeeperAddress, String kafkaAddress, String topicName, String param) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaAddress);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String[] split = param.split(",");
        for (String message : split) {
            producer.send(new ProducerRecord<>(topicName, null, message));
        }
        producer.close();
    }

    public String kafkaMessageGet(String zookeeperAddress, String kafkaAddress, String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaAddress);
        props.put("group.id", String.valueOf(System.currentTimeMillis()));
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        StringBuffer strbr = new StringBuffer();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (records.count() == 0) {
                strbr.substring(0, strbr.length() - 1);
                return strbr.toString();
            } else {
                for (ConsumerRecord<String, String> record : records) {
                    strbr.append(record.value()).append(",");
                }

            }
        }
    }
}
