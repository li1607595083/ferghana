package com.skyon.project.system.service.impl;

import com.google.common.base.Strings;
import com.skyon.common.exception.CustomException;
import com.skyon.common.utils.DateUtils;
import com.skyon.project.system.domain.TDataResultSource;
import com.skyon.project.system.domain.TDataSource;
import com.skyon.project.system.domain.TSqlDevelop;
import com.skyon.project.system.mapper.TDataResultSourceMapper;
import com.skyon.project.system.mapper.TDataSourceMapper;
import com.skyon.project.system.mapper.TSqlDevelopMapper;
import com.skyon.project.system.service.ITSqlDevelopService;
import com.skyon.project.system.util.PropertiesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.*;

import static org.apache.flink.optimizer.Optimizer.LOG;

/**
 * SQL任务开发Service业务层处理
 *
 *
 * @date 2020-06-04
 */
@Service
public class TSqlDevelopServiceImpl implements ITSqlDevelopService {

    private static final String JOBSUBMITPATH = "job_submit_path"; // 启动脚本路径
    private static final String JOBSUBMITNAME = "job_submit_name"; // 启动脚本文件名
    private static final String PACKAGETESTKILLPATH = "package_test_kill_path"; // 停用脚本路径
    private static final String PACKAGETESTKILLNAME = "package_test_kill_name"; // 停用脚本文件名
    @Autowired
    private TSqlDevelopMapper tSqlDevelopMapper;
    @Autowired
    private TDataSourceMapper tDataSourceMapper;
    @Autowired
    private TDataResultSourceMapper tDataResultSourceMapper;


    /**
     * 查询SQL任务开发
     *
     * @param sqlDevelopId SQL任务开发ID
     * @return SQL任务开发
     */
    @Override
    public TSqlDevelop selectTSqlDevelopById(Long sqlDevelopId) {
        return tSqlDevelopMapper.selectTSqlDevelopById(sqlDevelopId);
    }

    /**
     * 查询SQL任务开发列表
     *
     * @param tSqlDevelop SQL任务开发
     * @return SQL任务开发
     */
    @Override
    public List<TSqlDevelop> selectTSqlDevelopList(TSqlDevelop tSqlDevelop) {
        return tSqlDevelopMapper.selectTSqlDevelopList(tSqlDevelop);
    }

    /**
     * 新增SQL任务开发
     *
     * @param tSqlDevelop SQL任务开发
     * @return 结果
     */
    @Override
    public int insertTSqlDevelop(TSqlDevelop tSqlDevelop) {
        tSqlDevelop.setCreateTime(DateUtils.getNowDate());
        return tSqlDevelopMapper.insertTSqlDevelop(tSqlDevelop);
    }

    /**
     * 升级SQL任务开发
     * 先去查询最大的版本号，本版本号再 +1
     *
     * @param tSqlDevelop SQL任务开发
     * @return 结果
     */
    @Override
    public int updateTSqlDevelop(TSqlDevelop tSqlDevelop, Map<String, String> map) {
        if ("0".equals(tSqlDevelop.getRunStatus())) tSqlDevelop.setStopTime(new Date());
        else tSqlDevelop.setStartTime(new Date());

        if (map != null && map.size() > 0) {
            tSqlDevelop.setApplicationId(map.get("applicationId"));
            tSqlDevelop.setJobId(map.get("jobId"));
        }
        return tSqlDevelopMapper.updateTSqlDevelop(tSqlDevelop);
    }

    /**
     * 升级SQL任务开发
     * 先去查询最大的版本号，本版本号再 +1
     *
     * @param tSqlDevelop SQL任务开发
     * @return 结果
     */
    public String insertTSqlDevelopHigh(TSqlDevelop tSqlDevelop) {
        // 先去查询最大的版本号
        String s = tSqlDevelopMapper.selectMaxVersionBySqlTaskName(tSqlDevelop);
        BigDecimal bigDecimal = new BigDecimal(s).add(new BigDecimal("1.0"));
        tSqlDevelop.setSqlTaskVersion(bigDecimal.toString());
        tSqlDevelop.setCreateTime(DateUtils.getNowDate());
        tSqlDevelopMapper.insertTSqlDevelop(tSqlDevelop);
        return "作业名称:" + tSqlDevelop.getSqlTaskName() + ",版本:" + tSqlDevelop.getSqlTaskVersion() + " 升级成功";
    }

    public void selectBooleanStart(TSqlDevelop tSqlDevelop) {
        // 只在启动的时候检查
        if ("1".equals(tSqlDevelop.getRunStatus())) {
            Map s = tSqlDevelopMapper.selectBooleanStart(tSqlDevelop);
            if (s != null && s.size() > 0) {
                throw new CustomException("该作业中版本号为:" + s.get("sqlTaskVersion") + " 还有在启动中的，不能启动多个！");
            }
        }
    }

    /**
     * 批量删除SQL任务开发
     *
     * @param sqlDevelopIds 需要删除的SQL任务开发ID
     * @return 结果
     */
    @Override
    public int deleteTSqlDevelopByIds(Long[] sqlDevelopIds) {
        return tSqlDevelopMapper.deleteTSqlDevelopByIds(sqlDevelopIds);
    }

    /**
     * 删除SQL任务开发信息
     *
     * @param sqlDevelopId SQL任务开发ID
     * @return 结果
     */
    @Override
    public int deleteTSqlDevelopById(Long sqlDevelopId) {
        return tSqlDevelopMapper.deleteTSqlDevelopById(sqlDevelopId);
    }

    /**
     * @param tSqlDevelop SQL任务 实体
     * @return 对应sql作业名最大的版本号
     */
    @Override
    public String selectMaxVersionBySqlTaskName(TSqlDevelop tSqlDevelop) {
        return tSqlDevelopMapper.selectMaxVersionBySqlTaskName(tSqlDevelop);
    }

    /**
     * @param tSqlDevelop 参数实体
     */
    @Override
    public void decideTaskNameBySqlTaskName(TSqlDevelop tSqlDevelop) {
        String s = tSqlDevelopMapper.selectMaxVersionBySqlTaskName(tSqlDevelop);
        if (!Strings.isNullOrEmpty(s)) {
            throw new CustomException("该作业名称【" + tSqlDevelop.getSqlTaskName() + "】已存在！");
        }
    }

    /**
     * 启动作业
     *
     * @param sourceTableId 通常情况下就一个数据源表
     * @param sinkTableIds  可以有多个数据结果表，多个数据结果表用逗号拼接
     * @param sqls          可以有多个sql,多个sql用分号拼接
     * @param jobName       testRun       1代表调试运行，0代表正式运行
     */
    @Override
    public Map<String, String> jobSubmit(String sourceTableId, String sinkTableIds,
                                         String sqls, String jobName, String testRun) {
        // 处理
        String sqlHandle = this.sqlHandle(sqls);

        //第一个变量是sh命令，第二个变量是需要执行的脚本路径，从第三个变量开始是我们要传到脚本里的参数。
        String[] path = new String[]{"sh", PropertiesUtil.getPro(JOBSUBMITPATH) + PropertiesUtil.getPro(JOBSUBMITNAME),
                sourceTableId, sinkTableIds, sqlHandle, jobName, "0"};

        return exe(path);
    }


//    // 启动入口
    public Map exe(String[] path) {
        Map<String, Object> map = new HashMap();
//        try {
//            Runtime runtime = Runtime.getRuntime();
//            LOG.info("----------------exec--------start------------");
//            Process pro = runtime.exec(path);
//            LOG.info("----------------exec--------end------------");
//
//            BufferedReader br = new BufferedReader(new InputStreamReader(pro.getInputStream()));
//            StringBuffer strbr = new StringBuffer();
//            String line;
//            List jobIdList = new ArrayList();
//            List applicationIdList = new ArrayList();
//            while ((line = br.readLine()) != null) {
//                strbr.append(line).append("\n");
//                if (line.contains("Submitted application")) {
//                    String[] words = line.split(" ");
//                    String applicationId = words[words.length - 1];
//                    applicationIdList.add(applicationId);
//                    LOG.info("-----------------applicationId为：" + applicationId);
//
//                }
//                if (line.contains("Job has been submitted with JobID")) {
//                    String[] words = line.split(" ");
//                    String jobId = words[words.length - 1];
//                    jobIdList.add(jobId);
//                    LOG.info("-----------------启动成功！JobID为：" + jobId);
//                }
//            }
//            map.put("applicationId", applicationIdList);
//            map.put("jobId", jobIdList);
//            LOG.info("--------------------------strbr:" + strbr.toString());
//            LOG.info("--------------------------strbr------------------------end----------------");
//
//            // 如果jobID不是2个。则算启动不成功 要杀死另一个applicationId
//            if (jobIdList.size() == 0 || jobIdList.size() == 1) {
//                for (int i = 0; i < applicationIdList.size(); i++) {
//                    jobKill(applicationIdList.get(i).toString());
//                }
//
//                //读取标准错误流
//                BufferedReader brError = new BufferedReader(new InputStreamReader(pro.getErrorStream(), "gb2312"));
//                StringBuffer errline = new StringBuffer();
//                while (brError.readLine() != null) {
//                    errline.append(brError.readLine() + "\n");
//                    if ("null".equals(errline)) {
//                        LOG.info("------------err1:" + errline.toString());
//                    }
//                    LOG.info("----------------err2:" + errline.toString());
//                }
//                map.put("exception", errline.toString());
//            }
//        } catch (IOException ec) {
//            ec.printStackTrace();
//            throw new CustomException("该作业启动异常！");
//        }
        return map;
    }

    /**
     * 停止作业
     *
     * @param applicationId
     */
    @Override
    public void jobKill(String applicationId) {
//        String[] path = new String[]{"sh", PropertiesUtil.getPro(PACKAGETESTKILLPATH) + PropertiesUtil.getPro(PACKAGETESTKILLNAME), applicationId};
//        try {
//            Runtime runtime = Runtime.getRuntime();
//            Process pro = runtime.exec(path);
//            BufferedReader br = new BufferedReader(new InputStreamReader(pro.getInputStream()));
//            StringBuffer strbr = new StringBuffer();
//            String line;
//            while ((line = br.readLine()) != null) {
//                strbr.append(line).append("\n");
//                if (line.contains("Submitted application")) {
//                    String[] words = line.split(" ");
//                    applicationId = words[words.length - 1];
//                    LOG.info("-----------------applicationId为：");
//
//                }
//                if (line.contains("Job has been submitted with JobID")) {
//                    break;
//                }
//            }
//        } catch (IOException ec) {
//            ec.printStackTrace();
//            throw new CustomException("作业停止失败！");
//        }
    }

    /**
     * 用 @_@ 代替 空格
     * 末尾加 “;”
     *
     * @param sql
     * @return
     */
    private String sqlHandle(String sql) {
        String trim = sql.trim();
        if (!trim.endsWith(";")) trim += ";";
        return trim.replaceAll(" ", "@_@");

    }

    /**
     * @param sourceTableId 通常情况下就一个数据源表
     * @param sinkTableIds  可以有多个数据结果表，多个数据结果表用逗号拼接
     * @param sqls          可以有多个sql,多个sql用分号拼接
     * @param jobName       job名称
     * @param testRun       1代表调试运行，0代表正式运行
     * @param param         调试输入参数，多条参数用逗号拼接
     */
    @Override
    public Map testRun(String sourceTableId, String sinkTableIds, String sqls,
                       String jobName, String testRun, String[] param,
                       TDataSource tDataSource, List<TDataResultSource> tDataResultSources) {
        Map map = new HashMap();
        String sqlHandle = sqlHandle(sqls);

        //第一个变量是sh命令，第二个变量是需要执行的脚本路径，从第三个变量开始是我们要传到脚本里的参数。
        String[] path = new String[]{"sh", PropertiesUtil.getPro(JOBSUBMITPATH) + PropertiesUtil.getPro(JOBSUBMITNAME),
                sourceTableId, sinkTableIds, sqlHandle, jobName, "1"};
        String applicationId = "";
        try {
            Runtime runtime = Runtime.getRuntime();
            Process pro = runtime.exec(path);

            BufferedReader br = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            StringBuffer strbr = new StringBuffer();
            String line;
            String jobId;
            boolean runSuccess = false;

            while ((line = br.readLine()) != null) {
                strbr.append(line).append("\n");
                if (line.contains("Submitted application")) {
                    String[] words = line.split(" ");
                    applicationId = words[words.length - 1];
                    LOG.info("applicationId为：" + applicationId);
                    map.put("applicationId", applicationId);
                }
                if (line.contains("Job has been submitted with JobID")) {
                    String[] words = line.split(" ");
                    jobId = words[words.length - 1];
                    runSuccess = true;
                    LOG.info("作业启动成功！JobID为：" + jobId);
                    Thread.sleep(15000);
//                    kafkaMessageSend(tDataSource.getZookeeperAddress(), tDataSource.getKafkaAddress(),
//                            "upwisdom_test_" + tDataSource.getTopicName(), param);
                    StringBuilder s = new StringBuilder();
                    if (tDataResultSources != null && tDataResultSources.size() > 0) {
                        for (int i = 0; i < tDataResultSources.size(); i++) {
                            TDataResultSource tDataResultSource = tDataResultSources.get(i);
                            String message = kafkaMessageGet(tDataResultSource.getZookeeperAddress(), tDataResultSource.getKafkaAddress(),
                                    "upwisdom_test_" + tDataResultSource.getTopicName());
                            s.append(message).append("@_@");
                        }
                        map.put("message", s.substring(0, s.length() - 3));
                    }
                    break;
                }
            }
            if (!runSuccess) {
                //读取标准错误流
                BufferedReader brError = new BufferedReader(new InputStreamReader(pro.getErrorStream(), "gb2312"));
                StringBuffer errline = new StringBuffer();
                while (brError.readLine() != null) {
                    errline.append(brError.readLine() + "\n");
                    LOG.info(errline.toString());
                }
                map.put("exception", errline.toString());
            }
        } catch (IOException | InterruptedException ec) {
            ec.printStackTrace();
            throw new CustomException("该调试出现系统异常！");
        }
        return map;
    }

    @Override
    public Map selectTSqlDevelopByRunStatus() {
        return tSqlDevelopMapper.selectTSqlDevelopByRunStatus();
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
        boolean isFirst = true;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (records.count() == 0) {
                if (!isFirst) {
                    if (records.count() == 0) {
                        strbr = strbr.deleteCharAt(strbr.length() - 1);
                        consumer.close();
                        return strbr.toString();
                    }
                }
                isFirst = false;
            } else {
                for (ConsumerRecord<String, String> record : records) {
                    strbr.append(record.value()).append(",");
                }
            }
        }
    }
}
