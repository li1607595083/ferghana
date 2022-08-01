package com.skyon.project.system.util;

import com.alibaba.fastjson.JSONObject;
import com.skyon.project.system.domain.SysUser;
import com.skyon.project.system.domain.WarningLog;
import com.skyon.project.system.service.ISysUserService;
import com.skyon.project.system.service.WarningLogService;
import javafx.scene.input.DataFormat;
import kotlin.time.TimeMark;
import net.sf.json.JSONArray;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.mortbay.util.ajax.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Component
public class CheckWarningUtil {

    private static Map map;

    private static InfluxDB influxdbConnection = InfluxDBUtil.getInfluxdbConnection();

    @Autowired
    private WarningLogService warningLogService;

    @Autowired
    private ISysUserService iSysUserService;

    private static ISysUserService staticISysUserService;

    private static WarningLogService staticWarningLogService;

    @PostConstruct
    public void init(){
        staticWarningLogService = warningLogService;
        staticISysUserService = iSysUserService;
    }

    // 查询当前是否需要预警
    public static void query(Map warningConfigMap){
        map = warningConfigMap;
        // Application job id，每一个应用程序启动成功后，都存在一个唯一的 Job Id
        // eg: String job_id = "b2662abc51358f546031afface5da4f3";
        String job_id = String.valueOf(map.get("variablePackageJobId"));
        // 统计时长
        int checkTime = 5;
        // 数据库名
        String dataBase = "flink_metrics";
        // 预警内容
        JSONArray warningContentArray = JSONArray.fromObject(map.get("warningContent"));
        // 是否预警
        boolean warningFlag = false;
        // 真实预警内容
        List<String> warningContentList = new ArrayList<>();
        for(int i=0;i<warningContentArray.size();i++){
            JSONObject warningContent = JSONObject.parseObject(String.valueOf(warningContentArray.get(i)));
            String warningConfigIndicatorsId = String.valueOf(warningContent.get("warningConfigIndicatorsId"));
            String warningConfigIndicatorsName = String.valueOf(warningContent.get("warningConfigIndicatorsName"));
            if("4".equals(warningConfigIndicatorsId)){
                warningFlag = checkIsBack(job_id, checkTime, dataBase) ? true : warningFlag;
                if(checkIsBack(job_id, checkTime, dataBase)){
                    warningContentList.add(warningConfigIndicatorsName);
                }
            }
            else{
                String operatorId = String.valueOf(warningContent.get("operatorId"));
                String value = String.valueOf(warningContent.get("value"));
                if("1".equals(warningConfigIndicatorsId)){
                    warningFlag = restartNum(job_id, checkTime, dataBase, Double.parseDouble(value)).equals(operatorId) ||
                            restartNum(job_id, checkTime, dataBase, Double.parseDouble(value)).equals("3") ? true : warningFlag;
                    if(restartNum(job_id, checkTime, dataBase, Double.parseDouble(value)).equals(operatorId) ||
                            restartNum(job_id, checkTime, dataBase, Double.parseDouble(value)).equals("3")){
                        warningContentList.add(warningConfigIndicatorsName+(operatorId.equals("1") ? "大于" : "小于")+value);
                    }
                }
                else if("3".equals(warningConfigIndicatorsId)){
                    warningFlag = checkpointFailNum(job_id, checkTime, dataBase, Double.parseDouble(value)).equals(operatorId) ||
                            checkpointFailNum(job_id, checkTime, dataBase, Double.parseDouble(value)).equals("3") ? true : warningFlag;
                    if(checkpointFailNum(job_id, checkTime, dataBase, Double.parseDouble(value)).equals(operatorId) ||
                            checkpointFailNum(job_id, checkTime, dataBase, Double.parseDouble(value)).equals("3")){
                        warningContentList.add(warningConfigIndicatorsName+(operatorId.equals("1") ? "大于" : "小于")+value);
                    }
                }
            }
        }
        // 关闭连接
        influxdbConnection.close();
        // 预警Id
        String warningId = String.valueOf(map.get("warningId"));
        // 获取预警频率
        String warningFrequency = String.valueOf(map.get("warningFrequency"));
        Map timeMap = staticWarningLogService.checkWarningTime(Long.valueOf(warningId));
        // 查看距离上一次预警是否达到时间
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            String last = String.valueOf(timeMap.get(warningId));
            Date lastTime = simpleDateFormat.parse(last);
            Date now = new Date();
            long diff = lastTime.getTime() - now.getTime();
            long frequency = 0;
            // 预警频率1分钟
            if("1".equals(warningFrequency)){
                frequency = 1000 * 60;
            }
            // 30分钟
            else if("2".equals(warningFrequency)){
                frequency = 1000 * 60 * 30;
            }
            // 1小时
            else if("3".equals(warningFrequency)){
                frequency = 1000 * 60 * 60;
            }
            // 1天
            else if("4".equals(warningFrequency)){
                frequency = 1000 * 60 * 60 * 24;
            }
            if(diff < frequency){
                warningFlag = false;
            }
        }catch (Exception e){}
        // 获取预警联系人邮箱
        List<String> userMail = selectUserMailList();
        // 满足预警内容，开始预警，写入预警日志
        if(warningFlag){
            boolean flag = insertWarningLog(warningContentList);
            String msgSubject = map.get("variablePackageName") + " - 预警"; //"测试主题";
            String msgText = "预警时间 ： " + simpleDateFormat.format(new Date()) + "\n" + "预警内容 ： "; //"测试内容";
            for(int i=0;i<warningContentList.size();i++){
                msgText = msgText + (i == 0 ? "" : "、") + warningContentList.get(i);
            }
            for(int i=0;i<userMail.size();i++){
                String mailAddress = userMail.get(i);
                MailSendUtil.sendMail(mailAddress,msgSubject,msgText);
            }
        }
    }

    // 检查近5分钟是否出现背压
    private static boolean checkIsBack(String job_id, int checkTime, String dataBase){
        String isBackPressuredSql =
                "SELECT value "
                        + " FROM taskmanager_job_task_isBackPressured "
                        + " WHERE job_id = " + "'" + job_id + "' "
                        + " AND time >= now() - " + checkTime + "m"
                        + " AND value = 'true' "
                        + " tz('Asia/Shanghai')";

        boolean isBackPressuredBoolean = isBackPressured(isBackPressuredSql, dataBase);
//        System.out.println("是够出现被压:\t" + isBackPressuredBoolean);
        return isBackPressuredBoolean;
    }

    // 获取近5分钟checkpoint失败次数
    private static String checkpointFailNum(String job_id, int checkTime, String dataBase, double value){
        String checkpointFailNumbersSql =
                "SELECT value "
                        + " FROM jobmanager_job_numberOfFailedCheckpoints "
                        + " WHERE job_id = " + "'" + job_id + "' "
                        + " AND time >= now() - " + checkTime + "m "
                        + " ORDER BY time DESC"
                        + " tz('Asia/Shanghai')";

        double failNumbers =  getValue(checkpointFailNumbersSql, dataBase);
//        System.out.println("近5分钟checkpoint失败次数:\t" + failNumbers);
        return failNumbers > value ? "1" : failNumbers == value ? "3" : "2";
    }

    // 5分钟内重启次数
    private static String restartNum(String job_id, int checkTime, String dataBase, double value){
        String restartsNumbersSql =
                "SELECT value "
                        + " FROM jobmanager_job_numRestarts "
                        + " WHERE job_id = " + "'" + job_id + "' "
                        + " AND time >= now() - " + checkTime + "m "
                        + " ORDER BY time DESC"
                        + " tz('Asia/Shanghai')";
        double restartsNumbers =  getValue(restartsNumbersSql, dataBase);
//        System.out.println("5分钟内重启次数:\t" + restartsNumbers);
        return restartsNumbers > value ? "1" : restartsNumbers == value ? "3" : "2";
    }

    // 插入预警日志
    private static boolean insertWarningLog(List<String> warningContentList){
        WarningLog warningLog = new WarningLog();
        warningLog.setWarningContent(JSON.toString(warningContentList));
        warningLog.setWarningId(Long.valueOf(String.valueOf((map.get("warningId")))));
        warningLog.setWarningName(String.valueOf(map.get("warningName")));
        warningLog.setWarningNoticeType(map.get("warningNoticeType"));
        warningLog.setWarningNoticeUser(map.get("warningNoticeUser"));
        warningLog.setVariablePackageName(String.valueOf(map.get("variablePackageName")));
        return staticWarningLogService.insertWarningLogList(warningLog) > 0;
    }

    // 获取预警联系人邮箱
    private static List<String> selectUserMailList(){
        JSONArray users = JSONArray.fromObject(map.get("warningNoticeUser"));
        List<String> mailList = new ArrayList<>();
        users.forEach(item -> {
            SysUser sysUser = staticISysUserService.selectUserById(Long.valueOf(String.valueOf(item)));
            mailList.add(sysUser.getEmail());
        });
        return mailList;
    }


    private static boolean isBackPressured(String isBackPressuredSql, String database) {
        List<QueryResult.Series> series = null;
        QueryResult flink = influxdbConnection.query(new Query(isBackPressuredSql, database));
        List<QueryResult.Result> results = flink.getResults();
        for (QueryResult.Result result : results) {
            series = result.getSeries();
        }
        return series != null;
    }

    private static double getValue(String lastFailNumbersSql, String database) {
        QueryResult queryResult = influxdbConnection.query(new Query(lastFailNumbersSql, database));
        List<QueryResult.Result> results = queryResult.getResults();
        List<List<Object>> values = results.get(0).getSeries().get(0).getValues();
        double current = (double)values.get(0).get(1);
        double last = (double)values.get(values.size() - 1).get(1);
        return  current - last;
    }
}
