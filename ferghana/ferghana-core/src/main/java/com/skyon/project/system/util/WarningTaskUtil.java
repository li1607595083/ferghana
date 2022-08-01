package com.skyon.project.system.util;

import java.text.SimpleDateFormat;
import java.util.concurrent.*;

import com.alibaba.fastjson.JSONObject;
import com.skyon.project.system.service.WarningConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 *  预警实现类
 *
 */
class WarningTask implements Runnable {

    private ConcurrentHashMap<String, Future> futureMap;

    private String TaskId;

    private Map map;

    public WarningTask(String TaskId, ConcurrentHashMap<String, Future> futureMap, Map map){
        this.TaskId = TaskId;
        this.futureMap = futureMap;
        this.map = map;
    }

    @Override
    public void run() {
        try{
            // 查看是否在预警时间内
            JSONObject warningEffectTime = JSONObject.parseObject(String.valueOf(map.get("warningEffectTime")));
            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
            String minTime = warningEffectTime.getString("minTime");
            String maxTime = warningEffectTime.getString("maxTime");
            String now = sdf.format(new Date());
            Date beginTime = sdf.parse(minTime);
            Date endTime = sdf.parse(maxTime);
            Date nowTime = sdf.parse(now);
            if(beginTime.before(nowTime) && endTime.after(nowTime)){
                CheckWarningUtil.query(map);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public ConcurrentHashMap<String, Future> getFutureMap() {
        return futureMap;
    }

    public void setFutureMap(ConcurrentHashMap<String, Future> futureMap) {
        this.futureMap = futureMap;
    }

    public String getTaskId() {
        return TaskId;
    }

    public void setTaskId(String TaskId) {
        this.TaskId = TaskId;
    }

    public Map getMap() {
        return map;
    }

    public void setMap(Map map) {
        this.map = map;
    }
}

/**
 *
 *  预警管理工具类
 */
@Component
public class WarningTaskUtil {

    @Autowired
    private ScheduledExecutorService scheduledExecutorService;

    private static ScheduledExecutorService scheduled;

    private static ConcurrentHashMap<String, Future> futureMap = new ConcurrentHashMap<String, Future>();

    @PostConstruct
    public void init(){
        scheduled = scheduledExecutorService;
    }

    public static boolean startWarningTask(Map map) {
        try{
            // TaskId 预警线程唯一标识
            String taskId = String.valueOf(map.get("warningId"));
            WarningTask task = new WarningTask(taskId, futureMap, map);
            Future future = scheduled.scheduleAtFixedRate(task, 0, 5, TimeUnit.MINUTES);
            futureMap.put(task.getTaskId(),future);
            return true;
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public static boolean stopWarningTask(String taskId){
        try{
            // 检查当前预警线程是否正在运行
            if(futureMap.get(taskId) == null){
                return true;
            }
            Future future = futureMap.get(taskId);
            future.cancel(true);
            return true;
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

}

/**
 *
 *  项目启动自动开始预警
 *
 */
@Component
class WarningAutoRunner implements ApplicationRunner {

    @Autowired
    private WarningConfigService warningConfigService;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        System.out.println("-----------SpringBoot已启动,开始预警-----------");
        List<Map> list = warningConfigService.selectWarningConfigMapList();
        list.forEach(item -> {
            WarningTaskUtil.startWarningTask(item);
        });
    }
}