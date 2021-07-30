package com.skyon.project.system.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class StatusUtil {

    private static Long vCoreAvailable;
    private static Long vCoresUsed;
    private static Long vCoresTotal;
    private static Double memoryAvailable;
    private static Double memoryUsed;
    private static Double memoryTotal;
    private static ArrayList<String> applicationIds = new ArrayList<>();

    public static ArrayList<String> getApplicationIds() {
        return applicationIds;
    }

    /**
     * 获取状态为Running的所有ID,并且同时获取指定queue的resource剩余情况(即: 可使用的core和memory)
     * @return boolean
     * @return
     */
    public  void yarnAllRunningApiAndQueueResource(String specifyQueue) {
        Configuration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        applicationIds.clear();
        try {
            List<ApplicationReport> applications = yarnClient.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
            Iterator<ApplicationReport> iterator = applications.iterator();
            while (iterator.hasNext()){
                ApplicationReport applicationReport = iterator.next();
                applicationIds.add(applicationReport.getApplicationId().toString());
            }
            List<QueueInfo> allQueues = yarnClient.getAllQueues();
            for (QueueInfo allQueue : allQueues) {
                if (allQueue.getQueueName().equals(specifyQueue)){
                    QueueStatistics queueStatistics = allQueue.getQueueStatistics();
                    memoryAvailable = queueStatistics.getAvailableMemoryMB() / 1024.0;
                    memoryUsed = queueStatistics.getAllocatedMemoryMB() / 1024.0;
                    memoryTotal = memoryAvailable + memoryUsed;
                    vCoreAvailable  = queueStatistics.getAvailableVCores();
                    vCoresUsed = queueStatistics.getAllocatedVCores();
                    vCoresTotal = vCoreAvailable + vCoresUsed;
                }
            }
        } catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            yarnClient.stop();
        }
    }

    public List<Map<String, Double>> getStatus() throws IOException, InterruptedException {
        while(true) {
            List<Map<String, Double>> list = new ArrayList<>();
            Map<String, Double> coreMap = new HashMap<>();
            Map<String, Double> memoryMap = new HashMap<>();
            yarnAllRunningApiAndQueueResource("default");
            coreMap.put("total", Double.valueOf(vCoresTotal));
            coreMap.put("used", Double.valueOf(vCoresUsed));
            coreMap.put("available", Double.valueOf(vCoreAvailable));
            memoryMap.put("total", memoryTotal);
            memoryMap.put("used", memoryUsed);
            memoryMap.put("available", memoryAvailable);
            list.add(coreMap);
            list.add(memoryMap);
            return list;
        }
    }
}
