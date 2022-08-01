package com.skyon.project.system.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/3/15
 */
public class ApplicationStatusUtil {

    //
    private static Long availableCore;
    private static Long availableMem;
    private static ArrayList<String> applicationIds = new ArrayList<>();

    public static Long getAvailableCore() {
        return availableCore;
    }

    public static Long getAvailableMem() {
        return availableMem;
    }

    public static ArrayList<String> getApplicationIds() {
        return applicationIds;
    }

    /**
     * 获取状态为Running的所有ID,并且同时获取指定queue的resource剩余情况(即: 可使用的core和memory)
     * @return boolean
     * @return
     */
    public static ArrayList<String> yarnAllRunningApiAndQueueResource(String specifyQueue) {
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
                    availableMem = allQueue.getQueueStatistics().getAvailableMemoryMB();
                    availableCore  = allQueue.getQueueStatistics().getAvailableVCores();
                }
            }
        } catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            yarnClient.stop();
        }
//        applicationIds.add("application_1615431596457_0005");
//        applicationIds.add("application_1610616934348_0292");
//        applicationIds.add("application_1615431596457_0001");
        return applicationIds;
    }

    /*
     * public static void main(String[] args) throws IOException, InterruptedException {
     *
     *         while(true) {
     *             TimeUnit.SECONDS.sleep(3);
     *             yarnAllRunningApiAndQueueResource("default");
     *         }
     *
     *     }
     */

//    public static void main(String[] args) throws IOException, InterruptedException {
//        while(true) {
//            TimeUnit.SECONDS.sleep(3);
//            yarnAllRunningApiAndQueueResource("default");
//            for (String applicationId : getApplicationIds()) {
//                System.out.println(applicationId);
//            }
//            System.out.println("core: " + availableCore +"\t" + "memory: " + availableMem);
//        }
//    }

}
