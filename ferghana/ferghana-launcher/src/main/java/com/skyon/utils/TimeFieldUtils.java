package com.skyon.utils;

/**
 * @description: TimeFieldUtil
 * @author: ......
 * @create: 2022/3/3011:44
 */
public class TimeFieldUtils {
    public static String dealTimeField(String field_values){
        // yyyy-MM-ddTHH:mm:ss.SSS
        String[] dateAndTime = field_values.split("T", -1);
        // yyyy-MM-dd
        if (dateAndTime.length == 1){
            field_values = field_values.replaceFirst("T", "") + "T" + "00:00:00.000";
        } else if (dateAndTime.length == 2){
            String[] spTime = dateAndTime[1].split(":");
            // HH
            if (spTime.length == 1){
                field_values = field_values + ":00:00.000";
                // HH:mm
            } else if (spTime.length == 2){
                field_values = field_values + ":00.000";
                // HH:mm:ss.SSS
            } else if (spTime.length == 3){
                // ss
                if (!spTime[2].contains(".")){
                    field_values = field_values + ".000";
                }
            }

        }
        return field_values;
    }
}
