package com.skyon.utils;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @description: RedisUtils
 * @author: ......
 * @create: 2022/3/2118:31
 */
public class RedisUtils {


    public static Jedis getJedis(String host, int port){
        return new Jedis(host, port, 5000);
    }

    public static void addDataStr(Jedis jedis, String hashName, String data){
        while (!jedis.isConnected()){
            jedis.connect();
        }
        Transaction multi = jedis.multi();
        Object[] testData = JSONObject.parseArray(data).toArray();
        for (Object singleData : testData) {
            HashMap<String, String> hashMap = JSONObject.parseObject(singleData.toString(), HashMap.class);
            Iterator<Map.Entry<String, String>> iterator = hashMap.entrySet().iterator();
            if (iterator.hasNext()){
                Map.Entry<String, String> next = iterator.next();
                if (hashName == null) {
                    multi.set(next.getKey(), next.getValue());
                } else {
                    multi.hset(hashName, next.getKey(), next.getValue());
                }
            }
        }
        multi.exec();
    }


    public static void closeJedis(Jedis jedis){
        if (jedis != null){
            jedis.close();
        }
    }

}
