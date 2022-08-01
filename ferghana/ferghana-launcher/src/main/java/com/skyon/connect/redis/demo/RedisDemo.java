package com.skyon.connect.redis.demo;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

/**
 * redis 安装：https://blog.csdn.net/realize_dream/article/details/106227622
 * redis java client：https://www.cnblogs.com/chenyanbin/p/12088796.html
 */
public class RedisDemo {

    public static void main(String[] args) {
        singleConnect();
        poolConnect();
    }

    public static void singleConnect() {
        // jedis单实例连接
        Jedis jedis = new Jedis("192.168.4.95", 6379);
        String result = jedis.get("a");

        HashMap<String, Object> h = new HashMap<>();

        h.put("name", "laowang");
        h.put("name1", "t3");
        h.put("score", "3");

        String s = new Gson().toJson(h);

        jedis.set("a", s);
        jedis.hset("b", "tom001", s);
        System.out.println(result);
        jedis.close();

        JedisCluster jedisCluster = new JedisCluster(new HostAndPort("192.168.30.97", 6379));
        jedisCluster.hset("b", "tom001", s);
    }

    public static void poolConnect() {
        //jedis连接池
        JedisPool pool = new JedisPool("192.168.4.95", 6379);
        Jedis jedis = pool.getResource();
        String result = jedis.get("a");
        System.out.println("singlet:\t" + result);
        Map<String, String> a = jedis.hgetAll("b");
        for (Map.Entry<String, String> stringStringEntry : a.entrySet()) {
            System.out.println("b:\t" + stringStringEntry.getKey() + "|" + stringStringEntry.getValue());
        }
        jedis.close();
        pool.close();
        JedisCluster jedisCluster = new JedisCluster(new HostAndPort("192.168.30.97", 6379));
        String hget = jedisCluster.hget("b", "tom001");
        System.out.println("cluster:\t" + hget);
    }

}
