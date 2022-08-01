package com.skyon.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.expressions.In;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * RedisQueryFunction支持redis的以下方法：
 * get(取值，用法：get key)
 * hget(取值，用法：hget key field)
 * sismember(判断元素是否在集合中，用法：sismember key value)
 * scard(获得集合中元素的个数，用法：scard key)
 */
public class RedisQueryFunction extends ScalarFunction {

    public JedisPool jedisPool;
    public Jedis jedis;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(300);
        config.setMaxTotal(600);
        config.setMaxWaitMillis(10000);
        config.setTestOnBorrow(true);
        jedisPool = new JedisPool(config, "master", 6379, 10000);
        jedis = jedisPool.getResource();
    }

    public String eval(String methodName, String key, String field) {
        String result = null;
        if ("get".equals(methodName)) {
            result = jedis.get(key);
        } else if ("hget".equals(methodName)) {
            result = jedis.hget(key, field);
        } else if ("sismember".equals(methodName)) {
            result = jedis.sismember(key, field) ? "1" : "0";
        }
        return result;
    }

    public int eval(String methodName, String key, String field, String isRemoveRepeat) {
        String result = null;
        int count = 0;
        if ("hcard".equals(methodName)) {
            result = jedis.hget(key, field);
            String[] split = result.split("\\@");
            if ("1".equals(isRemoveRepeat)) {
                Set<String> set = new HashSet<>(Arrays.asList(split));
                count = set.size();
            } else {
                count = split.length;
            }
        }
        return count;
    }

    public Long eval(String methodName, String key) {
        Long result = null;
        if ("scard".equals(methodName)) {
            result = jedis.scard(key);
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        super.close();
        jedis.close();
    }
}
