package com.skyon.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampToStringYMD extends ScalarFunction {

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

    public String eval(Timestamp tranTime) {
        if (tranTime != null){
            long tranTimeMillis = tranTime.getTime();
            Date date = new Date();
            date.setTime(tranTimeMillis);
            return new SimpleDateFormat("yyyy-MM-dd").format(date);
        } else {
            return null;
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
        jedis.close();
    }
}