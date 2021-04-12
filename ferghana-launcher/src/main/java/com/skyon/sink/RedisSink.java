package com.skyon.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisSink extends RichSinkFunction<String> {

    private transient JedisPool jedisPool;
    private transient Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(300);
        config.setMaxTotal(600);
        config.setMaxWaitMillis(10000);
        config.setTestOnBorrow(true);
        jedisPool = new JedisPool(config, "192.168.4.95", 6379, 10000);
        jedis = jedisPool.getResource();
        if (!jedis.isConnected()){
            jedis = jedisPool.getResource();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedis.isConnected()){
            jedis.close();
        }
        if (!jedisPool.isClosed()){
            jedisPool.close();
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        String[] split = value.split("\t", 2);
        jedis.hset("ep_openacct_flow_topic",split[0], split[1]);
    }
}
