package com.skyon.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * 判断客户信用评分等级是否下降
 */
public class CreditLevelDownFunction extends ScalarFunction {

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

    /**
     * @param custNo
     * @param creditLevel
     * @return 1：信用等级降低；0：信用等级没有降低
     */
    public String eval(String custNo, String creditLevel) {
        List<String> creditLevles = new ArrayList<>();
        creditLevles.add("A");
        creditLevles.add("B");
        creditLevles.add("C");
        creditLevles.add("D");
        String oldCreditLevel = jedis.hget("creditLevel", custNo);
        jedis.hset("creditLevel", custNo, creditLevel);
        if (null != oldCreditLevel) {
            if (creditLevles.indexOf(oldCreditLevel) < creditLevles.indexOf(creditLevel)) {
                return "1";
            } else {
                return "0";
            }
        } else {
            return "0";
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        jedis.close();
    }
}
