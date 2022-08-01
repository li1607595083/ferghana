package com.skyon.connect.redis.container;

import java.util.Objects;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import org.apache.flink.streaming.connectors.redis.common.container.RedisClusterContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;


public class RedisLookupCommandsContainerBuilder {

    public static RedisLookupCommandsContainer build(FlinkJedisConfigBase flinkJedisConfigBase) {
        if (flinkJedisConfigBase instanceof FlinkJedisPoolConfig) {
            FlinkJedisPoolConfig flinkJedisPoolConfig = (FlinkJedisPoolConfig) flinkJedisConfigBase;
            return RedisLookupCommandsContainerBuilder.build(flinkJedisPoolConfig);
        } else if (flinkJedisConfigBase instanceof FlinkJedisClusterConfig) {
            FlinkJedisClusterConfig flinkJedisClusterConfig = (FlinkJedisClusterConfig) flinkJedisConfigBase;
            return RedisLookupCommandsContainerBuilder.build(flinkJedisClusterConfig);
        } else {
            throw new IllegalArgumentException("Jedis configuration not found");
        }
    }

    public static RedisLookupCommandsContainer build(FlinkJedisClusterConfig jedisClusterConfig) {
        Objects.requireNonNull(jedisClusterConfig, "Redis cluster config should not be Null");
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisClusterConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisClusterConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisClusterConfig.getMinIdle());
        JedisCluster jedisCluster = new JedisCluster(jedisClusterConfig.getNodes(), jedisClusterConfig.getConnectionTimeout(), jedisClusterConfig.getMaxRedirections(), genericObjectPoolConfig);
        return new RedisLookupClusterContainer(jedisCluster);
    }

    public static RedisLookupCommandsContainer build(FlinkJedisPoolConfig jedisPoolConfig) {
        Objects.requireNonNull(jedisPoolConfig, "Redis pool config should not be Null");
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisPoolConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisPoolConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisPoolConfig.getMinIdle());
        JedisPool jedisPool = new JedisPool(genericObjectPoolConfig, jedisPoolConfig.getHost(),
                jedisPoolConfig.getPort(), jedisPoolConfig.getConnectionTimeout(), jedisPoolConfig.getPassword(),
                jedisPoolConfig.getDatabase());
        return new RedisLookupContainer(jedisPool);
    }

}
