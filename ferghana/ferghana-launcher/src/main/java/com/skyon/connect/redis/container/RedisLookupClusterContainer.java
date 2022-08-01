package com.skyon.connect.redis.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.Objects;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/11/17
 */
public class RedisLookupClusterContainer implements RedisLookupCommandsContainer{

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.streaming.connectors.redis.common.container.RedisClusterContainer.class);
    private transient JedisCluster jedisCluster;

    public RedisLookupClusterContainer(JedisCluster jedisCluster) {
        Objects.requireNonNull(jedisCluster, "Jedis cluster can not be null");
        this.jedisCluster = jedisCluster;
    }

    public void open() throws Exception {
        this.jedisCluster.echo("Test");
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            return jedisCluster.get(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot query Redis message with command GET to key {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public byte[] hget(byte[] key, byte[] hashField) {
        try {
            return jedisCluster.hget(key, hashField);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot query Redis message with command HGET to key {} hashField {} error message {}",
                        key, hashField, e.getMessage());
            }
            throw e;
        }
    }

    public void close() throws IOException {
        this.jedisCluster.close();
    }
}
