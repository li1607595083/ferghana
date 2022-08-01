package com.skyon.connect.redis.container;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;


public class RedisLookupContainer implements RedisLookupCommandsContainer, Closeable {

    private static final long serialVersionUID = 1L;
    private transient JedisPool jedisPool;
    private transient JedisSentinelPool jedisSentinelPool;

    private static final Logger
            LOG = LoggerFactory.getLogger(RedisLookupContainer.class);


    public RedisLookupContainer(JedisPool jedisPool) {
        Objects.requireNonNull(jedisPool, "Jedis Pool can not be null");
        this.jedisPool = jedisPool;
        this.jedisSentinelPool = null;
    }

    public RedisLookupContainer(JedisSentinelPool sentinelPool) {
        Objects.requireNonNull(sentinelPool, "Jedis Sentinel Pool can not be null");
        this.jedisPool = null;
        this.jedisSentinelPool = sentinelPool;
    }

    private Jedis getInstance() {
        if (jedisSentinelPool != null) {
            return jedisSentinelPool.getResource();
        } else {
            return jedisPool.getResource();
        }
    }

    private void releaseInstance(final Jedis jedis) {
        if (jedis == null) {
            return;
        }
        try {
            jedis.close();
        } catch (Exception e) {
            LOG.error("Failed to close (return) instance to pool", e);
        }
    }

    @Override
    public void open() throws Exception {
        getInstance().echo("Test");
    }

    @Override
    public byte[] get(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            return jedis.get(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot query Redis message with command GET to key {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public byte[] hget(byte[] key, byte[] hashField) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            return jedis.hget(key, hashField);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot query Redis message with command HGET to key {} hashField {} error message {}",
                        key, hashField, e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void close() throws IOException {
        if (this.jedisPool != null) {
            this.jedisPool.close();
        }
        if (this.jedisSentinelPool != null) {
            this.jedisSentinelPool.close();
        }
    }
}
