package com.skyon.connect.redis.mapper;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;

import java.util.Objects;


public class RedisCommandDescription {

    private static final long serialVersionUID = 1L;

    private RedisLookupCommand redisLookupCommand;

    private String additionalKey;

    public RedisCommandDescription(RedisLookupCommand redisLookupCommand, String additionalKey) {
        Objects.requireNonNull(redisLookupCommand, "Redis command type can not be null");
        this.redisLookupCommand = redisLookupCommand;
        this.additionalKey = additionalKey;

        if (redisLookupCommand.getRedisDataType() == RedisDataType.HASH) {
            if (additionalKey == null) {
                throw new IllegalArgumentException("Hash should have additional key");
            }
        }
    }

    public RedisCommandDescription(RedisLookupCommand redisLookupCommand) {
        this(redisLookupCommand, null);
    }


    public RedisLookupCommand getRedisLookupCommand() {
        return redisLookupCommand;
    }

    public String getAdditionalKey() {
        return additionalKey;
    }
}
