package com.skyon.connect.redis.mapper;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;


public enum RedisLookupCommand {

    GET(RedisDataType.STRING),

    HGET(RedisDataType.HASH),;

    private RedisDataType redisDataType;

    private RedisLookupCommand(RedisDataType redisDataType) {
        this.redisDataType = redisDataType;
    }

    public RedisDataType getRedisDataType() {
        return redisDataType;
    }
}
