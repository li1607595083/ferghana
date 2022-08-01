/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.skyon.connect.redis.table.source;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.skyon.connect.redis.container.RedisLookupCommandsContainer;
import com.skyon.connect.redis.container.RedisLookupCommandsContainerBuilder;
import com.skyon.connect.redis.mapper.RedisLookupMapper;
import com.skyon.connect.redis.mapper.RedisLookupCommand;
import com.skyon.connect.redis.mapper.RedisCommandDescription;
import com.skyon.connect.redis.options.RedisLookupOptions;

/**
 * The RedisRowDataLookupFunction is a standard user-defined table function, it can be used in
 * tableAPI and also useful for temporal table join plan in SQL. It looks up the result as {@link
 * RowData}.
 */
@Internal
public class RedisRowDataLookupFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(
            RedisRowDataLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private String additionalKey;
    private RedisLookupMapper redisLookupMapper;
    private RedisLookupCommand redisLookupCommand;

    protected final RedisLookupOptions redisLookupOptions;

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private transient RedisLookupCommandsContainer redisLookupCommandsContainer;

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final String primarykey;

    private transient Cache<Object, RowData> cache;

    private transient Consumer<Object[]> evaler;

    public RedisRowDataLookupFunction(
            FlinkJedisConfigBase flinkJedisConfigBase
            , RedisLookupMapper redisLookupMapper
            , RedisLookupOptions redisLookupOptions
            , String hashname
            , String primarykey
            ) {

        this.flinkJedisConfigBase = flinkJedisConfigBase;

        this.redisLookupMapper = redisLookupMapper;
        this.redisLookupOptions = redisLookupOptions;
        RedisCommandDescription redisCommandDescription = redisLookupMapper.getCommandDescription(hashname);
        this.redisLookupCommand = redisCommandDescription.getRedisLookupCommand();
        this.additionalKey = redisCommandDescription.getAdditionalKey();

        this.cacheMaxSize = this.redisLookupOptions.getCacheMaxSize();
        this.cacheExpireMs = this.redisLookupOptions.getCacheExpireMs();
        this.maxRetryTimes = this.redisLookupOptions.getMaxRetryTimes();
        this.primarykey = primarykey;
    }

    /**
     * The invoke entry point of lookup function.
     *
     * @param objects the lookup key. Currently only support single rowkey.
     */
    public void eval(Object... objects) throws IOException {

        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                // fetch result
                this.evaler.accept(objects);
                break;
            } catch (Exception e) {
                LOG.error(String.format("Redis lookup error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of Redis lookup failed.", e);
                }
                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }


    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");

        try {
            this.redisLookupCommandsContainer =
                    RedisLookupCommandsContainerBuilder
                            .build(this.flinkJedisConfigBase);
            this.redisLookupCommandsContainer.open();
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw new RuntimeException(e);
        }

        this.cache = cacheMaxSize <= 0 || cacheExpireMs <= 0 ? null : CacheBuilder.newBuilder()
                .recordStats()
                .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                .maximumSize(cacheMaxSize)
                .build();

        if (cache != null) {
            context.getMetricGroup()
                    .gauge("lookupCacheHitRate", (Gauge<Double>) () -> cache.stats().hitRate());

            this.evaler = in -> {
                String inkey = inToString(in);
                RowData cacheRowData = (RowData) cache.getIfPresent(inkey);
                if (cacheRowData != null) {
                    collect(cacheRowData);
                } else {
                    // fetch result
                    byte[] key = redisLookupMapper.serialize(in);
                    byte[] value = null;

                    switch (redisLookupCommand) {
                        case GET:
                            value = this.redisLookupCommandsContainer.get(key);
                            break;
                        case HGET:
                            value = this.redisLookupCommandsContainer.hget(this.additionalKey.getBytes(), key);
                            value = value == null ? "{}".getBytes() : value;
                            break;
                        default:
                            throw new IllegalArgumentException("Cannot process such data type: " + redisLookupCommand);
                    }
                    RowData rowData = this.redisLookupMapper.deserialize(value, primarykey, inkey);
                    collect(rowData);
                    cache.put(inkey, rowData);
                }
            };

        } else {
            this.evaler = in -> {
                // fetch result
                byte[] key = redisLookupMapper.serialize(in);

                byte[] value = null;

                switch (redisLookupCommand) {
                    case GET:
                        value = this.redisLookupCommandsContainer.get(key);
                        break;
                    case HGET:
                        value = this.redisLookupCommandsContainer.hget(this.additionalKey.getBytes(), key);
                        value = value == null ? "{}".getBytes() : value;
                        break;
                    default:
                        throw new IllegalArgumentException("Cannot process such data type: " + redisLookupCommand);
                }
                RowData rowData = this.redisLookupMapper.deserialize(value, primarykey, inToString(in));

                collect(rowData);
            };
        }

        LOG.info("end open.");
    }

    /**
     * @desc 数组转转字符串，并去掉括号;
     * @param in
     * @return
     */
    private String inToString(Object[] in){
        String s = Arrays.toString(in);
        return s.substring(1, s.length() - 1);
    }

    @Override
    public void close() {
        LOG.info("start close ...");
        if (redisLookupCommandsContainer != null) {
            try {
                redisLookupCommandsContainer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        LOG.info("end close.");
    }
}
