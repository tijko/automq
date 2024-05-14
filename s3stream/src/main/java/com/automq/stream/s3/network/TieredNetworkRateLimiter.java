/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.network;

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.stats.NetworkStats;
import com.automq.stream.s3.metrics.wrapper.RateCounter;
import com.automq.stream.utils.AsyncRateLimiter;
import com.automq.stream.utils.LogContext;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TieredNetworkRateLimiter {
    private static final Logger LOGGER = new LogContext().logger(TieredNetworkRateLimiter.class);
    private static final float PRESERVED_TOKEN_RATIO = 0.1f;
    private final Type type;
    private final long capacity;
    private final long preservedToken;
    private final Map<ThrottleStrategy, AsyncRateLimiter> rateLimiterMap;
    private final Map<ThrottleStrategy, RateCounter> desiredRateMap;
    private final ScheduledExecutorService callBackThreadPool;
    private final ScheduledExecutorService adjustThreadPool;

    public TieredNetworkRateLimiter(Type type, long capacity) {
        this.type = type;
        this.capacity = capacity;
        this.preservedToken = (long) (capacity * PRESERVED_TOKEN_RATIO);
        this.rateLimiterMap = new TreeMap<>();
        this.desiredRateMap = new TreeMap<>();
        this.callBackThreadPool = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("throttle-callback"));
        this.adjustThreadPool = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("throttle-adjust"));
        this.adjustThreadPool.scheduleAtFixedRate(this::adjustThrottle, 0, 2, TimeUnit.SECONDS);
        for (ThrottleStrategy strategy : ThrottleStrategy.values()) {
            if (strategy != ThrottleStrategy.BYPASS) {
                this.rateLimiterMap.put(strategy, new AsyncRateLimiter(capacity));
            }
            RateCounter rateCounter = new RateCounter();
            desiredRateMap.put(strategy, rateCounter);
        }
        S3StreamMetricsManager.registerNetworkRateLimitSupplier(type, () -> this.rateLimiterMap);
    }

    public void shutdown() {
        callBackThreadPool.shutdown();
        adjustThreadPool.shutdown();
    }

    private void adjustThrottle() {
        long bypassRate = (long) desiredRateMap.get(ThrottleStrategy.BYPASS).rate(TimeUnit.SECONDS);
        long tokensRemain = Math.max(preservedToken, capacity - bypassRate);
        for (Map.Entry<ThrottleStrategy, RateCounter> entry : desiredRateMap.entrySet()) {
            ThrottleStrategy strategy = entry.getKey();
            RateCounter rateCounter = entry.getValue();
            if (strategy == ThrottleStrategy.BYPASS) {
                continue;
            }
            rateLimiterMap.get(strategy).setRate(tokensRemain);
            LOGGER.info("Adjusting rate limiter for {} to {}byte/s", strategy, tokensRemain);
            tokensRemain = Math.max(1, tokensRemain - (long) rateCounter.rate(TimeUnit.SECONDS));
        }
    }

    public void forceConsume(long token) {
        desiredRateMap.get(ThrottleStrategy.BYPASS).inc(token);
        logMetrics(token, ThrottleStrategy.BYPASS);
    }

    public CompletableFuture<Void> consume(ThrottleStrategy throttleStrategy, long token) {
        if (throttleStrategy == ThrottleStrategy.BYPASS) {
            forceConsume(token);
            return CompletableFuture.completedFuture(null);
        }
        return rateLimiterMap.get(throttleStrategy).acquire((int) token)
                .whenComplete((unused, throwable) -> {
                    desiredRateMap.get(throttleStrategy).inc(token);
                    logMetrics(token, throttleStrategy);
                });
    }

    private void logMetrics(long size, ThrottleStrategy strategy) {
        NetworkStats.getInstance().networkUsageTotalStats(type, strategy).add(MetricsLevel.INFO, size);
    }

    public enum Type {
        INBOUND("Inbound"),
        OUTBOUND("Outbound");

        private final String name;

        Type(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
