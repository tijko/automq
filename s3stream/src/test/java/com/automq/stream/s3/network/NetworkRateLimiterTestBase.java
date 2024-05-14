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

import com.automq.stream.s3.metrics.wrapper.RateCounter;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class NetworkRateLimiterTestBase {
    protected static final double DELTA = 2.0;

    public boolean isDesiredRateReached(double rate, double desiredRate, double delta) {
        return Math.abs(rate - desiredRate) < delta;
    }

    public void createConsumer(ThrottleStrategy strategy, TieredNetworkRateLimiter limiter, Supplier<Long> desiredRate,
                               Duration interval, RateCounter counter, ScheduledExecutorService scheduledExecutorService) {
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            long size = (long) (desiredRate.get() * ((double) interval.toMillis() / 1000));
            limiter.consume(strategy, size).whenComplete((v, e) -> {
                counter.inc(size);
            });
        }, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    }
}
