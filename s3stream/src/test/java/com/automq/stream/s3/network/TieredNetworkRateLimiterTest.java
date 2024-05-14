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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;

public class TieredNetworkRateLimiterTest extends NetworkRateLimiterTestBase {

    @Test
    public void testMultiLevelThrottle() {
        TieredNetworkRateLimiter limiter = new TieredNetworkRateLimiter(TieredNetworkRateLimiter.Type.INBOUND, 100);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
        RateCounter bypassRater = new RateCounter();
        RateCounter fastRater = new RateCounter();
        RateCounter slowRater = new RateCounter();
        AtomicLong expectedBypassRate = new AtomicLong(20);
        AtomicLong expectedFastRate = new AtomicLong(50);
        AtomicLong expectedSlowRate = new AtomicLong(30);

        createConsumer(ThrottleStrategy.BYPASS, limiter, expectedBypassRate::get, Duration.ofMillis(100), bypassRater, executorService);
        createConsumer(ThrottleStrategy.CATCH_UP, limiter, expectedFastRate::get, Duration.ofMillis(100), fastRater, executorService);
        createConsumer(ThrottleStrategy.COMPACTION, limiter, expectedSlowRate::get, Duration.ofMillis(100), slowRater, executorService);

        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofSeconds(2)).until(() -> {
            double bypassRate = bypassRater.rate(TimeUnit.SECONDS);
            double fastRate = fastRater.rate(TimeUnit.SECONDS);
            double slowRate = slowRater.rate(TimeUnit.SECONDS);
            System.out.printf("bypassRate: %f/s (%d), fastRate: %f/s (%d), slowRate: %f/s (%d)%n",
                    bypassRate, bypassRater.get(), fastRate, fastRater.get(), slowRate, slowRater.get());
            return isDesiredRateReached(bypassRate, expectedBypassRate.get(), DELTA)
                    && isDesiredRateReached(fastRate, expectedFastRate.get(), DELTA)
                    && isDesiredRateReached(slowRate, expectedSlowRate.get(), DELTA);
        });

        expectedBypassRate.set(40);
        System.out.printf("bypass read: %d, fast read: %d, slow read: %d%n", expectedBypassRate.get(), expectedFastRate.get(), expectedSlowRate.get());
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofSeconds(2)).until(() -> {
            double bypassRate = bypassRater.rate(TimeUnit.SECONDS);
            double fastRate = fastRater.rate(TimeUnit.SECONDS);
            double slowRate = slowRater.rate(TimeUnit.SECONDS);
            System.out.printf("bypassRate: %f/s (%d), fastRate: %f/s (%d), slowRate: %f/s (%d)%n",
                    bypassRate, bypassRater.get(), fastRate, fastRater.get(), slowRate, slowRater.get());
            return isDesiredRateReached(bypassRate, expectedBypassRate.get(), DELTA)
                    && isDesiredRateReached(fastRate, expectedFastRate.get(), DELTA)
                    && isDesiredRateReached(slowRate, 10, DELTA);
        });

        expectedBypassRate.set(50);
        System.out.printf("bypass read: %d, fast read: %d, slow read: %d%n", expectedBypassRate.get(), expectedFastRate.get(), expectedSlowRate.get());
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofSeconds(2)).until(() -> {
            double bypassRate = bypassRater.rate(TimeUnit.SECONDS);
            double fastRate = fastRater.rate(TimeUnit.SECONDS);
            double slowRate = slowRater.rate(TimeUnit.SECONDS);
            System.out.printf("bypassRate: %f/s (%d), fastRate: %f/s (%d), slowRate: %f/s (%d)%n",
                    bypassRate, bypassRater.get(), fastRate, fastRater.get(), slowRate, slowRater.get());
            return isDesiredRateReached(bypassRate, expectedBypassRate.get(), DELTA)
                    && isDesiredRateReached(fastRate, expectedFastRate.get(), DELTA)
                    && isDesiredRateReached(slowRate, 0, DELTA);
        });

        expectedSlowRate.set(100);
        System.out.printf("bypass read: %d, fast read: %d, slow read: %d%n", expectedBypassRate.get(), expectedFastRate.get(), expectedSlowRate.get());
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofSeconds(2)).until(() -> {
            double bypassRate = bypassRater.rate(TimeUnit.SECONDS);
            double fastRate = fastRater.rate(TimeUnit.SECONDS);
            double slowRate = slowRater.rate(TimeUnit.SECONDS);
            System.out.printf("bypassRate: %f/s (%d), fastRate: %f/s (%d), slowRate: %f/s (%d)%n",
                    bypassRate, bypassRater.get(), fastRate, fastRater.get(), slowRate, slowRater.get());
            return isDesiredRateReached(bypassRate, expectedBypassRate.get(), DELTA)
                    && isDesiredRateReached(fastRate, expectedFastRate.get(), DELTA)
                    && isDesiredRateReached(slowRate, 0, DELTA);
        });

        expectedBypassRate.set(20);
        expectedFastRate.set(60);
        System.out.printf("bypass read: %d, fast read: %d, slow read: %d%n", expectedBypassRate.get(), expectedFastRate.get(), expectedSlowRate.get());
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofSeconds(2)).until(() -> {
            double bypassRate = bypassRater.rate(TimeUnit.SECONDS);
            double fastRate = fastRater.rate(TimeUnit.SECONDS);
            double slowRate = slowRater.rate(TimeUnit.SECONDS);
            System.out.printf("bypassRate: %f/s (%d), fastRate: %f/s (%d), slowRate: %f/s (%d)%n",
                    bypassRate, bypassRater.get(), fastRate, fastRater.get(), slowRate, slowRater.get());
            return isDesiredRateReached(bypassRate, expectedBypassRate.get(), DELTA)
                    && isDesiredRateReached(fastRate, expectedFastRate.get(), DELTA)
                    && isDesiredRateReached(slowRate, 20, DELTA);
        });

        expectedBypassRate.set(10);
        expectedFastRate.set(30);
        expectedSlowRate.set(60);
        System.out.printf("bypass read: %d, fast read: %d, slow read: %d%n", expectedBypassRate.get(), expectedFastRate.get(), expectedSlowRate.get());
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofSeconds(2)).until(() -> {
            double bypassRate = bypassRater.rate(TimeUnit.SECONDS);
            double fastRate = fastRater.rate(TimeUnit.SECONDS);
            double slowRate = slowRater.rate(TimeUnit.SECONDS);
            System.out.printf("bypassRate: %f/s (%d), fastRate: %f/s (%d), slowRate: %f/s (%d)%n",
                    bypassRate, bypassRater.get(), fastRate, fastRater.get(), slowRate, slowRater.get());
            return isDesiredRateReached(bypassRate, expectedBypassRate.get(), DELTA)
                    && isDesiredRateReached(fastRate, expectedFastRate.get(), DELTA)
                    && isDesiredRateReached(slowRate, expectedSlowRate.get(), DELTA);
        });

        expectedBypassRate.set(100);
        System.out.printf("bypass read: %d, fast read: %d, slow read: %d%n", expectedBypassRate.get(), expectedFastRate.get(), expectedSlowRate.get());
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofSeconds(2)).until(() -> {
            double bypassRate = bypassRater.rate(TimeUnit.SECONDS);
            double fastRate = fastRater.rate(TimeUnit.SECONDS);
            double slowRate = slowRater.rate(TimeUnit.SECONDS);
            System.out.printf("bypassRate: %f/s (%d), fastRate: %f/s (%d), slowRate: %f/s (%d)%n",
                    bypassRate, bypassRater.get(), fastRate, fastRater.get(), slowRate, slowRater.get());
            return isDesiredRateReached(bypassRate, expectedBypassRate.get(), DELTA)
                    && isDesiredRateReached(fastRate, 10, DELTA)
                    && isDesiredRateReached(slowRate, 0, DELTA);
        });

        executorService.shutdown();
    }
}
