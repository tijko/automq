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

package com.automq.stream.s3.metrics.wrapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RateCounterTest {
    @Test
    public void testRate() throws InterruptedException {
        RateCounter rateCounter = new RateCounter();
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(() -> rateCounter.inc(10), 0, 100, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(0, rateCounter.rate(TimeUnit.SECONDS));
        Thread.sleep(1000);
        Assertions.assertEquals(100, rateCounter.rate(TimeUnit.SECONDS), 10);
        executorService.shutdown();
    }
}
