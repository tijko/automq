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

import java.util.concurrent.TimeUnit;

public class RateCounter extends Counter {
    private long lastCount;
    private long lastTimeNs;

    public RateCounter() {
        lastCount = 0;
        lastTimeNs = System.nanoTime();
    }

    public double rate(TimeUnit timeUnit) {
        try {
            if (lastTimeNs == 0) {
                return 0.0;
            }
            long currentTimeNs = System.nanoTime();
            long timeDiffNs = currentTimeNs - lastTimeNs;
            if (timeDiffNs <= 0) {
                return 0.0;
            }
            long countDiff = get() - lastCount;
            return ((double) countDiff / timeDiffNs) * timeUnit.toNanos(1);
        } finally {
            lastCount = get();
            lastTimeNs = System.nanoTime();
        }
    }
}
