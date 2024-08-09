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

package kafka.autobalancer.model;


import com.automq.stream.utils.LogContext;
import com.automq.stream.utils.LogSuppressor;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.common.types.MetricVersion;
import kafka.autobalancer.common.types.Resource;
import kafka.autobalancer.model.samples.AbstractTimeWindowSamples;
import kafka.autobalancer.model.samples.SimpleTimeWindowSamples;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractInstanceUpdater {
    protected static final Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
    protected static final LogSuppressor LOG_SUPPRESSOR = new LogSuppressor(LOGGER, 10000);
    protected final Lock lock = new ReentrantLock();
    protected Map<Byte, AbstractTimeWindowSamples> metricSampleMap = new HashMap<>();
    protected long lastUpdateTimestamp = 0L;
    protected MetricVersion metricVersion = defaultVersion();

    public boolean update(Map<Byte, Double> metricsMap, long time) {
        lock.lock();
        try {
            if (time < lastUpdateTimestamp) {
                LOGGER.warn("Metrics for {} is outdated at {}, last updated time {}", name(), time, lastUpdateTimestamp);
                return false;
            }
            if (!validateMetrics(metricsMap)) {
                LOG_SUPPRESSOR.warn("Metrics validation failed for: {} of version {}, metrics: {}", name(), metricVersion, metricsMap.keySet());
                return false;
            }
            update0(metricsMap, time);
        } finally {
            lock.unlock();
        }
        return true;
    }

    protected MetricVersion defaultVersion() {
        return MetricVersion.V0;
    }

    public MetricVersion metricVersion() {
        return metricVersion;
    }

    public void setMetricVersion(MetricVersion metricVersion) {
        this.metricVersion = metricVersion;
    }

    protected void update0(Map<Byte, Double> metricsMap, long timestamp) {
        for (Map.Entry<Byte, Double> entry : metricsMap.entrySet()) {
            byte metricType = entry.getKey();
            double value = entry.getValue();
            metricSampleMap.computeIfAbsent(metricType, k -> createSample(metricType)).append(value);
        }
        this.lastUpdateTimestamp = timestamp;
    }

    protected AbstractTimeWindowSamples createSample(byte metricType) {
        return new SimpleTimeWindowSamples(1, 1, 1);
    }

    public long getLastUpdateTimestamp() {
        long timestamp;
        lock.lock();
        try {
            timestamp = this.lastUpdateTimestamp;
        } finally {
            lock.unlock();
        }
        return timestamp;
    }

    public AbstractInstance get() {
        return get(-1);
    }

    public AbstractInstance get(long timeSince) {
        lock.lock();
        try {
            return createInstance(lastUpdateTimestamp < timeSince);
        } finally {
            lock.unlock();
        }
    }

    protected abstract String name();

    protected abstract boolean validateMetrics(Map<Byte, Double> metricsMap);

    protected abstract AbstractInstance createInstance(boolean metricsOutOfDate);

    public static abstract class AbstractInstance {
        protected final Map<Byte, Load> loads = new HashMap<>();
        protected final long timestamp;
        protected final MetricVersion metricVersion;
        protected boolean metricsOutOfDate;

        public AbstractInstance(long timestamp, MetricVersion metricVersion, boolean metricsOutOfDate) {
            this.timestamp = timestamp;
            this.metricVersion = metricVersion;
            this.metricsOutOfDate = metricsOutOfDate;
        }

        public boolean isMetricsOutOfDate() {
            return metricsOutOfDate;
        }

        public void setMetricsOutOfDate(boolean metricsOutOfDate) {
            this.metricsOutOfDate = metricsOutOfDate;
        }

        public abstract AbstractInstance copy();

        public void addLoad(byte resource, Load load) {
            this.loads.compute(resource, (k, v) -> {
                if (v == null) {
                    return load;
                }
                v.add(load);
                return v;
            });
        }

        public void reduceLoad(byte resource, Load load) {
            this.loads.compute(resource, (k, v) -> {
                if (v == null) {
                    return load;
                }
                v.reduceValue(load);
                return v;
            });
        }

        public void setLoad(byte resource, Load load) {
            this.loads.put(resource, load);
        }

        public void setLoad(byte resource, double value) {
            setLoad(resource, value, true);
        }

        public void setLoad(byte resource, double value, boolean trusted) {
            this.loads.put(resource, new Load(trusted, value));
        }

        public Load load(byte resource) {
            return loads.getOrDefault(resource, new Load(true, 0));
        }

        public double loadValue(byte resource) {
            Load load = loads.get(resource);
            return load == null ? 0 : load.getValue();
        }

        public Map<Byte, Load> getLoads() {
            return this.loads;
        }

        protected void copyLoads(AbstractInstance other) {
            for (Map.Entry<Byte, Load> entry : other.loads.entrySet()) {
                this.loads.put(entry.getKey(), new Load(entry.getValue()));
            }
        }

        public MetricVersion getMetricVersion() {
            return metricVersion;
        }

        protected String timeString() {
            return "timestamp=" + timestamp;
        }

        protected String loadString() {
            return "Loads={" +
                    buildLoadString() +
                    "}";
        }

        protected String buildLoadString() {
            StringBuilder builder = new StringBuilder();
            int index = 0;
            for (Map.Entry<Byte, Load> entry : loads.entrySet()) {
                String resourceStr = Resource.resourceString(entry.getKey(), entry.getValue().getValue());
                builder.append(resourceStr);
                builder.append(" (");
                builder.append(entry.getValue().isTrusted() ? "trusted" : "untrusted");
                builder.append(")");
                if (index++ != loads.size() - 1) {
                    builder.append(", ");
                }
            }
            return builder.toString();
        }

        @Override
        public String toString() {
            return timeString() + ", " + loadString();
        }
    }

    public static class Load {
        private boolean trusted;
        private double value;

        public Load(boolean trusted, double value) {
            this.trusted = trusted;
            this.value = value;
        }

        public Load(Load other) {
            this.trusted = other.trusted;
            this.value = other.value;
        }

        public boolean isTrusted() {
            return trusted;
        }

        public double getValue() {
            return value;
        }

        public void add(Load load) {
            this.value += load.value;
            this.trusted &= load.trusted;
        }

        public void reduceValue(Load load) {
            this.value -= load.value;
        }
    }
}
