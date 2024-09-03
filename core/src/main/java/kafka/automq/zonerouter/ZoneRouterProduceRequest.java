/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.automq.zonerouter;

import java.util.Objects;
import org.apache.kafka.common.message.ProduceRequestData;

public class ZoneRouterProduceRequest {
    private final short apiVersion;
    private final ProduceRequestData data;

    public ZoneRouterProduceRequest(short apiVersion, ProduceRequestData data) {
        this.apiVersion = apiVersion;
        this.data = data;
    }

    public short apiVersion() {
        return apiVersion;
    }

    public ProduceRequestData data() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ZoneRouterProduceRequest request = (ZoneRouterProduceRequest) o;
        return apiVersion == request.apiVersion && Objects.equals(data, request.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(apiVersion, data);
    }
}
