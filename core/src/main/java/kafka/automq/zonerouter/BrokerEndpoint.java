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

public class BrokerEndpoint {
    private final int id;
    private final String host;
    private final int port;

    public BrokerEndpoint(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public int id() {
        return id;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public String toString() {
        return "BrokerEndpoint{" +
            "id=" + id +
            ", host='" + host + '\'' +
            ", port=" + port +
            '}';
    }
}
