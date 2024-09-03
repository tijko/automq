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

public class Position {
    private final int position;
    private final int size;

    public Position(int position, int size) {
        this.position = position;
        this.size = size;
    }

    public int position() {
        return position;
    }

    public int size() {
        return size;
    }
}
