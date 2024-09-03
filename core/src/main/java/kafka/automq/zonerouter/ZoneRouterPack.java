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

import com.automq.stream.s3.metadata.ObjectUtils;

public class ZoneRouterPack {
    public static final byte PRODUCE_DATA_BLOCK_MAGIC = 0x01;
    public static final int FOOTER_SIZE = 48;
    public static final long PACK_MAGIC = 0x88e241b785f4cff9L;

    public static String genObjectPath(int nodeId, long objectId) {
        String pathPrefix = new StringBuilder(String.format("%08x", nodeId)).reverse().toString();
        return pathPrefix + "/" + ObjectUtils.getNamespace() + "/router/" + objectId;
    }
}
