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

import com.automq.stream.s3.operator.ObjectStorage;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import static kafka.automq.zonerouter.ZoneRouterPack.PRODUCE_DATA_BLOCK_MAGIC;
import static kafka.automq.zonerouter.ZoneRouterPack.genObjectPath;

public class ZoneRouterPackReader {
    private final short bucketId;
    private final String path;
    private final ObjectStorage objectStorage;

    public ZoneRouterPackReader(int nodeId, short bucketId, long objectId, ObjectStorage objectStorage) {
        this.path = genObjectPath(nodeId, objectId);
        this.bucketId = bucketId;
        this.objectStorage = objectStorage;
    }

    public CompletableFuture<List<ZoneRouterProduceRequest>> readProduceRequests(Position position) {
        return objectStorage
            .rangeRead(new ObjectStorage.ReadOptions().bucket(bucketId), path, position.position(), position.position() + position.size())
            .thenApply(ZoneRouterPackReader::decodeDataBlock);
    }

    static List<ZoneRouterProduceRequest> decodeDataBlock(ByteBuf buf) {
        byte magic = buf.readByte();
        if (magic != PRODUCE_DATA_BLOCK_MAGIC) {
            throw new IllegalArgumentException("Invalid magic byte: " + magic);
        }
        List<ZoneRouterProduceRequest> requests = new ArrayList<>();
        while (buf.readableBytes() > 0) {
            short apiVersion = buf.readShort();
            int dataSize = buf.readInt();
            ByteBuf dataBuf = buf.slice(buf.readerIndex(), dataSize);
            ProduceRequestData produceRequestData = new ProduceRequestData();
            produceRequestData.read(new ByteBufferAccessor(dataBuf.nioBuffer()), (short) 11);
            buf.skipBytes(dataSize);
            requests.add(new ZoneRouterProduceRequest(apiVersion, produceRequestData));
        }
        return requests;
    }

}