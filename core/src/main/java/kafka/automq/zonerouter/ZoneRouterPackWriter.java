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

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.Writer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import static kafka.automq.zonerouter.ZoneRouterPack.FOOTER_SIZE;
import static kafka.automq.zonerouter.ZoneRouterPack.PACK_MAGIC;
import static kafka.automq.zonerouter.ZoneRouterPack.genObjectPath;

public class ZoneRouterPackWriter {
    private final String path;
    private final Writer writer;
    private final CompositeByteBuf dataBuf;

    public ZoneRouterPackWriter(int nodeId, long objectId, ObjectStorage objectStorage) {
        this.path = genObjectPath(nodeId, objectId);
        this.dataBuf = ByteBufAlloc.compositeByteBuffer();
        this.writer = objectStorage.writer(new ObjectStorage.WriteOptions(), path);
    }

    public Position addProduceRequests(List<ZoneRouterProduceRequest> produceRequests) {
        int position = dataBuf.writerIndex();
        ByteBuf buf = encodeDataBlock(produceRequests);
        int size = buf.readableBytes();
        dataBuf.addComponent(true, buf);
        return new Position(position, size);
    }

    public short bucketId() {
        return writer.bucketId();
    }

    public CompletableFuture<Void> close() {
        ByteBuf footer = ByteBufAlloc.byteBuffer(FOOTER_SIZE);
        footer.writeZero(40);
        footer.writeLong(PACK_MAGIC);
        dataBuf.addComponent(true, footer);
        writer.write(dataBuf);
        return writer.close();
    }

    static ByteBuf encodeDataBlock(List<ZoneRouterProduceRequest> produceRequests) {
        int size = 1 /* magic */;
        List<ObjectSerializationCache> objectSerializationCaches = new ArrayList<>(produceRequests.size());
        List<Integer> dataSizes = new ArrayList<>(produceRequests.size());
        for (ZoneRouterProduceRequest produceRequest : produceRequests) {

            size += 2 /* api version */ + 4 /* data size */;

            ProduceRequestData data = produceRequest.data();
            ObjectSerializationCache objectSerializationCache = new ObjectSerializationCache();
            objectSerializationCaches.add(objectSerializationCache);
            int dataSize = data.size(objectSerializationCache, (short) 11);
            dataSizes.add(dataSize);
            size += dataSize;
        }
        ByteBuf buf = ByteBufAlloc.byteBuffer(size);
        buf.writeByte(ZoneRouterPack.PRODUCE_DATA_BLOCK_MAGIC);
        for (int i = 0; i < produceRequests.size(); i++) {
            ZoneRouterProduceRequest produceRequest = produceRequests.get(i);
            int dataSize = dataSizes.get(i);
            ProduceRequestData data = produceRequest.data();
            ObjectSerializationCache objectSerializationCache = objectSerializationCaches.get(i);

            buf.writeShort(produceRequest.apiVersion());
            buf.writeInt(dataSize);
            data.write(new ByteBufferAccessor(buf.nioBuffer(buf.writerIndex(), dataSize)), objectSerializationCache, (short) 11);
            buf.writerIndex(buf.writerIndex() + dataSize);
        }
        return buf;
    }

}
