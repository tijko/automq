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

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("S3Unit")
public class ZoneRouterPackTest {

    @Test
    public void testDataBlockCodec() {
        List<ZoneRouterProduceRequest> produceRequests = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ProduceRequestData requestData = new ProduceRequestData();
            requestData.setTransactionalId("trans" + i);
            requestData.setAcks((short) -1);
            ProduceRequestData.TopicProduceDataCollection produceData = new ProduceRequestData.TopicProduceDataCollection();
            ProduceRequestData.PartitionProduceData partitionProduceData = new ProduceRequestData.PartitionProduceData();
            partitionProduceData.setIndex(i);
            partitionProduceData.setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(("simplerecord" + i).getBytes(StandardCharsets.UTF_8))));
            produceData.add(
                new ProduceRequestData.TopicProduceData()
                    .setName("topic")
                    .setPartitionData(List.of(partitionProduceData)));
            requestData.setTopicData(produceData);
            produceRequests.add(new ZoneRouterProduceRequest((short) 11, requestData));
        }
        ByteBuf buf = ZoneRouterPackWriter.encodeDataBlock(produceRequests);
        List<ZoneRouterProduceRequest> decoded = ZoneRouterPackReader.decodeDataBlock(buf);
        Assertions.assertEquals(produceRequests, decoded);
    }

}
