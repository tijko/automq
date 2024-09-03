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
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.RequestLocal;
import kafka.server.TransactionSupportedOperation;
import kafka.server.streamaspect.ElasticKafkaApis;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordValidationStats;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultProduceRouter implements ProduceRouter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultProduceRouter.class);
    private final ExecutorService executorService = Threads.newFixedThreadPoolWithMonitor(1, "router", true, LOGGER);
    private final ElasticKafkaApis kafkaApis;
    private final MetadataCache metadataCache;
    private final KafkaConfig kafkaConfig;
    private final AsyncSender asyncSender;
    private final ObjectStorage objectStorage;

    private final AtomicLong nextObjectId = new AtomicLong();

    public DefaultProduceRouter(ElasticKafkaApis kafkaApis, MetadataCache metadataCache, KafkaConfig kafkaConfig,
        ObjectStorage objectStorage) {
        this.kafkaApis = kafkaApis;
        this.metadataCache = metadataCache;
        this.kafkaConfig = kafkaConfig;
        this.asyncSender = new AsyncSender.BrokersAsyncSender(
            kafkaConfig,
            kafkaApis.metrics(),
            Time.SYSTEM,
            "__automq_zonerouter",
            new LogContext()
        );
        this.objectStorage = objectStorage;
    }

    @Override
    public void handleProduceAppend(
        long timeout,
        short requiredAcks,
        boolean internalTopicsAllowed,
        String transactionId,
        Map<TopicPartition, MemoryRecords> entriesPerPartition,
        Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback,
        Consumer<Map<TopicPartition, RecordValidationStats>> recordValidationStatsCallback,
        RequestLocal requestLocal, TransactionSupportedOperation transactionSupportedOperation,
        String clientId
    ) {
        if (clientId.equals("__automq_zonerouter")) {
            LOGGER.info("receive router request={}", entriesPerPartition);
            ByteBuf buf = Unpooled.wrappedBuffer(entriesPerPartition.get(new TopicPartition("__automq_zonerouter", 0)).records().iterator().next().value());
            buf.readByte();
            int nodeId = buf.readInt();
            short bucket = buf.readShort();
            long objectId = buf.readLong();
            int position = buf.readInt();
            int size = buf.readInt();

            new ZoneRouterPackReader(nodeId, bucket, objectId, objectStorage).readProduceRequests(new Position(position, size)).thenAccept(produces -> {
                // TODO: write each produce request
                ZoneRouterProduceRequest zoneRouterProduceRequest = produces.get(0);
                ProduceRequestData data = zoneRouterProduceRequest.data();
                LOGGER.info("read zone router request from s3, data={}", data);
                short apiVersion = zoneRouterProduceRequest.apiVersion();

                Map<TopicPartition, MemoryRecords> realEntriesPerPartition = new HashMap<>();
                data.topicData().forEach(topicData -> {
                    topicData.partitionData().forEach(partitionData -> {
                        realEntriesPerPartition.put(
                            new TopicPartition(topicData.name(), partitionData.index()),
                            (MemoryRecords) partitionData.records()
                        );
                    });
                });

                kafkaApis.handleProduceAppendJavaCompatible(
                    10000L,
                    data.acks(),
                    true, // TODO: pass internal topics allowed
                    data.transactionalId(),
                    realEntriesPerPartition,
                    rst -> {
                        responseCallback.accept(rst);
                        return null;
                    },
                    rst -> {
                        recordValidationStatsCallback.accept(rst);
                        return null;
                    },
                    apiVersion
                );
            });
            return;
        }

        ProduceRequestData data = new ProduceRequestData();
        data.setTransactionalId(transactionId);
        data.setAcks(requiredAcks);

        Map<String, Map<Integer, MemoryRecords>> topicData = new HashMap<>();
        entriesPerPartition.forEach((tp, records) -> topicData.compute(tp.topic(), (topicName, map) -> {
            if (map == null) {
                map = new HashMap<>();
            }
            map.put(tp.partition(), records);
            return map;
        }));
        ProduceRequestData.TopicProduceDataCollection list = new ProduceRequestData.TopicProduceDataCollection();
        topicData.forEach((topicName, partitionData) -> {
            list.add(
                new ProduceRequestData.TopicProduceData()
                    .setName(topicName)
                    .setPartitionData(
                        partitionData.entrySet()
                            .stream()
                            .map(e -> new ProduceRequestData.PartitionProduceData().setIndex(e.getKey()).setRecords(e.getValue()))
                            .collect(Collectors.toList())
                    )
            );
        });
        data.setTopicData(list);

        // TODO: batch and select node based on router
        Node node = metadataCache.getAliveBrokerNode(kafkaConfig.nodeId(), kafkaConfig.interBrokerListenerName()).get();

        long objectId = nextObjectId.incrementAndGet();
        LOGGER.info("try write s3 objectId={} with {}", objectId, data);
        ZoneRouterPackWriter writer = new ZoneRouterPackWriter(kafkaConfig.nodeId(), objectId, objectStorage);
        Position position = writer.addProduceRequests(List.of(new ZoneRouterProduceRequest((short) 11, data)));

        writer.close().thenAccept(nil -> {
            LOGGER.info("write s3 objectId={} with position={}", objectId, position);
            // TODO: codec
            ByteBuf buf = Unpooled.buffer(1 /* magic */ + 4 /* nodeId */ + 2 /* bucketId */ + 8 /* objectId */ + 4 /* position */ + 4 /* size */);
            buf.writeByte(0x01);
            buf.writeInt(kafkaConfig.nodeId());
            buf.writeShort((short) 0);
            buf.writeLong(objectId);
            buf.writeInt(position.position());
            buf.writeInt(position.size());

            ProduceRequestData.PartitionProduceData partitionProduceData = new ProduceRequestData.PartitionProduceData();
            partitionProduceData.setIndex(0);
            partitionProduceData.setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(buf.array())));

            ProduceRequestData.TopicProduceData topicProduceData = new ProduceRequestData.TopicProduceData();
            topicProduceData.setName("__automq_zonerouter");
            topicProduceData.setPartitionData(List.of(partitionProduceData));

            ProduceRequestData.TopicProduceDataCollection topicProduceDataCollection = new ProduceRequestData.TopicProduceDataCollection();
            topicProduceDataCollection.add(topicProduceData);

            ProduceRequestData produceRequestData = new ProduceRequestData();
            produceRequestData.setTopicData(topicProduceDataCollection);
            produceRequestData.setAcks((short) -1);

            // 设置 record ，携带 object 信息和位点信息
            ProduceRequest.Builder builder = ProduceRequest.forMagic(RecordBatch.MAGIC_VALUE_V2, produceRequestData);
            asyncSender.sendRequest(node, builder).thenAccept(clientResponse -> {
                LOGGER.info("receive router response={}", clientResponse);
                if (!clientResponse.hasResponse()) {
                    LOGGER.error("has no response cause by other error");
                    return;
                }
                // TODO: 拆分 batch 响应
                ProduceResponse produceResponse = (ProduceResponse) clientResponse.responseBody();
                ProduceResponseData.TopicProduceResponseCollection topicProduceResponseCollection = produceResponse.data().responses();
                Map<TopicPartition, ProduceResponse.PartitionResponse> partitionResponseMap = new HashMap<>();
                topicProduceResponseCollection.forEach(topicProduceResponse -> {
                    String topicName = topicProduceResponse.name();
                    topicProduceResponse.partitionResponses().forEach(partitionProduceResponse -> {
                        int partition = partitionProduceResponse.index();

                        partitionResponseMap.put(new TopicPartition(topicName, partition), new ProduceResponse.PartitionResponse(
                            Errors.forCode(partitionProduceResponse.errorCode()),
                            partitionProduceResponse.baseOffset(),
                            0, // last offset , the network layer don't need
                            partitionProduceResponse.logAppendTimeMs(),
                            partitionProduceResponse.logStartOffset(),
                            partitionProduceResponse.recordErrors().stream().map(e -> new ProduceResponse.RecordError(e.batchIndex(), e.batchIndexErrorMessage())).collect(Collectors.toList()),
                            partitionProduceResponse.errorMessage(),
                            partitionProduceResponse.currentLeader()
                        ));
                    });
                });
                responseCallback.accept(partitionResponseMap);
            }).exceptionally(ex -> {
                LOGGER.error("[UNEXPECTED]", ex);
                return null;
            });
        }).exceptionally(ex -> {
            LOGGER.error("[UNEXPECTED]", ex);
            return null;
        });
    }
}
