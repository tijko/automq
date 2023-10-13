/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller.stream;

import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import java.util.HashMap;
import java.util.stream.Stream;
import org.apache.kafka.common.message.CloseStreamsRequestData.CloseStreamRequest;
import org.apache.kafka.common.message.CloseStreamsResponseData.CloseStreamResponse;
import org.apache.kafka.common.message.CommitStreamObjectRequestData;
import org.apache.kafka.common.message.CommitStreamObjectResponseData;
import org.apache.kafka.common.message.CommitWALObjectRequestData;
import org.apache.kafka.common.message.CommitWALObjectRequestData.ObjectStreamRange;
import org.apache.kafka.common.message.CommitWALObjectRequestData.StreamObject;
import org.apache.kafka.common.message.CommitWALObjectResponseData;
import org.apache.kafka.common.message.CreateStreamsRequestData.CreateStreamRequest;
import org.apache.kafka.common.message.CreateStreamsResponseData.CreateStreamResponse;
import org.apache.kafka.common.message.DeleteStreamsRequestData.DeleteStreamRequest;
import org.apache.kafka.common.message.DeleteStreamsResponseData.DeleteStreamResponse;
import org.apache.kafka.common.message.GetOpeningStreamsRequestData;
import org.apache.kafka.common.message.GetOpeningStreamsResponseData;
import org.apache.kafka.common.message.GetOpeningStreamsResponseData.StreamMetadata;
import org.apache.kafka.common.message.OpenStreamsRequestData.OpenStreamRequest;
import org.apache.kafka.common.message.OpenStreamsResponseData.OpenStreamResponse;
import org.apache.kafka.common.message.TrimStreamsRequestData.TrimStreamRequest;
import org.apache.kafka.common.message.TrimStreamsResponseData.TrimStreamResponse;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveNodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveWALObjectRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;
import org.apache.kafka.common.metadata.WALObjectRecord.StreamIndex;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.metadata.stream.Convertor;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3WALObject;
import com.automq.stream.s3.metadata.StreamState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineLong;
import org.apache.kafka.timeline.TimelineObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;

/**
 * The StreamControlManager manages all Stream's lifecycle, such as create, open, delete, etc.
 */
@SuppressWarnings("all")
public class StreamControlManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamControlManager.class);

    public static class S3StreamMetadata {

        // current epoch, when created but not open, use -1 represent
        private final TimelineLong currentEpoch;
        // rangeIndex, when created but not open, there is no range, use -1 represent
        private final TimelineInteger currentRangeIndex;
        /**
         * The visible start offset of stream, it may be larger than the start offset of current range.
         */
        private final TimelineLong startOffset;
        private final TimelineObject<StreamState> currentState;
        private final TimelineHashMap<Integer/*rangeIndex*/, RangeMetadata> ranges;
        private final TimelineHashMap<Long/*objectId*/, S3StreamObject> streamObjects;

        public S3StreamMetadata(long currentEpoch, int currentRangeIndex, long startOffset,
                                StreamState currentState, SnapshotRegistry registry) {
            this.currentEpoch = new TimelineLong(registry);
            this.currentEpoch.set(currentEpoch);
            this.currentRangeIndex = new TimelineInteger(registry);
            this.currentRangeIndex.set(currentRangeIndex);
            this.startOffset = new TimelineLong(registry);
            this.startOffset.set(startOffset);
            this.currentState = new TimelineObject<StreamState>(registry, currentState);
            this.ranges = new TimelineHashMap<>(registry, 0);
            this.streamObjects = new TimelineHashMap<>(registry, 0);
        }

        public long currentEpoch() {
            return currentEpoch.get();
        }

        public int currentRangeIndex() {
            return currentRangeIndex.get();
        }

        public long startOffset() {
            return startOffset.get();
        }

        public StreamState currentState() {
            return currentState.get();
        }

        public Map<Integer, RangeMetadata> ranges() {
            return ranges;
        }

        public RangeMetadata currentRangeMetadata() {
            return ranges.get(currentRangeIndex.get());
        }

        public Map<Long, S3StreamObject> streamObjects() {
            return streamObjects;
        }

        @Override
        public String toString() {
            return "S3StreamMetadata{" +
                    "currentEpoch=" + currentEpoch.get() +
                    ", currentState=" + currentState.get() +
                    ", currentRangeIndex=" + currentRangeIndex.get() +
                    ", startOffset=" + startOffset.get() +
                    ", ranges=" + ranges +
                    ", streamObjects=" + streamObjects +
                    '}';
        }
    }

    public static class NodeS3WALMetadata {

        private final int nodeId;
        private final TimelineLong nodeEpoch;
        private final TimelineHashMap<Long/*objectId*/, S3WALObject> walObjects;

        public NodeS3WALMetadata(int nodeId, long nodeEpoch, SnapshotRegistry registry) {
            this.nodeId = nodeId;
            this.nodeEpoch = new TimelineLong(registry);
            this.nodeEpoch.set(nodeEpoch);
            this.walObjects = new TimelineHashMap<>(registry, 0);
        }

        public int getNodeId() {
            return nodeId;
        }

        public long getNodeEpoch() {
            return nodeEpoch.get();
        }

        public TimelineHashMap<Long, S3WALObject> walObjects() {
            return walObjects;
        }

        @Override
        public String toString() {
            return "NodeS3WALMetadata{" +
                    "nodeId=" + nodeId +
                    ", nodeEpoch=" + nodeEpoch +
                    ", walObjects=" + walObjects +
                    '}';
        }
    }

    private final SnapshotRegistry snapshotRegistry;

    private final Logger log;

    private final S3ObjectControlManager s3ObjectControlManager;

    /**
     * The next stream id to be assigned.
     */
    private final TimelineLong nextAssignedStreamId;

    private final TimelineHashMap<Long/*streamId*/, S3StreamMetadata> streamsMetadata;

    private final TimelineHashMap<Integer/*nodeId*/, NodeS3WALMetadata> nodesMetadata;

    public StreamControlManager(
            SnapshotRegistry snapshotRegistry,
            LogContext logContext,
            S3ObjectControlManager s3ObjectControlManager) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(StreamControlManager.class);
        this.s3ObjectControlManager = s3ObjectControlManager;
        this.nextAssignedStreamId = new TimelineLong(snapshotRegistry);
        this.streamsMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.nodesMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    public ControllerResult<CreateStreamResponse> createStream(int nodeId, long nodeEpoch, CreateStreamRequest request) {
        CreateStreamResponse resp = new CreateStreamResponse();

        // verify node epoch
        Errors nodeEpochCheckResult = nodeEpochCheck(nodeId, nodeEpoch);
        if (nodeEpochCheckResult != Errors.NONE) {
            resp.setErrorCode(nodeEpochCheckResult.code());
            log.warn("[CreateStream]: node: {}'s epoch: {} check failed, code: {}",
                nodeId, nodeEpoch, nodeEpochCheckResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // TODO: pre assigned a batch of stream id in controller
        long streamId = nextAssignedStreamId.get();
        // update assigned id
        ApiMessageAndVersion record0 = new ApiMessageAndVersion(new AssignedStreamIdRecord()
            .setAssignedStreamId(streamId), (short) 0);
        // create stream
        ApiMessageAndVersion record = new ApiMessageAndVersion(new S3StreamRecord()
            .setStreamId(streamId)
            .setEpoch(S3StreamConstant.INIT_EPOCH)
            .setStartOffset(S3StreamConstant.INIT_START_OFFSET)
            .setRangeIndex(S3StreamConstant.INIT_RANGE_INDEX), (short) 0);
        resp.setStreamId(streamId);
        log.info("[CreateStream]: create stream {} success", streamId);
        return ControllerResult.atomicOf(Arrays.asList(record0, record), resp);
    }


    /**
     * Open stream.
     * <p>
     * <b>Response Errors Enum:</b>
     * <ul>
     *     <li>
     *         <code>STREAM_FENCED</code>:
     *          <ol>
     *              <li> stream's epoch is larger than request epoch </li>
     *              <li> stream's current range's node is not equal to request node </li>
     *              <li> stream's epoch matched, but stream's state is <code>CLOSED</code> </li>
     *          </ol>
     *     </li>
     *     <li>
     *         <code>STREAM_NOT_EXIST</code>
     *         <ol>
     *             <li> stream's id not exist in current stream-metadata </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <code>STREAM_NOT_CLOSED</code>
     *         <ol>
     *             <li> request with higher epoch but stream's state is <code>OPENED</code> </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <code>STREAM_INNER_ERROR</code>
     *         <ol>
     *             <li> stream's current range not exist when stream has been opened </li>
     *         </ol>
     *     </li>
     * </ul>
     */
    public ControllerResult<OpenStreamResponse> openStream(int nodeId, long nodeEpoch, OpenStreamRequest request) {
        OpenStreamResponse resp = new OpenStreamResponse();
        long streamId = request.streamId();
        long epoch = request.streamEpoch();

        // verify node epoch
        Errors nodeEpochCheckResult = nodeEpochCheck(nodeId, nodeEpoch);
        if (nodeEpochCheckResult != Errors.NONE) {
            resp.setErrorCode(nodeEpochCheckResult.code());
            log.warn("[OpenStream]: node: {}'s epoch: {} check failed, code: {}",
                nodeId, nodeEpoch, nodeEpochCheckResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        // verify stream exist
        if (!this.streamsMetadata.containsKey(streamId)) {
            resp.setErrorCode(Errors.STREAM_NOT_EXIST.code());
            log.warn("[OpenStream]: stream {} not exist", streamId);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // verify epoch match
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata.currentEpoch.get() > epoch) {
            resp.setErrorCode(Errors.STREAM_FENCED.code());
            log.warn("[OpenStream]: stream {}'s epoch {} is larger than request epoch {}", streamId,
                streamMetadata.currentEpoch.get(), epoch);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (streamMetadata.currentEpoch.get() == epoch) {
            // node may use the same epoch to open -> close -> open stream.
            // verify node
            RangeMetadata rangeMetadata = streamMetadata.ranges.get(streamMetadata.currentRangeIndex());
            if (rangeMetadata == null) {
                // should not happen
                log.error("[OpenStream]: stream {}'s current range {} not exist when open stream with epoch: {}", streamId,
                    streamMetadata.currentRangeIndex(), epoch);
                resp.setErrorCode(Errors.STREAM_INNER_ERROR.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            if (rangeMetadata.nodeId() != nodeId) {
                log.warn("[OpenStream]: stream {}'s current range {}'s node {} is not equal to request node {}",
                    streamId, streamMetadata.currentRangeIndex(), rangeMetadata.nodeId(), nodeId);
                resp.setErrorCode(Errors.STREAM_FENCED.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            // epoch equals, node equals, regard it as redundant open operation, just return success
            resp.setStartOffset(streamMetadata.startOffset());
            resp.setNextOffset(rangeMetadata.endOffset());
            List<ApiMessageAndVersion> records = new ArrayList<>();
            if (streamMetadata.currentState() == StreamState.CLOSED) {
                records.add(new ApiMessageAndVersion(new S3StreamRecord()
                    .setStreamId(streamId)
                    .setEpoch(epoch)
                    .setRangeIndex(streamMetadata.currentRangeIndex())
                    .setStartOffset(streamMetadata.startOffset())
                    .setStreamState(StreamState.OPENED.toByte()), (short) 0));
            }
            return ControllerResult.of(records, resp);
        }
        if (streamMetadata.currentState() == StreamState.OPENED) {
            // stream still in opened state, can't open until it is closed
            log.warn("[OpenStream]: stream {}'s state still is OPENED at epoch: {}", streamId, streamMetadata.currentEpoch());
            resp.setErrorCode(Errors.STREAM_NOT_CLOSED.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // now the request in valid, update the stream's epoch and create a new range for this node
        List<ApiMessageAndVersion> records = new ArrayList<>();
        int newRangeIndex = streamMetadata.currentRangeIndex() + 1;
        // stream update record
        records.add(new ApiMessageAndVersion(new S3StreamRecord()
            .setStreamId(streamId)
            .setEpoch(epoch)
            .setRangeIndex(newRangeIndex)
            .setStartOffset(streamMetadata.startOffset())
            .setStreamState(StreamState.OPENED.toByte()), (short) 0));
        // get new range's start offset
        // default regard this range is the first range in stream, use 0 as start offset
        long startOffset = 0;
        if (newRangeIndex > 0) {
            // means that the new range is not the first range in stream, get the last range's end offset
            RangeMetadata lastRangeMetadata = streamMetadata.ranges.get(streamMetadata.currentRangeIndex.get());
            startOffset = lastRangeMetadata.endOffset();
        }
        // range create record
        records.add(new ApiMessageAndVersion(new RangeRecord()
            .setStreamId(streamId)
            .setNodeId(nodeId)
            .setStartOffset(startOffset)
            .setEndOffset(startOffset)
            .setEpoch(epoch)
            .setRangeIndex(newRangeIndex), (short) 0));
        resp.setStartOffset(streamMetadata.startOffset());
        resp.setNextOffset(startOffset);
        log.info("[OpenStream]: node: {} open stream: {} with epoch: {} success", nodeId, streamId, epoch);
        return ControllerResult.atomicOf(records, resp);
    }

    /**
     * Close stream.
     * <p>
     * <b>Response Errors Enum:</b>
     * <ul>
     *     <li>
     *         <code>STREAM_FENCED</code>:
     *         <ol>
     *             <li> stream's epoch is larger than request epoch </li>
     *             <li> stream's current range's node is not equal to request node </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <code>STREAM_NOT_EXIST</code>
     *         <ol>
     *             <li> stream's id not exist in current stream-metadata </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <code>STREAM_INNER_ERROR</code>
     *         <ol>
     *             <li> stream's current range not exist when stream has been opened </li>
     *             <li> close stream with higher epoch </li>
     *         </ol>
     *     </li>
     * </ul>
     */
    public ControllerResult<CloseStreamResponse> closeStream(int nodeId, long nodeEpoch, CloseStreamRequest request) {
        CloseStreamResponse resp = new CloseStreamResponse();
        long streamId = request.streamId();
        long epoch = request.streamEpoch();

        // verify node epoch
        Errors nodeEpochCheckResult = nodeEpochCheck(nodeId, nodeEpoch);
        if (nodeEpochCheckResult != Errors.NONE) {
            resp.setErrorCode(nodeEpochCheckResult.code());
            log.warn("[CloseStream]: node: {}'s epoch: {} check failed, code: {}",
                nodeId, nodeEpoch, nodeEpochCheckResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        // verify ownership
        Errors authResult = streamOwnershipCheck(streamId, epoch, nodeId, "CloseStream");
        if (authResult != Errors.NONE) {
            resp.setErrorCode(authResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata.currentState() == StreamState.CLOSED) {
            // regard it as redundant close operation, just return success
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // now the request in valid, update the stream's state
        // stream update record
        List<ApiMessageAndVersion> records = List.of(
            new ApiMessageAndVersion(new S3StreamRecord()
                .setStreamId(streamId)
                .setEpoch(epoch)
                .setRangeIndex(streamMetadata.currentRangeIndex())
                .setStartOffset(streamMetadata.startOffset())
                .setStreamState(StreamState.CLOSED.toByte()), (short) 0));
        log.info("[CloseStream]: node: {} close stream: {} with epoch: {} success", nodeId, streamId, epoch);
        return ControllerResult.atomicOf(records, resp);
    }

    public ControllerResult<TrimStreamResponse> trimStream(int nodeId, long nodeEpoch, TrimStreamRequest request) {
        // TODO: speed up offset updating
        long epoch = request.streamEpoch();
        long streamId = request.streamId();
        long newStartOffset = request.newStartOffset();
        TrimStreamResponse resp = new TrimStreamResponse();

        // verify node epoch
        Errors nodeEpochCheckResult = nodeEpochCheck(nodeId, nodeEpoch);
        if (nodeEpochCheckResult != Errors.NONE) {
            resp.setErrorCode(nodeEpochCheckResult.code());
            log.warn("[TrimStream]: node: {}'s epoch: {} check failed, code: {}",
                nodeId, nodeEpoch, nodeEpochCheckResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        // verify ownership
        Errors authResult = streamOwnershipCheck(streamId, epoch, nodeId, "TrimStream");
        if (authResult != Errors.NONE) {
            resp.setErrorCode(authResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata.currentState() == StreamState.CLOSED) {
            log.warn("[TrimStream]: stream {}'s state is CLOSED, can't trim", streamId);
            resp.setErrorCode(Errors.STREAM_NOT_OPENED.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (streamMetadata.startOffset() > newStartOffset) {
            log.warn("[TrimStream]: stream {}'s start offset {} is larger than request new start offset {}",
                streamId, streamMetadata.startOffset(), newStartOffset);
            resp.setErrorCode(Errors.OFFSET_NOT_MATCHED.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (streamMetadata.startOffset() == newStartOffset) {
            // regard it as redundant trim operation, just return success
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // now the request is valid
        // update the stream metadata start offset
        List<ApiMessageAndVersion> records = new ArrayList<>();
        records.add(new ApiMessageAndVersion(new S3StreamRecord()
            .setStreamId(streamId)
            .setEpoch(epoch)
            .setRangeIndex(streamMetadata.currentRangeIndex())
            .setStartOffset(newStartOffset)
            .setStreamState(streamMetadata.currentState().toByte()), (short) 0));
        // remove range or update range's start offset
        streamMetadata.ranges.entrySet().stream().forEach(it -> {
            Integer rangeIndex = it.getKey();
            RangeMetadata range = it.getValue();
            if (newStartOffset <= range.startOffset()) {
                return;
            }
            if (rangeIndex == streamMetadata.currentRangeIndex()) {
                // current range, update start offset
                // if current range is [50, 100)
                // 1. try to trim to 60, then current range will be [60, 100)
                // 2. try to trim to 100, then current range will be [100, 100)
                // 3. try to trim to 110, then current range will be [100, 100)
                long newRangeStartOffset = newStartOffset < range.endOffset() ? newStartOffset : range.endOffset();
                records.add(new ApiMessageAndVersion(new RangeRecord()
                    .setStreamId(streamId)
                    .setRangeIndex(rangeIndex)
                    .setNodeId(range.nodeId())
                    .setEpoch(range.epoch())
                    .setStartOffset(newRangeStartOffset)
                    .setEndOffset(range.endOffset()), (short) 0));
                return;
            }
            if (newStartOffset >= range.endOffset()) {
                // remove range
                records.add(new ApiMessageAndVersion(new RemoveRangeRecord()
                    .setStreamId(streamId)
                    .setRangeIndex(rangeIndex), (short) 0));
                return;
            }
            // update range's start offset
            records.add(new ApiMessageAndVersion(new RangeRecord()
                .setStreamId(streamId)
                .setNodeId(range.nodeId())
                .setStartOffset(newStartOffset)
                .setEndOffset(range.endOffset())
                .setEpoch(range.epoch())
                .setRangeIndex(rangeIndex), (short) 0));
        });
        // remove stream object
        streamMetadata.streamObjects.entrySet().stream().forEach(it -> {
            Long objectId = it.getKey();
            S3StreamObject streamObject = it.getValue();
            long streamStartOffset = streamObject.streamOffsetRange().getStartOffset();
            long streamEndOffset = streamObject.streamOffsetRange().getEndOffset();
            if (newStartOffset <= streamStartOffset) {
                return;
            }
            if (newStartOffset >= streamEndOffset) {
                // remove stream object
                records.add(new ApiMessageAndVersion(new RemoveS3StreamObjectRecord()
                    .setStreamId(streamId)
                    .setObjectId(objectId), (short) 0));
                ControllerResult<Boolean> markDestroyResult = this.s3ObjectControlManager.markDestroyObjects(
                    Collections.singletonList(objectId));
                if (!markDestroyResult.response()) {
                    log.error("[TrimStream]: Mark destroy stream object: {} failed", objectId);
                    resp.setErrorCode(Errors.STREAM_INNER_ERROR.code());
                    return;
                }
                records.addAll(markDestroyResult.records());
            }
        });
        if (resp.errorCode() != Errors.NONE.code()) {
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        // remove wal object or remove stream range in wal object
        // TODO: optimize
        this.nodesMetadata.values()
            .stream()
            .flatMap(entry -> entry.walObjects.values().stream())
            .filter(walObject -> walObject.offsetRanges().containsKey(streamId))
            .filter(walObject -> walObject.offsetRanges().get(streamId).getEndOffset() <= newStartOffset)
            .forEach(walObj -> {
                if (walObj.offsetRanges().size() == 1) {
                    // only this range, but we will remove this range, so now we can remove this wal object
                    records.add(new ApiMessageAndVersion(
                        new RemoveWALObjectRecord()
                            .setNodeId(walObj.nodeId())
                            .setObjectId(walObj.objectId()), (short) 0
                    ));
                    ControllerResult<Boolean> markDestroyResult = this.s3ObjectControlManager.markDestroyObjects(
                        List.of(walObj.objectId()));
                    if (!markDestroyResult.response()) {
                        log.error("[TrimStream]: Mark destroy wal object: {} failed", walObj.objectId());
                        resp.setErrorCode(Errors.STREAM_INNER_ERROR.code());
                        return;
                    }
                    records.addAll(markDestroyResult.records());
                    return;
                }
                Map<Long, StreamOffsetRange> newOffsetRange = new HashMap<>(walObj.offsetRanges());
                // remove offset range
                newOffsetRange.remove(streamId);
                records.add(new ApiMessageAndVersion(new WALObjectRecord()
                    .setObjectId(walObj.objectId())
                    .setNodeId(walObj.nodeId())
                    .setStreamsIndex(newOffsetRange.values().stream().map(Convertor::to).collect(Collectors.toList()))
                    .setDataTimeInMs(walObj.dataTimeInMs())
                    .setOrderId(walObj.orderId()), (short) 0));
            });
        if (resp.errorCode() != Errors.NONE.code()) {
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        log.info("[TrimStream]: node: {} trim stream: {} to new start offset: {} with epoch: {} success", nodeId, streamId, newStartOffset, epoch);
        return ControllerResult.atomicOf(records, resp);
    }

    public ControllerResult<DeleteStreamResponse> deleteStream(int nodeId, long nodeEpoch, DeleteStreamRequest request) {
        long epoch = request.streamEpoch();
        long streamId = request.streamId();
        DeleteStreamResponse resp = new DeleteStreamResponse();

        // verify node epoch
        Errors nodeEpochCheckResult = nodeEpochCheck(nodeId, nodeEpoch);
        if (nodeEpochCheckResult != Errors.NONE) {
            resp.setErrorCode(nodeEpochCheckResult.code());
            log.warn("[DeleteStream]: node: {}'s epoch: {} check failed, code: {}",
                nodeId, nodeEpoch, nodeEpochCheckResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        // verify ownership
        Errors authResult = streamOwnershipCheck(streamId, epoch, nodeId, "DeleteStream");
        if (authResult != Errors.NONE) {
            resp.setErrorCode(authResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);

        // generate remove stream record
        List<ApiMessageAndVersion> records = new ArrayList<>();
        records.add(new ApiMessageAndVersion(new RemoveS3StreamRecord()
            .setStreamId(streamId), (short) 0));
        // generate stream objects destroy records
        List<Long> streamObjectIds = streamMetadata.streamObjects.keySet().stream().collect(Collectors.toList());
        ControllerResult<Boolean> markDestroyResult = this.s3ObjectControlManager.markDestroyObjects(streamObjectIds);
        if (!markDestroyResult.response()) {
            log.error("[DeleteStream]: Mark destroy stream objects: {} failed", streamObjectIds);
            resp.setErrorCode(Errors.STREAM_INNER_ERROR.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        records.addAll(markDestroyResult.records());
        // remove wal object or remove stream-offset-range in wal object
        this.nodesMetadata.values()
            .stream()
            .flatMap(entry -> entry.walObjects.values().stream())
            .filter(walObject -> walObject.offsetRanges().containsKey(streamId))
            .forEach(walObj -> {
                if (walObj.offsetRanges().size() == 1) {
                    // only this range, but we will remove this range, so now we can remove this wal object
                    records.add(new ApiMessageAndVersion(
                        new RemoveWALObjectRecord()
                            .setNodeId(walObj.nodeId())
                            .setObjectId(walObj.objectId()), (short) 0
                    ));
                    ControllerResult<Boolean> result = this.s3ObjectControlManager.markDestroyObjects(
                        List.of(walObj.objectId()));
                    if (!result.response()) {
                        log.error("[DeleteStream]: Mark destroy wal object: {} failed", walObj.objectId());
                        resp.setErrorCode(Errors.STREAM_INNER_ERROR.code());
                        return;
                    }
                    records.addAll(result.records());
                    return;
                }
                Map<Long, StreamOffsetRange> newOffsetRange = new HashMap<>(walObj.offsetRanges());
                // remove offset range
                newOffsetRange.remove(streamId);
                records.add(new ApiMessageAndVersion(new WALObjectRecord()
                    .setObjectId(walObj.objectId())
                    .setNodeId(walObj.nodeId())
                    .setStreamsIndex(newOffsetRange.values().stream().map(Convertor::to).collect(Collectors.toList()))
                    .setDataTimeInMs(walObj.dataTimeInMs())
                    .setOrderId(walObj.orderId()), (short) 0));
            });
        if (resp.errorCode() != Errors.NONE.code()) {
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        log.info("[DeleteStream]: node: {} delete stream: {} with epoch: {} success", nodeId, streamId, epoch);
        return ControllerResult.atomicOf(records, resp);
    }

    /**
     * Commit wal object.
     * <p>
     * <b>Response Errors Enum:</b>
     * <ul>
     *     <li>
     *         <code>OBJECT_NOT_EXIST</code>
     *         <ol>
     *             <li> wal object not exist when commit </li>
     *             <li> stream object not exist when commit </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <code>COMPACTED_OBJECTS_NOT_FOUND</code>
     *         <ol>
     *             <li> compacted objects not found when mark destroy </li>
     *         </ol>
     *     </li>
     * </ul>
     */
    @SuppressWarnings("all")
    public ControllerResult<CommitWALObjectResponseData> commitWALObject(CommitWALObjectRequestData data) {
        CommitWALObjectResponseData resp = new CommitWALObjectResponseData();
        long objectId = data.objectId();
        int nodeId = data.nodeId();
        long nodeEpoch = data.nodeEpoch();
        long objectSize = data.objectSize();
        long orderId = data.orderId();

        // verify node epoch
        Errors nodeEpochCheckResult = nodeEpochCheck(nodeId, nodeEpoch);
        if (nodeEpochCheckResult != Errors.NONE) {
            resp.setErrorCode(nodeEpochCheckResult.code());
            log.warn("[CommitWALObject]: node: {}'s epoch: {} check failed, code: {}",
                nodeId, nodeEpoch, nodeEpochCheckResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        List<ObjectStreamRange> streamRanges = data.objectStreamRanges();
        List<Long> compactedObjectIds = data.compactedObjectIds();
        List<StreamObject> streamObjects = data.streamObjects();
        long committedTs = System.currentTimeMillis();

        if (compactedObjectIds == null || compactedObjectIds.isEmpty()) {
            // verify stream continuity
            List<StreamOffsetRange> offsetRanges = Stream.concat(
                    streamRanges
                        .stream()
                        .map(range -> new StreamOffsetRange(range.streamId(), range.startOffset(), range.endOffset())),
                    streamObjects
                        .stream()
                        .map(obj -> new StreamOffsetRange(obj.streamId(), obj.startOffset(), obj.endOffset())))
                .collect(Collectors.toList());
            Errors continuityCheckResult = streamAdvanceCheck(offsetRanges);
            if (continuityCheckResult != Errors.NONE) {
                log.error("[CommitWALObject]: stream: {} advance check failed, error: {}", offsetRanges, continuityCheckResult);
                resp.setErrorCode(continuityCheckResult.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
        }

        // commit object
        ControllerResult<Errors> commitResult = this.s3ObjectControlManager.commitObject(objectId, objectSize, committedTs);
        if (commitResult.response() == Errors.OBJECT_NOT_EXIST) {
            log.error("[CommitWALObject]: object {} not exist when commit wal object", objectId);
            resp.setErrorCode(Errors.OBJECT_NOT_EXIST.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (commitResult.response() == Errors.REDUNDANT_OPERATION) {
            // regard it as redundant commit operation, just return success
            log.warn("[CommitWALObject]: object {} already committed", objectId);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        List<ApiMessageAndVersion> records = new ArrayList<>(commitResult.records());
        long dataTs = committedTs;
        // mark destroy compacted object
        if (compactedObjectIds != null && !compactedObjectIds.isEmpty()) {
            ControllerResult<Boolean> destroyResult = this.s3ObjectControlManager.markDestroyObjects(compactedObjectIds);
            if (!destroyResult.response()) {
                log.error("[CommitWALObject]: Mark destroy compacted objects: {} failed", compactedObjectIds);
                resp.setErrorCode(Errors.COMPACTED_OBJECTS_NOT_FOUND.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            records.addAll(destroyResult.records());
            // update dataTs to the min compacted object's dataTs
            //noinspection OptionalGetWithoutIsPresent
            dataTs = compactedObjectIds.stream()
                    .map(id -> this.nodesMetadata.get(nodeId).walObjects.get(id))
                    .map(S3WALObject::dataTimeInMs)
                    .min(Long::compareTo).get();
        }
        List<StreamOffsetRange> indexes = streamRanges.stream()
                .map(range -> new StreamOffsetRange(range.streamId(), range.startOffset(), range.endOffset()))
                .collect(Collectors.toList());
        // update node's wal object
        NodeS3WALMetadata nodeMetadata = this.nodesMetadata.get(nodeId);
        if (nodeMetadata == null) {
            // first time commit wal object, generate node's metadata record
            records.add(new ApiMessageAndVersion(new NodeWALMetadataRecord()
                    .setNodeId(nodeId), (short) 0));
        }
        if (objectId != NOOP_OBJECT_ID) {
            // generate node's wal object record
            List<StreamIndex> streamIndexes = indexes.stream()
                    .map(Convertor::to)
                    .collect(Collectors.toList());
            WALObjectRecord walObjectRecord = new WALObjectRecord()
                    .setObjectId(objectId)
                    .setDataTimeInMs(dataTs)
                    .setOrderId(orderId)
                    .setNodeId(nodeId)
                    .setStreamsIndex(streamIndexes);
            records.add(new ApiMessageAndVersion(walObjectRecord, (short) 0));
        }
        // commit stream objects
        if (streamObjects != null && !streamObjects.isEmpty()) {
            // commit objects
            for (StreamObject streamObject : streamObjects) {
                ControllerResult<Errors> streamObjectCommitResult = this.s3ObjectControlManager.commitObject(streamObject.objectId(),
                        streamObject.objectSize(), committedTs);
                if (streamObjectCommitResult.response() != Errors.NONE) {
                    log.error("[CommitWALObject]: stream object: {} not exist when commit wal object: {}", streamObject.objectId(), objectId);
                    resp.setErrorCode(streamObjectCommitResult.response().code());
                    return ControllerResult.of(Collections.emptyList(), resp);
                }
                records.addAll(streamObjectCommitResult.records());
            }
            // create stream object records
            streamObjects.forEach(obj -> {
                long streamId = obj.streamId();
                long startOffset = obj.startOffset();
                long endOffset = obj.endOffset();
                records.add(new S3StreamObject(obj.objectId(), streamId, startOffset, endOffset, committedTs).toRecord());
            });
        }
        // generate compacted objects' remove record
        if (compactedObjectIds != null && !compactedObjectIds.isEmpty()) {
            compactedObjectIds.forEach(id -> records.add(new ApiMessageAndVersion(new RemoveWALObjectRecord()
                    .setNodeId(nodeId)
                    .setObjectId(id), (short) 0)));
        }
        log.info("[CommitWALObject]: node: {} commit wal object: {} success, compacted objects: {}, stream objects: {}", nodeId, objectId,
                compactedObjectIds, streamObjects);
        return ControllerResult.atomicOf(records, resp);
    }

    /**
     * Commit stream object.
     * <p>
     * <b>Response Errors Enum:</b>
     * <ul>
     *     <li>
     *         <code>OBJECT_NOT_EXIST</code>
     *         <ol>
     *             <li> stream object not exist when commit </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <code>COMPACTED_OBJECTS_NOT_FOUND</code>
     *         <ol>
     *             <li> compacted objects not found when mark destroy </li>
     *         </ol>
     *     </li>
     * </ul>
     */
    public ControllerResult<CommitStreamObjectResponseData> commitStreamObject(CommitStreamObjectRequestData data) {
        int nodeId = data.nodeId();
        long nodeEpoch = data.nodeEpoch();
        long streamObjectId = data.objectId();
        long streamId = data.streamId();
        long startOffset = data.startOffset();
        long endOffset = data.endOffset();
        long objectSize = data.objectSize();
        List<Long> sourceObjectIds = data.sourceObjectIds();
        CommitStreamObjectResponseData resp = new CommitStreamObjectResponseData();
        long committedTs = System.currentTimeMillis();

        // verify node epoch
        Errors nodeEpochCheckResult = nodeEpochCheck(nodeId, nodeEpoch);
        if (nodeEpochCheckResult != Errors.NONE) {
            resp.setErrorCode(nodeEpochCheckResult.code());
            log.warn("[CommitStreamObject]: node: {}'s epoch: {} check failed, code: {}",
                nodeId, nodeEpoch, nodeEpochCheckResult.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }

        // commit object
        ControllerResult<Errors> commitResult = this.s3ObjectControlManager.commitObject(streamObjectId, objectSize, committedTs);
        if (commitResult.response() == Errors.OBJECT_NOT_EXIST) {
            log.error("[CommitStreamObject]: object {} not exist when commit stream object", streamObjectId);
            resp.setErrorCode(Errors.OBJECT_NOT_EXIST.code());
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        if (commitResult.response() == Errors.REDUNDANT_OPERATION) {
            // regard it as redundant commit operation, just return success
            log.warn("[CommitStreamObject]: object {} already committed", streamObjectId);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        List<ApiMessageAndVersion> records = new ArrayList<>(commitResult.records());

        long dataTs = committedTs;
        // mark destroy compacted object
        if (sourceObjectIds != null && !sourceObjectIds.isEmpty()) {
            ControllerResult<Boolean> destroyResult = this.s3ObjectControlManager.markDestroyObjects(sourceObjectIds);
            if (!destroyResult.response()) {
                log.error("[CommitStreamObject]: Mark destroy compacted objects: {} failed", sourceObjectIds);
                resp.setErrorCode(Errors.COMPACTED_OBJECTS_NOT_FOUND.code());
                return ControllerResult.of(Collections.emptyList(), resp);
            }
            records.addAll(destroyResult.records());
            // update dataTs to the min compacted object's dataTs
            //noinspection OptionalGetWithoutIsPresent
            dataTs = sourceObjectIds.stream()
                    .map(id -> this.streamsMetadata.get(streamId).streamObjects.get(id))
                    .map(S3StreamObject::dataTimeInMs)
                    .min(Long::compareTo).get();
        }

        // generate stream object record
        records.add(new ApiMessageAndVersion(new S3StreamObjectRecord()
                .setObjectId(streamObjectId)
                .setStreamId(streamId)
                .setStartOffset(startOffset)
                .setEndOffset(endOffset)
                .setDataTimeInMs(dataTs), (short) 0));

        // generate compacted objects' remove record
        if (sourceObjectIds != null && !sourceObjectIds.isEmpty()) {
            sourceObjectIds.forEach(id -> records.add(new ApiMessageAndVersion(new RemoveS3StreamObjectRecord()
                    .setObjectId(id)
                    .setStreamId(streamId), (short) 0)));
        }
        log.info("[CommitStreamObject]: stream object: {} commit success, compacted objects: {}", streamObjectId, sourceObjectIds);
        return ControllerResult.atomicOf(records, resp);
    }

    public ControllerResult<GetOpeningStreamsResponseData> getOpeningStreams(GetOpeningStreamsRequestData data) {
        GetOpeningStreamsResponseData resp = new GetOpeningStreamsResponseData();
        int nodeId = data.nodeId();
        long nodeEpoch = data.nodeEpoch();

        // verify and update node epoch
        if (nodesMetadata.containsKey(nodeId) && nodeEpoch < nodesMetadata.get(nodeId).getNodeEpoch()) {
            // node epoch has been expired
            resp.setErrorCode(Errors.NODE_EPOCH_EXPIRED.code());
            log.warn("[GetOpeningStreams]: node: {}'s epoch: {} has been expired", nodeId, nodeEpoch);
            return ControllerResult.of(Collections.emptyList(), resp);
        }
        List<ApiMessageAndVersion> records = new ArrayList<>();
        // update node epoch
        records.add(new ApiMessageAndVersion(new NodeWALMetadataRecord()
            .setNodeId(nodeId)
            .setNodeEpoch(nodeEpoch), (short) 0));

        // The getOpeningStreams is invoked when node startup, so we just iterate all streams to get the node opening streams.
        List<StreamMetadata> streamStatusList = this.streamsMetadata.entrySet().stream().filter(entry -> {
            S3StreamMetadata streamMetadata = entry.getValue();
            if (!StreamState.OPENED.equals(streamMetadata.currentState.get())) {
                return false;
            }
            int rangeIndex = streamMetadata.currentRangeIndex.get();
            if (rangeIndex < 0) {
                return false;
            }
            RangeMetadata rangeMetadata = streamMetadata.ranges.get(rangeIndex);
            return rangeMetadata.nodeId() == nodeId;
        }).map(e -> {
            S3StreamMetadata streamMetadata = e.getValue();
            RangeMetadata rangeMetadata = streamMetadata.ranges.get(streamMetadata.currentRangeIndex.get());
            return new StreamMetadata()
                    .setStreamId(e.getKey())
                    .setEpoch(streamMetadata.currentEpoch.get())
                    .setStartOffset(streamMetadata.startOffset.get())
                    .setEndOffset(rangeMetadata.endOffset());
        }).collect(Collectors.toList());
        resp.setStreamMetadataList(streamStatusList);
        return ControllerResult.atomicOf(records, resp);
    }


    // private check methods

    /**
     * Check whether this node is the owner of this stream.
     */
    private Errors streamOwnershipCheck(long streamId, long epoch, int nodeId, String operationName) {
        if (!this.streamsMetadata.containsKey(streamId)) {
            log.warn("[{}]: stream {} not exist", operationName, streamId);
            return Errors.STREAM_NOT_EXIST;
        }
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata.currentEpoch() > epoch) {
            log.warn("[{}]: stream {}'s epoch {} is larger than request epoch {}", operationName, streamId,
                streamMetadata.currentEpoch.get(), epoch);
            return Errors.STREAM_FENCED;
        }
        if (streamMetadata.currentEpoch() < epoch) {
            // should not happen
            log.error("[{}]: stream {}'s epoch {} is smaller than request epoch {}", operationName, streamId,
                streamMetadata.currentEpoch.get(), epoch);
            return Errors.STREAM_INNER_ERROR;
        }
        // verify node
        RangeMetadata rangeMetadata = streamMetadata.ranges.get(streamMetadata.currentRangeIndex());
        if (rangeMetadata == null) {
            // should not happen
            log.error("[{}]: stream {}'s current range {} not exist when trim stream with epoch: {}", operationName, streamId,
                streamMetadata.currentRangeIndex(), epoch);
            return Errors.STREAM_INNER_ERROR;
        }
        if (rangeMetadata.nodeId() != nodeId) {
            log.warn("[{}]: stream {}'s current range {}'s node {} is not equal to request node {}", operationName,
                streamId, streamMetadata.currentRangeIndex(), rangeMetadata.nodeId(), nodeId);
            return Errors.STREAM_FENCED;
        }
        return Errors.NONE;
    }

    /**
     * Check whether these stream-offset-ranges can be committed to advance the streams' offset
     * <p>
     * <b>Check List:</b>
     * <ul>
     *      <li>
     *          <code>Stream Exist Check</code>
     *      </li>
     *      <li>
     *          <code>Stream Open Check</code>
     *      </li>
     *      <li>
     *          <code>Stream Continuity Check</code>
     *      </li>
     * <ul/>
     */
    private Errors streamAdvanceCheck(List<StreamOffsetRange> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            return Errors.NONE;
        }
        for (StreamOffsetRange range : ranges) {
            // verify stream exist
            if (!this.streamsMetadata.containsKey(range.getStreamId())) {
                log.warn("[streamAdvanceCheck]: stream {} not exist", range.getStreamId());
                return Errors.STREAM_NOT_EXIST;
            }
            // check if this stream open
            if (this.streamsMetadata.get(range.getStreamId()).currentState() != StreamState.OPENED) {
                log.warn("[streamAdvanceCheck]: stream {} not opened", range.getStreamId());
                return Errors.STREAM_NOT_OPENED;
            }
            RangeMetadata rangeMetadata = this.streamsMetadata.get(range.getStreamId()).currentRangeMetadata();
            if (rangeMetadata == null) {
                // should not happen
                log.error("[streamAdvanceCheck]: stream {}'s current range {} not exist when stream has been ",
                    range.getStreamId(), this.streamsMetadata.get(range.getStreamId()).currentRangeIndex());
                return Errors.STREAM_INNER_ERROR;
            }
            if (rangeMetadata.endOffset() != range.getStartOffset()) {
                log.warn("[streamAdvanceCheck]: stream {}'s current range {}'s end offset {} is not equal to request start offset {}",
                    range.getStreamId(), this.streamsMetadata.get(range.getStreamId()).currentRangeIndex(),
                    rangeMetadata.endOffset(), range.getStartOffset());
                return Errors.OFFSET_NOT_MATCHED;
            }
        }
        return Errors.NONE;
    }

    /**
     * Check whether this node is valid to operate the stream related resources.
     */
    private Errors nodeEpochCheck(int nodeId, long nodeEpoch) {
        if (!this.nodesMetadata.containsKey(nodeId)) {
            // should not happen
            log.error("[NodeEpochCheck]: node {} not exist when check node epoch", nodeId);
            return Errors.NODE_EPOCH_NOT_EXIST;
        }
        if (this.nodesMetadata.get(nodeId).getNodeEpoch() > nodeEpoch) {
            log.warn("[NodeEpochCheck]: node {}'s epoch {} is larger than request epoch {}", nodeId,
                this.nodesMetadata.get(nodeId).getNodeEpoch(), nodeEpoch);
            return Errors.NODE_EPOCH_EXPIRED;
        }
        return Errors.NONE;
    }

    public void replay(AssignedStreamIdRecord record) {
        this.nextAssignedStreamId.set(record.assignedStreamId() + 1);
    }

    public void replay(S3StreamRecord record) {
        long streamId = record.streamId();
        // already exist, update the stream's self metadata
        if (this.streamsMetadata.containsKey(streamId)) {
            S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
            streamMetadata.startOffset.set(record.startOffset());
            streamMetadata.currentEpoch.set(record.epoch());
            streamMetadata.currentRangeIndex.set(record.rangeIndex());
            streamMetadata.currentState.set(StreamState.fromByte(record.streamState()));
            return;
        }
        // not exist, create a new stream
        S3StreamMetadata streamMetadata = new S3StreamMetadata(record.epoch(), record.rangeIndex(),
                record.startOffset(), StreamState.fromByte(record.streamState()), this.snapshotRegistry);
        this.streamsMetadata.put(streamId, streamMetadata);
    }

    public void replay(RemoveS3StreamRecord record) {
        long streamId = record.streamId();
        this.streamsMetadata.remove(streamId);
    }

    public void replay(RangeRecord record) {
        long streamId = record.streamId();
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            // should not happen
            log.error("stream {} not exist when replay range record {}", streamId, record);
            return;
        }
        streamMetadata.ranges.put(record.rangeIndex(), RangeMetadata.of(record));
    }

    public void replay(RemoveRangeRecord record) {
        long streamId = record.streamId();
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            // should not happen
            log.error("stream {} not exist when replay remove range record {}", streamId, record);
            return;
        }
        streamMetadata.ranges.remove(record.rangeIndex());
    }

    public void replay(NodeWALMetadataRecord record) {
        int nodeId = record.nodeId();
        long nodeEpoch = record.nodeEpoch();
        // already exist, update the node's self metadata
        if (this.nodesMetadata.containsKey(nodeId)) {
            NodeS3WALMetadata nodeMetadata = this.nodesMetadata.get(nodeId);
            nodeMetadata.nodeEpoch.set(nodeEpoch);
            return;
        }
        // not exist, create a new node
        this.nodesMetadata.put(nodeId, new NodeS3WALMetadata(nodeId, nodeEpoch, this.snapshotRegistry));
    }

    public void replay(WALObjectRecord record) {
        long objectId = record.objectId();
        int nodeId = record.nodeId();
        long orderId = record.orderId();
        long dataTs = record.dataTimeInMs();
        List<StreamIndex> streamIndexes = record.streamsIndex();
        NodeS3WALMetadata nodeMetadata = this.nodesMetadata.get(nodeId);
        if (nodeMetadata == null) {
            // should not happen
            log.error("node {} not exist when replay wal object record {}", nodeId, record);
            return;
        }

        // create wal object
        Map<Long, StreamOffsetRange> indexMap = streamIndexes
                .stream()
                .collect(Collectors.toMap(StreamIndex::streamId, Convertor::to));
        nodeMetadata.walObjects.put(objectId, new S3WALObject(objectId, nodeId, indexMap, orderId, dataTs));

        // update range
        record.streamsIndex().forEach(index -> {
            long streamId = index.streamId();
            S3StreamMetadata metadata = this.streamsMetadata.get(streamId);
            if (metadata == null) {
                // ignore it
                LOGGER.error("[REPLAY_WAL_FAIL] cannot find {} metadata", streamId);
                return;
            }
            RangeMetadata rangeMetadata = metadata.currentRangeMetadata();
            if (rangeMetadata == null) {
                // ignore it
                LOGGER.error("[REPLAY_WAL_FAIL] cannot find {} stream range metadata", streamId);
                return;
            }
            if (rangeMetadata.endOffset() < index.startOffset()) {
                LOGGER.error("[REPLAY_WAL_FAIL] stream {} offset is not continuous, expect {} real {}", streamId,
                        rangeMetadata.endOffset(), index.startOffset());
                return;
            } else if (rangeMetadata.endOffset() > index.startOffset()) {
                // ignore it, the WAL object is the compacted WAL object.
                return;
            }
            rangeMetadata.setEndOffset(index.endOffset());
        });
    }

    public void replay(RemoveWALObjectRecord record) {
        long objectId = record.objectId();
        NodeS3WALMetadata walMetadata = this.nodesMetadata.get(record.nodeId());
        if (walMetadata == null) {
            // should not happen
            log.error("node {} not exist when replay remove wal object record {}", record.nodeId(), record);
            return;
        }
        walMetadata.walObjects.remove(objectId);
    }

    public void replay(S3StreamObjectRecord record) {
        long objectId = record.objectId();
        long streamId = record.streamId();
        long startOffset = record.startOffset();
        long endOffset = record.endOffset();
        long dataTs = record.dataTimeInMs();

        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            // should not happen
            log.error("stream {} not exist when replay stream object record {}", streamId, record);
            return;
        }
        streamMetadata.streamObjects.put(objectId, new S3StreamObject(objectId, streamId, startOffset, endOffset, dataTs));
        // update range
        RangeMetadata rangeMetadata = streamMetadata.currentRangeMetadata();
        if (rangeMetadata == null) {
            LOGGER.error("[REPLAY_WAL_FAIL] cannot find {} stream range metadata", streamId);
            return;
        }
        if (rangeMetadata.endOffset() < startOffset) {
            LOGGER.error("[REPLAY_WAL_FAIL] stream {} offset is not continuous, expect {} real {}", streamId,
                    rangeMetadata.endOffset(), startOffset);
            return;
        } else if (rangeMetadata.endOffset() > startOffset) {
            // ignore it, the WAL object compact and stream compact may generate this StreamObjectRecord.
            return;
        }
        rangeMetadata.setEndOffset(endOffset);
    }

    public void replay(RemoveS3StreamObjectRecord record) {
        long streamId = record.streamId();
        long objectId = record.objectId();
        S3StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            // should not happen
            log.error("stream {} not exist when replay remove stream object record {}", streamId, record);
            return;
        }
        streamMetadata.streamObjects.remove(objectId);
    }

    public void replay(RemoveNodeWALMetadataRecord record) {
        int nodeId = record.nodeId();
        this.nodesMetadata.remove(nodeId);
    }


    public Map<Long, S3StreamMetadata> streamsMetadata() {
        return streamsMetadata;
    }

    public Map<Integer, NodeS3WALMetadata> nodesMetadata() {
        return nodesMetadata;
    }

    public Long nextAssignedStreamId() {
        return nextAssignedStreamId.get();
    }

    @Override
    public String toString() {
        return "StreamControlManager{" +
                "snapshotRegistry=" + snapshotRegistry +
                ", s3ObjectControlManager=" + s3ObjectControlManager +
                ", streamsMetadata=" + streamsMetadata +
                ", nodesMetadata=" + nodesMetadata +
                '}';
    }
}