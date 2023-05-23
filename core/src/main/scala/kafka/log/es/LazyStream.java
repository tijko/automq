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

package kafka.log.es;

import sdk.elastic.stream.api.AppendResult;
import sdk.elastic.stream.api.CreateStreamOptions;
import sdk.elastic.stream.api.FetchResult;
import sdk.elastic.stream.api.OpenStreamOptions;
import sdk.elastic.stream.api.RecordBatch;
import sdk.elastic.stream.api.Stream;
import sdk.elastic.stream.api.StreamClient;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Lazy stream, create stream when append record.
 */
public class LazyStream implements Stream {
    public static final long NOOP_STREAM_ID = -1L;
    private static final Stream NOOP_STREAM = new NoopStream();
    private final String name;
    private final StreamClient client;
    private volatile Stream inner = NOOP_STREAM;
    private ElasticEventListener eventListener;

    public LazyStream(String name, long streamId, StreamClient client) throws ExecutionException, InterruptedException {
        this.name = name;
        this.client = client;
        if (streamId != NOOP_STREAM_ID) {
            // open exist stream
            inner = client.openStream(streamId, OpenStreamOptions.newBuilder().build()).get();
        }
    }

    @Override
    public long streamId() {
        return inner.streamId();
    }

    @Override
    public long startOffset() {
        return inner.startOffset();
    }

    @Override
    public long nextOffset() {
        return inner.nextOffset();
    }

    @Override
    public synchronized CompletableFuture<AppendResult> append(RecordBatch recordBatch) {
        if (this.inner == NOOP_STREAM) {
            try {
                // TODO: keep retry or fail all succeed request.
                // TODO: replica count
                this.inner = client.createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(1).build()).get();
                notifyListener(ElasticMetaEvent.STREAM_DO_CREATE);
            } catch (Throwable e) {
                return FutureUtil.failedFuture(new IOException(e));
            }
        }
        return inner.append(recordBatch);
    }

    @Override
    public CompletableFuture<Void> trim(long newStartOffset) {
        return inner.trim(newStartOffset);
    }

    @Override
    public CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxBytesHint) {
        return inner.fetch(startOffset, endOffset, maxBytesHint);
    }

    @Override
    public CompletableFuture<Void> close() {
        return inner.close();
    }

    @Override
    public CompletableFuture<Void> destroy() {
        return inner.destroy();
    }

    @Override
    public String toString() {
        return "LazyStream{name='" + name + '\'' + '}';
    }

    public void setListener(ElasticEventListener listener) {
        this.eventListener = listener;
    }

    public void notifyListener(ElasticMetaEvent event) {
        try {
            Optional.ofNullable(eventListener).ifPresent(listener -> listener.onEvent(inner.streamId(), event));
        } catch (Throwable e) {
            //TODO: log unexpected exception
        }
    }

    static class NoopStream implements Stream {
        @Override
        public long streamId() {
            return NOOP_STREAM_ID;
        }

        @Override
        public long startOffset() {
            return 0;
        }

        @Override
        public long nextOffset() {
            return 0;
        }

        @Override
        public CompletableFuture<AppendResult> append(RecordBatch recordBatch) {
            return FutureUtil.failedFuture(new UnsupportedOperationException("noop stream"));
        }

        @Override
        public CompletableFuture<Void> trim(long newStartOffset) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxBytesHint) {
            return CompletableFuture.completedFuture(Collections::emptyList);
        }

        @Override
        public CompletableFuture<Void> close() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> destroy() {
            return CompletableFuture.completedFuture(null);
        }
    }
}