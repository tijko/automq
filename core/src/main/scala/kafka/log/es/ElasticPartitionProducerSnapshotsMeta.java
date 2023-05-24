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

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * refers to all valid snapshot files
 */
public class ElasticPartitionProducerSnapshotsMeta {
    public static final ElasticPartitionProducerSnapshotsMeta EMPTY = new ElasticPartitionProducerSnapshotsMeta(new HashSet<>());
    private Set<Long> snapshots;

    public ElasticPartitionProducerSnapshotsMeta(Set<Long> snapshots) {
        this.snapshots = snapshots;
    }

    public Set<Long> getSnapshots() {
        return snapshots;
    }

    public void remove(Long offset) {
        snapshots.remove(offset);
    }

    public void add(Long offset) {
        snapshots.add(offset);
    }

    public ByteBuffer encode() {
        ByteBuffer buffer = ByteBuffer.allocate(snapshots.size() * 8);
        snapshots.forEach(item -> {
            if (item != null) {
                buffer.putLong(item);
            }
        });
        buffer.flip();
        return buffer;
    }

    public static ElasticPartitionProducerSnapshotsMeta decode(ByteBuffer buffer) {
        Set<Long> snapshots = new HashSet<>();
        while (buffer.hasRemaining()) {
            snapshots.add(buffer.getLong());
        }
        return new ElasticPartitionProducerSnapshotsMeta(snapshots);
    }

    public boolean isEmpty() {
        return snapshots.isEmpty();
    }
}
