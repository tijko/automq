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

package kafka.log.stream;

import kafka.log.stream.api.RecordBatch;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class DefaultRecordBatch implements RecordBatch {
    private final int count;
    private final long baseTimestamp;
    private final Map<String, String> properties;
    private final ByteBuffer rawPayload;

    public DefaultRecordBatch(int count, long baseTimestamp, Map<String, String> properties, ByteBuffer rawPayload) {
        this.count = count;
        this.baseTimestamp = baseTimestamp;
        this.properties = properties;
        this.rawPayload = rawPayload;
    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public long baseTimestamp() {
        return baseTimestamp;
    }

    @Override
    public Map<String, String> properties() {
        if (properties == null) {
            return Collections.emptyMap();
        }
        return properties;
    }

    @Override
    public ByteBuffer rawPayload() {
        return rawPayload.duplicate();
    }
}