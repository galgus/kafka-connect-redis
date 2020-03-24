package net.galgus.kafka.connect.redis.source;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moilioncircle.redis.replicator.event.Event;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class RedisSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(RedisSourceTask.class);

    private RedisBacklogEventBuffer redisBacklogEventBuffer;
    private MemoryChecker memoryChecker;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private String kafkaOutputTopic;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(final Map<String, String> props) {
        log.info("Start an Redis Source Task");

        final Map<String, Object> configuration = RedisSourceTaskConfig.CONFIG_DEF.parse(props);

        long maxInMemoryEventsSize = (long) configuration.get(RedisSourceTaskConfig.REDIS_MAX_EVENTS_IN_MEMORY);
        double memoryRatio = (double) configuration.get(RedisSourceTaskConfig.REDIS_MEMORY_RATIO);
        String cachedEventsInFile = (String) configuration.get(RedisSourceTaskConfig.REDIS_CACHE_EVENTS_IN_FILE);
        kafkaOutputTopic = (String) configuration.get(RedisSourceTaskConfig.REDIS_TOPIC);

        redisBacklogEventBuffer = new RedisBacklogEventBuffer(maxInMemoryEventsSize, cachedEventsInFile);

        memoryChecker = new MemoryChecker(redisBacklogEventBuffer, memoryRatio);
        memoryChecker.start();

        final RedisPartialSyncWorker psyncWorker = new RedisPartialSyncWorker(redisBacklogEventBuffer, props);
        final Thread workerThread = new Thread(psyncWorker);
        workerThread.start();
    }

    @Override
    public List<SourceRecord> poll() {
        return redisBacklogEventBuffer
            .getAvailableEvents()
            .stream()
            .filter(Objects::nonNull)
            .map(this::getSourceRecord)
            .collect(Collectors.toList());
    }

    SourceRecord getSourceRecord(final Event event) {
        SourceRecord record = null;
        final Map<String, String> partition = Collections.singletonMap(RedisSourceTaskConfig.SOURCE_PARTITION_KEY, RedisSourceTaskConfig.SOURCE_PARTITION_VALUE);
        final SchemaBuilder bytesSchema = SchemaBuilder.bytes();

        // Redis backlog has no offset or timestamp
        final Timestamp ts = new Timestamp(System.currentTimeMillis()); // avoid invalid timestamp exception
        final long timestamp = ts.getTime();

        // set timestamp as offset
        final Map<String, ?> offset = Collections.singletonMap(RedisSourceTaskConfig.REDIS_OFFSET_KEY, timestamp);
        try {
            final String cmd = objectMapper.writeValueAsString(event);
            record = new SourceRecord(
              partition,
              offset,
              this.kafkaOutputTopic,
              null,
              bytesSchema,
              event.getClass().getName().getBytes(),
              null,
              cmd,
              timestamp
            );
        } catch (final JsonProcessingException e) {
            log.error("Error converting event to JSON", e);
        }
        return record;
    }

    @Override
    public void stop() {
        log.info("Stopping Redis source task...");

        this.redisBacklogEventBuffer.stop();
        this.memoryChecker.stop();

        log.info("Redis source task stopped");
    }
}
