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


import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

public class RedisSourceTaskConfig extends AbstractConfig {
    public static final String REDIS_HOST = "redis.host";
    public static final String REDIS_PORT = "redis.port";
    public static final String REDIS_MAX_EVENTS_IN_MEMORY = "redis.max_events_in_memory";
    public static final String REDIS_MEMORY_RATIO = "redis.memory_ratio";
    public static final String REDIS_CACHE_EVENTS_IN_FILE = "redis.event_cache_file";
    public static final String REDIS_USE_PSYNC2 = "redis.use_psync2";
    public static final String REDIS_TOPIC = "redis.topic";

    public static final String SOURCE_PARTITION_KEY = "partition";
    public static final String SOURCE_PARTITION_VALUE = "redis";
    public static final String REDIS_OFFSET_KEY = "offset";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
              REDIS_HOST,
              ConfigDef.Type.STRING,
              "localhost",
              ConfigDef.Importance.HIGH,
              "Redis server host (default: localhost)"
            )
            .define(
              REDIS_PORT,
              ConfigDef.Type.INT,
              6379,
              ConfigDef.Importance.HIGH,
              "Redis server port (default: 6379)"
            )
            .define(
              REDIS_TOPIC,
              ConfigDef.Type.STRING,
              "redisEvents",
              ConfigDef.Importance.HIGH,
              "Kafka topic where write Redis database events"
            )
            .define(
              REDIS_MAX_EVENTS_IN_MEMORY,
              ConfigDef.Type.LONG,
              1024L,
              ConfigDef.Importance.HIGH,
              "Max Redis events in memory"
            )
            .define(
              REDIS_MEMORY_RATIO,
              ConfigDef.Type.DOUBLE,
              0.5,
              ConfigDef.Importance.HIGH,
              "Memory ratio limit"
            )
            .define(
              REDIS_CACHE_EVENTS_IN_FILE,
              ConfigDef.Type.STRING,
              "events",
              ConfigDef.Importance.HIGH,
              "Path where persist Redis events"
            )
            .define(
              REDIS_USE_PSYNC2,
              ConfigDef.Type.BOOLEAN,
              false,
              ConfigDef.Importance.HIGH,
              "Whether use Psync 2 introduced by redis 4.0"
            );

    public RedisSourceTaskConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    public Map<String, String> defaultsStrings() {
        Map<String, String> copy = new HashMap<>();
        for (Map.Entry<String, ?> entry : values().entrySet()) {
            copy.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        return copy;
    }
}
