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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MemoryChecker {
    private static final Logger log = LoggerFactory.getLogger(MemoryChecker.class);

    private final double memoryLimitRatio;

    private final RedisBacklogEventBuffer redisBacklogEventBuffer;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public MemoryChecker(RedisBacklogEventBuffer redisBacklogEventBuffer, double ratio) {
        log.info("Initializing memory checker...");

        this.redisBacklogEventBuffer = redisBacklogEventBuffer;
        this.memoryLimitRatio = ratio;

        scheduledExecutorService.scheduleAtFixedRate(
          this::checkMemoryRatio,
          0,
          60,
          TimeUnit.SECONDS);

        log.info("Memory checker started.");
    }

    public void start() {
        scheduledExecutorService.scheduleAtFixedRate(
          this::checkMemoryRatio,
          0,
          60,
          TimeUnit.SECONDS);
    }

    public void stop() {
        log.info("Stopping memory ratio checker...");
        scheduledExecutorService.shutdown();
        log.info("Memory ratio checker stopped");
    }

    private void checkMemoryRatio() {
        // Maximum amount of memory that Java virtual machine will attempt to use
        long javaMaxMemoryToUse = Runtime.getRuntime().maxMemory();

        long javaAllocatedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long javaAvailableMemory = javaMaxMemoryToUse - javaAllocatedMemory;
        double memoryRatio = 1.0 - javaAvailableMemory * 1.0 / javaMaxMemoryToUse;

        log.debug("Checking memory ratio: {} bytes", memoryRatio);

        this.redisBacklogEventBuffer.setShouldEvict(memoryRatio > memoryLimitRatio);
    }

}
