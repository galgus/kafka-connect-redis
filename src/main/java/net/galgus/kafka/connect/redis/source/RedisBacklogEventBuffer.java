package net.galgus.kafka.connect.redis.source;

import com.moilioncircle.redis.replicator.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

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

public class RedisBacklogEventBuffer {
    private static final Logger log = LoggerFactory.getLogger(RedisBacklogEventBuffer.class);

    private final ConcurrentLinkedQueue<Event> eventConcurrentLinkedQueue;

    private final String eventCacheFileName;
    private File eventsCacheFile;

    private ObjectInputStream objectInputStream;
    private ObjectOutputStream objectOutputStream;

    private final long maxInMemoryEvents;

    private int eventsInMemory = 0;
    private int cachedEventsInFile = 0;
    private boolean shouldEvict = false;

    private enum State {
        RUNNING, STOPPED
    }

    private State currentState = State.RUNNING;

    public RedisBacklogEventBuffer(long maxInMemoryEvents, String eventsCacheFileName) {
        log.debug("Creating new instance");
        this.eventConcurrentLinkedQueue = new ConcurrentLinkedQueue<>();
        this.maxInMemoryEvents = maxInMemoryEvents;
        this.eventCacheFileName = eventsCacheFileName;
        this.eventsCacheFile = new File(eventCacheFileName);

        try {
            objectOutputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(eventsCacheFile)));
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    public void put(Event event) {
        log.debug("Adding new event to buffer");

        if (currentState.equals(State.RUNNING)) {
            if (event != null) {

                if (shouldEvict || (eventsInMemory >= maxInMemoryEvents)) {
                    log.debug("Max in memory events reached. Persisting event in file");
                    persistInFile(event);
                    cachedEventsInFile++;
                } else { // Add to memory buffer
                    eventConcurrentLinkedQueue.offer(event);
                    eventsInMemory++;
                }

            } else {
                log.warn("Attempt to put a null event in buffer. Rejecting event.");
            }
        } else {
            log.warn("Attempt to put an event in buffer with stopped state. Rejecting event.");
        }
    }

    private void persistInFile(Event event) {
        if(objectOutputStream == null || !eventsCacheFile.canWrite()) {
            try {
                Files.delete(Paths.get(eventCacheFileName));
                eventsCacheFile = new File(eventCacheFileName);
                objectOutputStream = new ObjectOutputStream(
                  new BufferedOutputStream(
                    new FileOutputStream(eventsCacheFile)
                  )
                );
            } catch (IOException pE) {
                log.error("Error to delete '{}': {}", eventCacheFileName, pE);
            }

            if (objectOutputStream != null) {
                try {
                    objectOutputStream.writeObject(event);
                } catch (IOException pE) {
                    log.error("Fail to write in '{}': {}", eventCacheFileName, pE);
                }
            } else {
                log.error("Error to write in '{}': objectOutputSTream is null", eventCacheFileName);
            }
        }
    }

    public List<Event> getAvailableEvents() {
        List<Event> eventList = new ArrayList<>();

        if (cachedEventsInFile > 0 && objectInputStream == null) {
            try {
                objectOutputStream.flush();
            } catch (IOException e) {
                log.error("Fail to flush events to file '{}': {}", cachedEventsInFile, e.getMessage());
            }

            try {
                objectInputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(
                  eventsCacheFile)));

                while(cachedEventsInFile > 0) {
                    Event event = (Event) objectInputStream.readObject();
                    eventList.add(event);

                    cachedEventsInFile--;
                }

                objectOutputStream.close();
                objectOutputStream = null;
            } catch (IOException e) {
                log.error("Fail to read events from file '{}': {} ", eventsCacheFile, e.getMessage());
            } catch (ClassNotFoundException e) {
                log.error("Fail to deserialize event from file '{}': {}", eventsCacheFile, e.getMessage());
            }
        }

        while (!eventConcurrentLinkedQueue.isEmpty()) {
            eventList.add(eventConcurrentLinkedQueue.poll());
            eventsInMemory--;
        }

        return eventList;
    }

//    public Event poll()  {
//        Event event = null;
//        if(cachedEventsInFile > 0) {
//            if ( objectInputStream == null ) {
//                try {
//                    objectOutputStream.flush();
//                } catch (IOException e) {
//                    log.error("Fail to flush events to file '{}': {}", cachedEventsInFile, e.getMessage());
//                }
//
//                try {
//                    objectInputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(
//                      eventsCacheFile)));
//                    event = (Event) objectInputStream.readObject();
//                    cachedEventsInFile--;
//
//                    if (cachedEventsInFile == 0) {
//                        objectOutputStream.close();
//                        objectOutputStream = null;
//                    }
//
//                } catch (IOException e) {
//                    log.error("Fail to read events from file '{}': {} ", cachedEventsInFile, e.getMessage());
//                } catch (ClassNotFoundException e) {
//                    log.error("Fail to deserialize event from file '{}': {}", cachedEventsInFile, e.getMessage());
//                }
//            }
//        } else if(!eventConcurrentLinkedQueue.isEmpty()) {
//            event = eventConcurrentLinkedQueue.poll();
//            eventsInMemory--;
//        }
//        return event;
//    }

    public void setShouldEvict(boolean shouldEvict) {
        while (eventsInMemory > 0) {
            persistInFile(eventConcurrentLinkedQueue.poll());
            cachedEventsInFile++;
            eventsInMemory--;
        }

        this.shouldEvict = shouldEvict;
    }

    public void stop() {
        log.info("Stopping Redis event buffer...");
        this.currentState = State.STOPPED;
    }
}
