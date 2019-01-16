/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.front50.model;

import com.aliyun.mns.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.netflix.spectator.api.Registry;
import com.netflix.spinnaker.front50.config.OssProperties;
import com.netflix.spinnaker.front50.model.events.OssEvent;
import com.netflix.spinnaker.front50.model.events.OssEventWrapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static net.logstash.logback.argument.StructuredArguments.value;

/**
 * An ObjectKeyLoader is responsible for returning a last modified timestamp for all objects of a particular type.
 * <p>
 * This implementation listens to an Oss event stream and applies incremental updates whenever an event is received
 * indicating that an object has been modified (add/update/delete).
 * <p>
 * It is significantly faster than delegating to `OssStorageService.listObjectKeys()` with some slight latency attributed
 * to the time taken for an event to be received and processed.
 * <p>
 * Expected latency is less than 1s (Amazon
 */
public class EventingOssObjectKeyLoader implements ObjectKeyLoader, Runnable {
  private static final Logger log = LoggerFactory.getLogger(EventingOssObjectKeyLoader.class);
  private static final Executor executor = Executors.newFixedThreadPool(5);

  private final ObjectMapper objectMapper;
  private final TemporaryMNSQueue temporaryMNSQueue;
  private final OssStorageService ossStorageService;
  private final Registry registry;

  private final Cache<KeyWithObjectType, Long> objectKeysByLastModifiedCache;
  private final LoadingCache<ObjectType, Map<String, Long>> objectKeysByObjectTypeCache;

  private final String rootFolder;

  private boolean pollForMessages = true;

  public EventingOssObjectKeyLoader(ExecutorService executionService,
                                    ObjectMapper objectMapper,
                                    OssProperties ossProperties,
                                    TemporaryMNSQueue temporaryMNSQueue,
                                    OssStorageService ossStorageService,
                                    Registry registry,
                                    boolean scheduleImmediately) {
    this.objectMapper = objectMapper;
    this.temporaryMNSQueue = temporaryMNSQueue;
    this.ossStorageService = ossStorageService;
    this.registry = registry;

    this.objectKeysByLastModifiedCache = CacheBuilder
      .newBuilder()
      // ensure that these keys only expire _after_ their object type has been refreshed
      .expireAfterWrite(ossProperties.getEventing().getRefreshIntervalMs() + 60000, TimeUnit.MILLISECONDS)
      .recordStats()
      .build();


    this.objectKeysByObjectTypeCache = CacheBuilder
      .newBuilder()
      .refreshAfterWrite(ossProperties.getEventing().getRefreshIntervalMs(), TimeUnit.MILLISECONDS)
      .recordStats()
      .build(
        new CacheLoader<ObjectType, Map<String, Long>>() {
          @Override
          public Map<String, Long> load(ObjectType objectType) throws Exception {
            log.debug("Loading object keys for {}", value("type", objectType));
            return ossStorageService.listObjectKeys(objectType);
          }

          @Override
          public ListenableFuture<Map<String, Long>> reload(ObjectType objectType, Map<String, Long> previous) throws Exception {
            ListenableFutureTask<Map<String, Long>> task = ListenableFutureTask.create(
              () -> {
                log.debug("Refreshing object keys for {} (asynchronous)", value("type", objectType));
                return ossStorageService.listObjectKeys(objectType);
              }
            );
            executor.execute(task);
            return task;
          }
        }
      );

    this.rootFolder = ossProperties.getRootFolder();

    if (scheduleImmediately) {
      executionService.submit(this);
    }
  }

  @Override
  @PreDestroy
  public void shutdown() {
    log.debug("Stopping ...");
    pollForMessages = false;
    log.debug("Stopped");
  }

  @Override
  public Map<String, Long> listObjectKeys(ObjectType objectType) {
    try {
      Map<String, Long> objectKeys = objectKeysByObjectTypeCache.get(objectType);
      objectKeysByLastModifiedCache.asMap().entrySet()
        .stream()
        .filter(e -> e.getKey().objectType == objectType)
        .forEach(e -> {
          String key = e.getKey().key;
          if (objectKeys.containsKey(key)) {
            Long currentLastModifiedTime = e.getValue();
            Long previousLastModifiedTime = objectKeys.get(key);
            if (currentLastModifiedTime > previousLastModifiedTime) {
              log.info(
                "Detected Recent Modification (type: {}, key: {}, previous: {}, current: {})",
                value("type", objectType),
                value("key", key),
                value("previousTime", new Date(previousLastModifiedTime)),
                value("currentTime", new Date(e.getValue()))
              );
              objectKeys.put(key, currentLastModifiedTime);
            }
          } else {
            log.info(
              "Detected Recent Modification (type: {}, key: {}, current: {})",
              value("type", objectType),
              value("key", key),
              value("currentTime", new Date(e.getValue()))
            );
            objectKeys.put(key, e.getValue());
          }
        });
      return objectKeys;
    } catch (ExecutionException e) {
      log.error("Unable to fetch keys from cache", e);
      return ossStorageService.listObjectKeys(objectType);
    }
  }

  @Override
  public void run() {
    while (pollForMessages) {
      try {
        List<Message> messages = temporaryMNSQueue.fetchMessages();

        if (messages.isEmpty()) {
          continue;
        }

        messages.forEach(message -> {
          OssEvent ossEvent = unmarshall(objectMapper, message.getMessageBody());
          if (ossEvent != null) {
            tick(ossEvent);
          }
          temporaryMNSQueue.markMessageAsHandled(message.getReceiptHandle());
        });
      } catch (Exception e) {
        log.error("Failed to poll for messages", e);
        registry.counter("oss.eventing.pollErrors").increment();
      }
    }
  }

  private void tick(OssEvent ossEvent) {
    ossEvent.records.forEach(record -> {
      if (record.oss.object.key.endsWith("last-modified.json")) {
        return;
      }

      String eventType = record.eventName;
      KeyWithObjectType keyWithObjectType = buildObjectKey(rootFolder, record.oss.object.key);
      DateTime eventTime = new DateTime(record.eventTime);

      log.debug(
        "Received Event (objectType: {}, type: {}, key: {}, delta: {})",
        value("objectType", keyWithObjectType.objectType),
        value("type", eventType),
        value("key", keyWithObjectType.key),
        value("delta", System.currentTimeMillis() - eventTime.getMillis())
      );

      objectKeysByLastModifiedCache.put(keyWithObjectType, eventTime.getMillis());
    });
  }

  private static KeyWithObjectType buildObjectKey(String rootFolder, String ossObjectKey) {
    if (!rootFolder.endsWith("/")) {
      rootFolder = rootFolder + "/";
    }

    ossObjectKey = ossObjectKey.replace(rootFolder, "");
    ossObjectKey = ossObjectKey.substring(ossObjectKey.indexOf("/") + 1);

    String metadataFilename = ossObjectKey.substring(ossObjectKey.lastIndexOf("/") + 1);
    ossObjectKey = ossObjectKey.substring(0, ossObjectKey.lastIndexOf("/"));

    try {
      ossObjectKey = URLDecoder.decode(ossObjectKey, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException("Invalid key '" + ossObjectKey + "' (non utf-8)");
    }

    ObjectType objectType = Arrays.stream(ObjectType.values())
      .filter(o -> o.defaultMetadataFilename.equalsIgnoreCase(metadataFilename))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException("No ObjectType found (defaultMetadataFileName: " + metadataFilename + ")"));

    return new KeyWithObjectType(objectType, ossObjectKey);
  }

  private static OssEvent unmarshall(ObjectMapper objectMapper, String messageBody) {
    OssEventWrapper ossEventWrapper;
    try {
      ossEventWrapper = objectMapper.readValue(messageBody, OssEventWrapper.class);
    } catch (IOException e) {
      log.debug("Unable unmarshal OssEventWrapper (body: {})", value("message", messageBody), e);
      return null;
    }

    try {
      return objectMapper.readValue(ossEventWrapper.message, OssEvent.class);
    } catch (IOException e) {
      log.debug("Unable unmarshal OssEvent (body: {})", value("body", ossEventWrapper.message), e);
      return null;
    }
  }

  private static class KeyWithObjectType {
    final ObjectType objectType;
    final String key;

    KeyWithObjectType(ObjectType objectType, String key) {
      this.objectType = objectType;
      this.key = key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      KeyWithObjectType that = (KeyWithObjectType) o;

      if (objectType != that.objectType) return false;
      return key.equals(that.key);
    }

    @Override
    public int hashCode() {
      int result = objectType.hashCode();
      result = 31 * result + key.hashCode();
      return result;
    }
  }
}
