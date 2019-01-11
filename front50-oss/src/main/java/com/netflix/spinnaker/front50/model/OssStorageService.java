/*
 * Copyright 2016 Netflix, Inc.
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

import com.aliyun.oss.OSSException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.OSSObject;
import org.apache.commons.lang.StringUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.front50.exception.NotFoundException;
import com.netflix.spinnaker.security.AuthenticatedRequest;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import static net.logstash.logback.argument.StructuredArguments.value;

public class OssStorageService implements StorageService {
  private static final Logger log = LoggerFactory.getLogger(OssStorageService.class);

  private final ObjectMapper objectMapper;
  private final OSS aliyunOss;
  private final String bucket;
  private final String rootFolder;
  private final Boolean readOnlyMode;
  private final String region;
  private final Integer maxKeys;

  public OssStorageService(ObjectMapper objectMapper,
                           OSS aliyunOss,
                           String bucket,
                           String rootFolder,
                           Boolean readOnlyMode,
                           String region,
                           Integer maxKeys) {
    this.objectMapper = objectMapper;
    this.aliyunOss = aliyunOss;
    this.bucket = bucket;
    this.rootFolder = rootFolder;
    this.readOnlyMode = readOnlyMode;
    this.region = region;
    this.maxKeys = maxKeys;
  }

  @Override
  public void ensureBucketExists() {
    if (aliyunOss.doesBucketExist(bucket))
      log.info("Bucket {} already available", value("bucket", bucket));
    else
      {
        if (StringUtils.isEmpty(region)) {
          log.info("Creating bucket {} in default region", value("bucket", bucket));
          aliyunOss.createBucket(bucket);
        } else {
          log.info("Creating bucket {} in region {}",
            value("bucket", bucket),
            value("region", region)
          );
          aliyunOss.createBucket(bucket);
        }
      }
  }

  @Override
  public boolean supportsVersioning() {
    return true;
  }

  @Override
  public <T extends Timestamped> T loadObject(ObjectType objectType, String objectKey) throws NotFoundException {
    try {
      OSSObject ossObject = aliyunOss.getObject(bucket, buildOssKey(objectType.group, objectKey, objectType.defaultMetadataFilename));
      T item = deserialize(ossObject, (Class<T>) objectType.clazz);
      item.setLastModified(ossObject.getObjectMetadata().getLastModified().getTime());
      return item;
    } catch (OSSException e) {
      if (e.getErrorCode().equals("NoSuchBucket")) {
        throw new NotFoundException("Object not found (key: " + objectKey + ")");
      }
      throw e;
    } catch (IOException e) {
      throw new IllegalStateException("Unable to deserialize object (key: " + objectKey + ")", e);
    }
  }

  @Override
  public void deleteObject(ObjectType objectType, String objectKey) {
    if (readOnlyMode) {
      throw new ReadOnlyModeException();
    }
    aliyunOss.deleteObject(bucket, buildOssKey(objectType.group, objectKey, objectType.defaultMetadataFilename));
    writeLastModified(objectType.group);
  }

  @Override
  public <T extends Timestamped> void storeObject(ObjectType objectType, String objectKey, T item) {
    if (readOnlyMode) {
      throw new ReadOnlyModeException();
    }
    try {
      item.setLastModifiedBy(AuthenticatedRequest.getSpinnakerUser().orElse("anonymous"));
      byte[] bytes = objectMapper.writeValueAsBytes(item);

      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setContentLength(bytes.length);
      objectMetadata.setContentMD5(new String(org.apache.commons.codec.binary.Base64.encodeBase64(DigestUtils.md5(bytes))));

      aliyunOss.putObject(
        bucket,
        buildOssKey(objectType.group, objectKey, objectType.defaultMetadataFilename),
        new ByteArrayInputStream(bytes),
        objectMetadata
      );
      writeLastModified(objectType.group);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Map<String, Long> listObjectKeys(ObjectType objectType) {
    long startTime = System.currentTimeMillis();
    ObjectListing bucketListing = aliyunOss.listObjects(
      new ListObjectsRequest(bucket, buildTypedFolder(rootFolder, objectType.group), null, null, maxKeys)
    );
    List<OSSObjectSummary> summaries = bucketListing.getObjectSummaries();

    while (bucketListing.isTruncated()) {
      summaries.addAll(bucketListing.getObjectSummaries());
    }
    log.debug("Took {}ms to fetch {} object keys for {}",
      value("fetchTime", (System.currentTimeMillis() - startTime)),
      summaries.size(),
      value("type", objectType));

    return summaries
      .stream()
      .filter(s -> filterOssObjectSummary(s, objectType.defaultMetadataFilename))
      .collect(Collectors.toMap((s -> buildObjectKey(objectType, s.getKey())), (s -> s.getLastModified().getTime())));
  }

  @Override
  public <T extends Timestamped> Collection<T> listObjectVersions(ObjectType objectType,
                                                                  String objectKey,
                                                                  int maxResults) throws NotFoundException {
    return null;
  }

  @Override

  public long getLastModified(ObjectType objectType) {
    try {
      Map<String, Long> lastModified = objectMapper.readValue(
        aliyunOss.getObject(bucket, buildTypedFolder(rootFolder, objectType.group) + "/last-modified.json").getObjectContent(),
        Map.class
      );

      return lastModified.get("lastModified");
    } catch (Exception e) {
      return 0L;
    }
  }

  @Override
  public long getHealthIntervalMillis() {
    return Duration.ofSeconds(2).toMillis();
  }

  private void writeLastModified(String group) {
    if (readOnlyMode) {
      throw new ReadOnlyModeException();
    }
    try {
      byte[] bytes = objectMapper.writeValueAsBytes(Collections.singletonMap("lastModified", System.currentTimeMillis()));

      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setContentLength(bytes.length);
      objectMetadata.setContentMD5(new String(org.apache.commons.codec.binary.Base64.encodeBase64(DigestUtils.md5(bytes))));

      aliyunOss.putObject(
        bucket,
        buildTypedFolder(rootFolder, group) + "/last-modified.json",
        new ByteArrayInputStream(bytes),
        objectMetadata
      );
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  private <T extends Timestamped> T deserialize(OSSObject ossObject, Class<T> clazz) throws IOException {
    return objectMapper.readValue(ossObject.getObjectContent(), clazz);
  }

  private boolean filterOssObjectSummary(OSSObjectSummary ossObjectSummary, String metadataFilename) {
    return ossObjectSummary.getKey().endsWith(metadataFilename);
  }

  private String buildOssKey(String group, String objectKey, String metadataFilename) {
    if (objectKey.endsWith(metadataFilename)) {
      return objectKey;
    }

    return (buildTypedFolder(rootFolder, group) + "/" + objectKey.toLowerCase() + "/" + metadataFilename).replace("//", "/");
  }

  private String buildObjectKey(ObjectType objectType, String ossKey) {
    return ossKey
      .replaceAll(buildTypedFolder(rootFolder, objectType.group) + "/", "")
      .replaceAll("/" + objectType.defaultMetadataFilename, "");
  }

  private static String buildTypedFolder(String rootFolder, String type) {
    return (rootFolder + "/" + type).replaceAll("//", "/");
  }
}
