package com.netflix.spinnaker.front50.model;

import com.aliyun.mns.client.*;
import com.aliyun.mns.common.utils.ServiceSettings;
import com.aliyun.mns.model.*;
import com.aliyun.mns.model.request.queue.CreateQueueRequest;
import com.aliyun.mns.client.CloudTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.List;

public class TemporaryMNSQueue {

  CloudAccount account = new CloudAccount(
    ServiceSettings.getMNSAccessKeyId(),
    ServiceSettings.getMNSAccessKeySecret(),
    ServiceSettings.getMNSAccountEndpoint());
  MNSClient client = account.getMNSClient();
  private final Logger logger = LoggerFactory.getLogger(TemporaryMNSQueue.class);

  private final QueueMeta qMeta;
  CloudQueue queue;
  SubscriptionMeta subMeta;
  CloudTopic topic;
  private final TemporaryQueue temporaryQueue;

  public TemporaryMNSQueue(QueueMeta qMeta, MNSClient client, String mnsTopicName, String instanceId) {
    this.qMeta = qMeta;
    this.client = client;
    String sanitizedInstanceId = getSanitizedInstanceId(instanceId);
    String mnsTopicArn = getMnsTopicArn(client, mnsTopicName);
    String mnsQueueName = mnsTopicName + "__" + sanitizedInstanceId;

    this.temporaryQueue = createQueue(mnsTopicArn, mnsQueueName);
  }

  List<Message> fetchMessages() {
    queue = client.getQueueRef(temporaryQueue.mnsQueueUrl);
    return queue.batchPopMessage(10, 1);
  }

  void markMessageAsHandled(String receiptHandle) {
    try {

      queue = client.getQueueRef(temporaryQueue.mnsQueueUrl);
      queue.deleteMessage(receiptHandle);
    } catch (Exception e) {
      logger.warn("Error deleting message, reason: {} (receiptHandle: {})", e.getMessage(), receiptHandle);
    }
  }

  @PreDestroy
  void shutdown() {
    try {
      logger.debug("Removing Temporary  Notification Queue: {}", "queue", temporaryQueue.mnsQueueUrl);
      CloudQueue queue = client.getQueueRef(temporaryQueue.mnsQueueUrl);
      queue.delete();

      logger.debug("Removed Temporary  Notification Queue: {}", "queue", temporaryQueue.mnsQueueUrl);
    } catch (Exception e) {
      logger.error("Unable to remove queue: {} (reason: {})", "queue", temporaryQueue.mnsQueueUrl, e.getMessage(), e);
    }

    try {
      logger.debug("Removing  Notification Subscription: {}", temporaryQueue.mnsTopicSubscriptionArn);
      topic = client.getTopicRef(temporaryQueue.mnsTopicSubscriptionArn);
      topic.unsubscribe("mnsTopicSubscriptionArn");
      logger.debug("Removed  Notification Subscription: {}", temporaryQueue.mnsTopicSubscriptionArn);
    } catch (Exception e) {
      logger.error("Unable to unsubscribe queue from topic: {} (reason: {})", "topic", temporaryQueue.mnsTopicSubscriptionArn, e.getMessage(), e);
    }
  }

  private String getMnsTopicArn(MNSClient client, String topicName) {
    PagingListResult pagingListResult = client.listTopic("cloud-", topicName, 1);

    return String.valueOf(pagingListResult);
  }

  private TemporaryQueue createQueue(String mnsTopicArn, String mnsQueueName) {
    qMeta.setQueueName(mnsQueueName);
    // qMeta.setMessageRetentionPeriod(60l);
    String mnsQueueUrl = client.createQueue(new CreateQueueRequest().getQueueMeta()).getQueueURL();

    logger.info("Created Temporary Notification Queue: {}", mnsQueueUrl);
    subMeta.setTopicName(mnsTopicArn);

    String mnsTopicSubscriptionArn = topic.subscribe(subMeta);


    return new TemporaryQueue(mnsQueueUrl, mnsTopicSubscriptionArn, mnsTopicArn);
  }

  static String getSanitizedInstanceId(String instanceId) {

    return instanceId.replaceAll("[^\\w\\-]", "_");
  }


  protected static class TemporaryQueue {
    final String mnsTopicArn;
    final String mnsQueueUrl;
    final String mnsTopicSubscriptionArn;

    TemporaryQueue(String mnsTopicArn, String mnsQueueUrl, String mnsTopicSubscriptionArn) {
      this.mnsTopicArn = mnsTopicArn;
      this.mnsQueueUrl = mnsQueueUrl;
      this.mnsTopicSubscriptionArn = mnsTopicSubscriptionArn;
    }
  }

}
