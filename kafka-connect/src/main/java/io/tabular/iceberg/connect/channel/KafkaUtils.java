/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.channel;

import io.tabular.iceberg.connect.events.Event;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

  static ConsumerGroupDescription consumerGroupDescription(String consumerGroupId, Admin admin) {
    try {
      DescribeConsumerGroupsResult result =
          admin.describeConsumerGroups(ImmutableList.of(consumerGroupId));
      return result.describedGroups().get(consumerGroupId).get();

    } catch (InterruptedException | ExecutionException e) {
      throw new ConnectException(
          "Cannot retrieve members for consumer group: " + consumerGroupId, e);
    }
  }

  /**
   * Continuously polls for new records and applies the given function so long as new records are
   * available. Returns when records are no longer available.
   */
  static void consumeAvailable(
      Consumer<String, byte[]> consumer,
      Duration pollDuration,
      java.util.function.Consumer<ConsumerRecord<String, byte[]>> consumerFn) {
    ConsumerRecords<String, byte[]> records = consumer.poll(pollDuration);
    while (!records.isEmpty()) {
      records.forEach(consumerFn);
      records = consumer.poll(pollDuration);
    }
  }

  /** Sends the given events in a single transaction. */
  static void send(
      UUID producerId, Producer<String, byte[]> producer, String topic, List<Event> events) {
    sendAndCommitOffsets(producerId, producer, topic, events, ImmutableMap.of(), null);
  }

  /** Sends the given events and commits offsets (if any) in a single transaction. */
  static void sendAndCommitOffsets(
      UUID producerId,
      Producer<String, byte[]> producer,
      String topic,
      List<Event> events,
      Map<TopicPartition, OffsetAndMetadata> consumerOffsets,
      ConsumerGroupMetadata consumerGroupMetadata) {
    sendAndCommitOffsets(
        producer,
        events.stream()
            .map(
                event -> {
                  LOG.info("Sending event of type: {}", event.type().name());
                  return new ProducerRecord<>(topic, producerId.toString(), Event.encode(event));
                })
            .collect(Collectors.toList()),
        consumerOffsets,
        consumerGroupMetadata);
  }

  /** Commits offsets (if any) via a transactional producer. */
  static void commitOffsets(
      Producer<String, byte[]> producer,
      Map<TopicPartition, OffsetAndMetadata> consumerOffsets,
      ConsumerGroupMetadata consumerGroupMetadata) {
    sendAndCommitOffsets(producer, ImmutableList.of(), consumerOffsets, consumerGroupMetadata);
  }

  /** Sends the given records and commits offsets (if any) in a single transaction. */
  private static void sendAndCommitOffsets(
      Producer<String, byte[]> producer,
      List<ProducerRecord<String, byte[]>> producerRecords,
      Map<TopicPartition, OffsetAndMetadata> consumerOffsets,
      ConsumerGroupMetadata consumerGroupMetadata) {

    producer.beginTransaction();
    try {
      producerRecords.forEach(producer::send);
      producer.flush();
      if (!consumerOffsets.isEmpty()) {
        producer.sendOffsetsToTransaction(consumerOffsets, consumerGroupMetadata);
      }
      producer.commitTransaction();
    } catch (Exception e) {
      try {
        producer.abortTransaction();
      } catch (Exception ex) {
        LOG.warn("Error aborting producer transaction", ex);
      }
      throw e;
    }
  }

  private KafkaUtils() {}
}
