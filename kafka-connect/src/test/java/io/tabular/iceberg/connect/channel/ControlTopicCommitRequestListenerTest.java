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

import static org.assertj.core.api.Assertions.assertThat;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.events.CommitCompletePayload;
import io.tabular.iceberg.connect.events.CommitRequestPayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventType;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class ControlTopicCommitRequestListenerTest {

  private static final String CONNECTOR_NAME = "connector-name";
  private static final String TABLE_1_NAME = "db.tbl1";
  private static final String CONTROL_TOPIC = "control-topic-name";
  private static final int CONTROL_TOPIC_PARTITION = 0;
  private static final IcebergSinkConfig BASIC_CONFIGS =
      new IcebergSinkConfig(
          ImmutableMap.of(
              "name", CONNECTOR_NAME,
              "iceberg.catalog.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog",
              "iceberg.tables", TABLE_1_NAME,
              "iceberg.control.topic", CONTROL_TOPIC));

  private static final String PRODUCER_ID = UUID.randomUUID().toString();

  private void initMockConsumerAfterSubscribe(MockConsumer<?, ?> mockConsumer) {
    TopicPartition tp = new TopicPartition(CONTROL_TOPIC, CONTROL_TOPIC_PARTITION);
    mockConsumer.rebalance(ImmutableList.of(tp));
    mockConsumer.updateEndOffsets(ImmutableMap.of(tp, 0L));
  }

  @Test
  public void testReturnsEmptyWhenThereAreNoMessages() throws IOException {
    final MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final KafkaClientFactory kafkaClientFactory = new MockKafkaClientFactory(consumer, null, null);
    try (ControlTopicCommitRequestListener commitRequestListener =
        new ControlTopicCommitRequestListener(BASIC_CONFIGS, kafkaClientFactory)) {
      initMockConsumerAfterSubscribe(consumer);

      assertThat(commitRequestListener.getCommitId()).isEmpty();
    }
  }

  @Test
  public void testReturnsEmptyWhenThereAreNoCommitRequests() throws IOException {
    final MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final KafkaClientFactory kafkaClientFactory = new MockKafkaClientFactory(consumer, null, null);
    try (ControlTopicCommitRequestListener commitRequestListener =
        new ControlTopicCommitRequestListener(BASIC_CONFIGS, kafkaClientFactory)) {
      initMockConsumerAfterSubscribe(consumer);

      consumer.addRecord(
          new ConsumerRecord<>(
              CONTROL_TOPIC,
              CONTROL_TOPIC_PARTITION,
              0,
              PRODUCER_ID,
              Event.encode(
                  new Event(
                      BASIC_CONFIGS.controlGroupId(),
                      EventType.COMMIT_COMPLETE,
                      new CommitCompletePayload(UUID.randomUUID(), 100L)))));

      assertThat(commitRequestListener.getCommitId()).isEmpty();
    }
  }

  @Test
  public void testReturnsCommitIdWhenThereIsACommitRequest() throws IOException {
    final MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final KafkaClientFactory kafkaClientFactory = new MockKafkaClientFactory(consumer, null, null);
    try (ControlTopicCommitRequestListener commitRequestListener =
        new ControlTopicCommitRequestListener(BASIC_CONFIGS, kafkaClientFactory)) {
      initMockConsumerAfterSubscribe(consumer);

      final UUID commitId = UUID.randomUUID();
      consumer.addRecord(
          new ConsumerRecord<>(
              CONTROL_TOPIC,
              CONTROL_TOPIC_PARTITION,
              0,
              PRODUCER_ID,
              Event.encode(
                  new Event(
                      BASIC_CONFIGS.controlGroupId(),
                      EventType.COMMIT_REQUEST,
                      new CommitRequestPayload(commitId)))));

      assertThat(commitRequestListener.getCommitId()).isEqualTo(Optional.of(commitId));
    }
  }

  @Test
  public void testReturnsLatestCommitIdWhenThereAreMultipleCommitRequestsAvailable()
      throws IOException {
    final MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final KafkaClientFactory kafkaClientFactory = new MockKafkaClientFactory(consumer, null, null);
    try (ControlTopicCommitRequestListener commitRequestListener =
        new ControlTopicCommitRequestListener(BASIC_CONFIGS, kafkaClientFactory)) {
      initMockConsumerAfterSubscribe(consumer);

      final UUID firstCommitId = UUID.randomUUID();
      final UUID secondCommitId = UUID.randomUUID();

      AtomicInteger currentOffset = new AtomicInteger();
      ImmutableList.of(firstCommitId, secondCommitId)
          .forEach(
              commitId ->
                  consumer.addRecord(
                      new ConsumerRecord<>(
                          CONTROL_TOPIC,
                          CONTROL_TOPIC_PARTITION,
                          currentOffset.getAndAdd(1),
                          PRODUCER_ID,
                          Event.encode(
                              new Event(
                                  BASIC_CONFIGS.controlGroupId(),
                                  EventType.COMMIT_REQUEST,
                                  new CommitRequestPayload(commitId))))));

      assertThat(commitRequestListener.getCommitId()).isEqualTo(Optional.of(secondCommitId));
    }
  }

  @Test
  public void testClosesUnderlyingConsumer() throws IOException {
    final MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final KafkaClientFactory kafkaClientFactory = new MockKafkaClientFactory(consumer, null, null);
    final ControlTopicCommitRequestListener commitRequestListener =
        new ControlTopicCommitRequestListener(BASIC_CONFIGS, kafkaClientFactory);
    initMockConsumerAfterSubscribe(consumer);

    assertThat(consumer.closed()).isFalse();
    commitRequestListener.close();
    assertThat(consumer.closed()).isTrue();
  }
}
