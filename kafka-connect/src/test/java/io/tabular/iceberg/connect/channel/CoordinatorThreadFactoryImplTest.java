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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CoordinatorThreadFactoryImplTest {

  private static final String CONNECTOR_NAME = "connector-name";
  private static final String TABLE_1_NAME = "db.tbl1";
  private static final String CONTROL_TOPIC = "control-topic-name";
  private static final TopicPartition CONTROL_TOPIC_PARTITION =
      new TopicPartition(CONTROL_TOPIC, 0);
  private static final IcebergSinkConfig BASIC_CONFIGS =
      new IcebergSinkConfig(
          ImmutableMap.of(
              "name",
              CONNECTOR_NAME,
              "iceberg.catalog.catalog-impl",
              "org.apache.iceberg.inmemory.InMemoryCatalog",
              "iceberg.tables",
              TABLE_1_NAME,
              "iceberg.control.topic",
              CONTROL_TOPIC,
              IcebergSinkConfig.INTERNAL_TRANSACTIONAL_SUFFIX_PROP,
              "-txn-" + UUID.randomUUID() + "-" + 0));

  private static final String TOPIC_0 = "source-topic-name-0";
  private static final String TOPIC_1 = "source-topic-name-1";
  private static final TopicPartition T0P0 = new TopicPartition(TOPIC_0, 0);
  private static final TopicPartition T0P1 = new TopicPartition(TOPIC_0, 1);
  private static final TopicPartition T1P0 = new TopicPartition(TOPIC_1, 0);
  private static final TopicPartition T1P1 = new TopicPartition(TOPIC_1, 1);
  private static final Set<TopicPartition> LEADER_ASSIGNMENT = ImmutableSet.of(T0P0, T1P1);
  private static final Set<TopicPartition> NON_LEADER_ASSIGNMENT = ImmutableSet.of(T0P1, T1P0);
  private static final List<MemberDescription> MEMBER_DESCRIPTIONS =
      ImmutableList.of(
          new MemberDescription(null, null, null, new MemberAssignment(LEADER_ASSIGNMENT)),
          new MemberDescription(null, null, null, new MemberAssignment(NON_LEADER_ASSIGNMENT)));

  private KafkaClientFactory kafkaClientFactory;
  private UUID producerId;
  private MockProducer<String, byte[]> producer;
  private MockConsumer<String, byte[]> consumer;
  private Admin admin;

  @BeforeEach
  public void before() {
    admin = mock(Admin.class);

    producerId = UUID.randomUUID();
    producer = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
    producer.initTransactions();

    consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

    kafkaClientFactory = mock(KafkaClientFactory.class);
    when(kafkaClientFactory.createConsumer(any())).thenReturn(consumer);
    when(kafkaClientFactory.createProducer(any())).thenReturn(Pair.of(producerId, producer));
    when(kafkaClientFactory.createAdmin()).thenReturn(admin);
  }

  @AfterEach
  public void after() {
    producer.close();
    consumer.close();
    admin.close();
  }

  private void whenAdminDescribeConsumerGroupThenReturn(
      ConsumerGroupState consumerGroupState, List<MemberDescription> memberDescriptions) {
    String connectGroupId = BASIC_CONFIGS.connectGroupId();

    when(admin.describeConsumerGroups(eq(ImmutableList.of(connectGroupId))))
        .thenReturn(
            new DescribeConsumerGroupsResult(
                ImmutableMap.of(
                    connectGroupId,
                    KafkaFuture.completedFuture(
                        new ConsumerGroupDescription(
                            connectGroupId,
                            true,
                            memberDescriptions,
                            null,
                            consumerGroupState,
                            mock(Node.class))))));
  }

  private void whenAdminDescribeConsumerGroupThenReturn(ConsumerGroupState consumerGroupState) {
    whenAdminDescribeConsumerGroupThenReturn(consumerGroupState, MEMBER_DESCRIPTIONS);
  }

  @Test
  public void testShouldReturnEmptyIfNotLeader() {
    whenAdminDescribeConsumerGroupThenReturn(ConsumerGroupState.STABLE);

    CoordinatorThreadFactoryImpl coordinatorThreadFactory =
        new CoordinatorThreadFactoryImpl(mock(Catalog.class), kafkaClientFactory);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    when(sinkTaskContext.assignment()).thenReturn(NON_LEADER_ASSIGNMENT);

    Optional<CoordinatorThread> maybeCoordinatorThread =
        coordinatorThreadFactory.create(sinkTaskContext, BASIC_CONFIGS);
    try {
      assertThat(maybeCoordinatorThread).isEmpty();
    } finally {
      maybeCoordinatorThread.ifPresent(CoordinatorThread::terminate);
    }
  }

  @Test
  public void testShouldReturnEmptyIfLeaderButGroupIsNotStable() {
    whenAdminDescribeConsumerGroupThenReturn(ConsumerGroupState.UNKNOWN);

    CoordinatorThreadFactoryImpl coordinatorThreadFactory =
        new CoordinatorThreadFactoryImpl(mock(Catalog.class), kafkaClientFactory);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    when(sinkTaskContext.assignment()).thenReturn(LEADER_ASSIGNMENT);

    Optional<CoordinatorThread> maybeCoordinatorThread =
        coordinatorThreadFactory.create(sinkTaskContext, BASIC_CONFIGS);
    try {
      assertThat(maybeCoordinatorThread).isEmpty();
    } finally {
      maybeCoordinatorThread.ifPresent(CoordinatorThread::terminate);
    }
  }

  @Test
  public void testShouldReturnThreadIfLeaderAndGroupIsStable() {
    whenAdminDescribeConsumerGroupThenReturn(ConsumerGroupState.STABLE);

    CoordinatorThreadFactoryImpl coordinatorThreadFactory =
        new CoordinatorThreadFactoryImpl(mock(Catalog.class), kafkaClientFactory);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    when(sinkTaskContext.assignment()).thenReturn(LEADER_ASSIGNMENT);

    Optional<CoordinatorThread> maybeCoordinatorThread =
        coordinatorThreadFactory.create(sinkTaskContext, BASIC_CONFIGS);
    try {
      assertThat(maybeCoordinatorThread).isPresent();
    } finally {
      maybeCoordinatorThread.ifPresent(CoordinatorThread::terminate);
    }
  }

  @Test
  public void testShouldThrowExceptionIfNoPartitionsAssigned() {
    // This could happen if a connector is configured with a topics.regex that doesn't match any
    // topics in cluster

    whenAdminDescribeConsumerGroupThenReturn(
        ConsumerGroupState.STABLE,
        ImmutableList.of(
            new MemberDescription(null, null, null, new MemberAssignment(ImmutableSet.of())),
            new MemberDescription(null, null, null, new MemberAssignment(ImmutableSet.of()))));

    CoordinatorThreadFactoryImpl coordinatorThreadFactory =
        new CoordinatorThreadFactoryImpl(mock(Catalog.class), kafkaClientFactory);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    when(sinkTaskContext.assignment()).thenReturn(ImmutableSet.of());

    assertThatThrownBy(() -> coordinatorThreadFactory.create(sinkTaskContext, BASIC_CONFIGS))
        .isInstanceOf(ConnectException.class)
        .hasMessage("No partitions assigned, cannot determine leader");
  }
}
