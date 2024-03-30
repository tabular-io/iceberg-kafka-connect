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
package io.tabular.iceberg.connect.writer;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

class WriterTest {
  private static final String SOURCE_TOPIC = "source-topic-name";
  private static final String CONNECTOR_NAME = "connector-name";
  private static final ConsumerGroupMetadata CONSUMER_GROUP_METADATA =
      new ConsumerGroupMetadata(String.format("connect-%s", CONNECTOR_NAME));
  private static final String TABLE_1_NAME = "db.tbl1";
  private static final String CONTROL_TOPIC = "control-topic-name";

  private static DynConstructors.Ctor<CoordinatorKey> ctorCoordinatorKey() {
    return DynConstructors.builder(CoordinatorKey.class)
        .hiddenImpl(
            "org.apache.kafka.clients.admin.internals.CoordinatorKey",
            FindCoordinatorRequest.CoordinatorType.class,
            String.class)
        .build();
  }

  private static DynConstructors.Ctor<ListConsumerGroupOffsetsResult>
      ctorListConsumerGroupOffsetsResult() {
    return DynConstructors.builder(ListConsumerGroupOffsetsResult.class)
        .hiddenImpl("org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult", Map.class)
        .build();
  }

  private static ListConsumerGroupOffsetsOptions listOffsetResultMatcher() {
    return argThat(x -> x.topicPartitions() == null && x.requireStable());
  }

  @Test
  public void testWorkersManagerResetsOffsetsSafely() {
    final SinkTaskContext context = mock(SinkTaskContext.class);

    final IcebergSinkConfig config =
        new IcebergSinkConfig(
            ImmutableMap.of(
                "name", CONNECTOR_NAME,
                "iceberg.catalog.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog",
                "iceberg.tables", TABLE_1_NAME,
                "iceberg.control.topic", CONTROL_TOPIC));

    final Set<TopicPartition> topicPartitions =
        ImmutableSet.of(new TopicPartition(SOURCE_TOPIC, 0), new TopicPartition(SOURCE_TOPIC, 1));

    final Admin admin = mock(Admin.class);
    final Map<TopicPartition, Long> safeOffsets =
        topicPartitions.stream()
            .collect(Collectors.toMap(Function.identity(), tp -> tp.partition() + 100L));
    final CoordinatorKey coordinatorKey =
        ctorCoordinatorKey()
            .newInstance(FindCoordinatorRequest.CoordinatorType.GROUP, "fakeCoordinatorKey");
    final ListConsumerGroupOffsetsResult safeOffsetsResult =
        ctorListConsumerGroupOffsetsResult()
            .newInstance(
                ImmutableMap.of(
                    coordinatorKey,
                    KafkaFuture.completedFuture(
                        safeOffsets.entrySet().stream()
                            .collect(
                                Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> new OffsetAndMetadata(e.getValue()))))));
    when(admin.listConsumerGroupOffsets(eq(config.connectGroupId()), listOffsetResultMatcher()))
        .thenReturn(safeOffsetsResult);

    new WriterImpl(config, topicPartitions);
    //
    //    InOrder inOrderVerifier = inOrder(partitionWorkerFactory, admin, context);
    //    // should create a worker for each topicPartition
    //    inOrderVerifier
    //        .verify(partitionWorkerFactory)
    //        .createWorker(CONSUMER_GROUP_METADATA, topicPartitions.get(0));
    //    inOrderVerifier
    //        .verify(partitionWorkerFactory)
    //        .createWorker(CONSUMER_GROUP_METADATA, topicPartitions.get(1));
    //    // only after that, should it retrieve offsets from kafka
    //    inOrderVerifier
    //        .verify(admin)
    //        .listConsumerGroupOffsets(eq(config.connectGroupId()), listOffsetResultMatcher());
    //    // only after that, should it rewind the connector to safe offsets
    //    inOrderVerifier.verify(context).offset(safeOffsets);
    //    inOrderVerifier.verifyNoMoreInteractions();
  }

  // TODO: test if adminClient returns nulls for new topic partitions that have not yet been
  // committed to.

  // TODO: test for new connector which won't have any offsets committed.
}
