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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.api.Committable;
import io.tabular.iceberg.connect.api.CommittableSupplier;
import io.tabular.iceberg.connect.api.Committer;
import io.tabular.iceberg.connect.data.Offset;
import io.tabular.iceberg.connect.data.Utilities;
import io.tabular.iceberg.connect.events.CommitReadyPayload;
import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventType;
import io.tabular.iceberg.connect.events.TableName;
import io.tabular.iceberg.connect.events.TopicPartitionOffset;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitterImpl implements Committer, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(CommitterImpl.class);
  private final SinkTaskContext context;
  private final IcebergSinkConfig config;
  private final Optional<CoordinatorThread> maybeCoordinatorThread;
  private final CommitRequestListener commitRequestListener;
  private final UUID producerId;
  private final Producer<String, byte[]> producer;

  public CommitterImpl(SinkTaskContext context, IcebergSinkConfig config) {
    this(context, config, new KafkaClientFactory(config.kafkaProps()));
  }

  private CommitterImpl(
      SinkTaskContext context, IcebergSinkConfig config, KafkaClientFactory kafkaClientFactory) {
    this(
        context,
        config,
        new ControlTopicCommitRequestListener(config, kafkaClientFactory),
        kafkaClientFactory,
        new CoordinatorThreadFactoryImpl(kafkaClientFactory));
  }

  @VisibleForTesting
  CommitterImpl(
      SinkTaskContext context,
      IcebergSinkConfig config,
      CommitRequestListener commitRequestListener,
      KafkaClientFactory kafkaClientFactory,
      CoordinatorThreadFactory coordinatorThreadFactory) {
    this.context = context;
    this.config = config;

    this.maybeCoordinatorThread = coordinatorThreadFactory.create(context, config);
    this.commitRequestListener = commitRequestListener;

    // use a random transactional-id to avoid producer generation based fencing
    String transactionalId =
        String.join(
            "-",
            config.connectGroupId(),
            config.taskId().toString(),
            "committer",
            UUID.randomUUID().toString());
    Pair<UUID, Producer<String, byte[]>> pair = kafkaClientFactory.createProducer(transactionalId);
    this.producerId = pair.first();
    this.producer = pair.second();

    // The source-of-truth for source-topic offsets is the control-group-id
    Map<TopicPartition, Long> committedOffsets =
        committedOffsets(kafkaClientFactory, config.controlGroupId());
    // Rewind kafka connect consumer to avoid duplicates
    context.offset(committedOffsets);
  }

  private Map<TopicPartition, Long> committedOffsets(
      KafkaClientFactory kafkaClientFactory, String groupId) {
    try (Admin admin = kafkaClientFactory.createAdmin()) {
      ListConsumerGroupOffsetsResult response =
          admin.listConsumerGroupOffsets(
              groupId, new ListConsumerGroupOffsetsOptions().requireStable(true));
      return response.partitionsToOffsetAndMetadata().get().entrySet().stream()
          .filter(entry -> context.assignment().contains(entry.getKey()))
          .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
    } catch (InterruptedException | ExecutionException e) {
      throw new ConnectException(e);
    }
  }

  private void throwExceptionIfCoordinatorIsTerminated() {
    if (maybeCoordinatorThread.map(CoordinatorThread::isTerminated).orElse(false)) {
      throw new IllegalStateException("Coordinator unexpectedly terminated");
    }
  }

  @Override
  public void commit(CommittableSupplier committableSupplier) {
    throwExceptionIfCoordinatorIsTerminated();

    Optional<UUID> maybeCommitId = commitRequestListener.getCommitId();
    if (maybeCommitId.isPresent()) {
      UUID commitId = maybeCommitId.get();

      Committable committable = committableSupplier.committable();

      List<Event> events = Lists.newArrayList();

      committable
          .writerResults()
          .forEach(
              writerResult -> {
                Event commitResponse =
                    new Event(
                        config.controlGroupId(),
                        EventType.COMMIT_RESPONSE,
                        new CommitResponsePayload(
                            writerResult.partitionStruct(),
                            commitId,
                            TableName.of(writerResult.tableIdentifier()),
                            writerResult.dataFiles(),
                            writerResult.deleteFiles()));

                events.add(commitResponse);
              });

      // include all assigned topic partitions even if no messages were read
      // from a partition, as the coordinator will use that to determine
      // when all data for a commit has been received
      List<TopicPartitionOffset> assignments =
          context.assignment().stream()
              .map(
                  topicPartition -> {
                    Offset offset =
                        committable.offsetsByTopicPartition().getOrDefault(topicPartition, null);
                    return new TopicPartitionOffset(
                        topicPartition.topic(),
                        topicPartition.partition(),
                        offset == null ? null : offset.offset(),
                        offset == null ? null : offset.timestamp());
                  })
              .collect(toList());

      Event commitReady =
          new Event(
              config.controlGroupId(),
              EventType.COMMIT_READY,
              new CommitReadyPayload(commitId, assignments));
      events.add(commitReady);

      Map<TopicPartition, OffsetAndMetadata> offsetsToCommit =
          committable.offsetsByTopicPartition().entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue().offset())));

      KafkaUtils.sendAndCommitOffsets(
          producerId,
          producer,
          config.controlTopic(),
          events,
          offsetsToCommit,
          new ConsumerGroupMetadata(config.controlGroupId()));

      KafkaUtils.commitOffsets(
          producer, offsetsToCommit, new ConsumerGroupMetadata(config.connectGroupId()));
    }
  }

  @Override
  public void close() throws IOException {
    Utilities.close(commitRequestListener);
    maybeCoordinatorThread.ifPresent(CoordinatorThread::terminate);
    Utilities.close(producer);
  }
}
