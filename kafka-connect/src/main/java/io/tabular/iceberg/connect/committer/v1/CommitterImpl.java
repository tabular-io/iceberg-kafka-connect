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
package io.tabular.iceberg.connect.committer.v1;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.api.CommittableSupplier;
import io.tabular.iceberg.connect.api.Committer;
import io.tabular.iceberg.connect.channel.CommitRequestListener;
import io.tabular.iceberg.connect.channel.ControlTopicCommitRequestListener;
import io.tabular.iceberg.connect.channel.CoordinatorThread;
import io.tabular.iceberg.connect.data.Utilities;
import io.tabular.iceberg.connect.events.CommitReadyPayload;
import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventType;
import io.tabular.iceberg.connect.events.TableName;
import io.tabular.iceberg.connect.events.TopicPartitionOffset;
import io.tabular.iceberg.connect.kafka.AdminFactory;
import io.tabular.iceberg.connect.kafka.ConsumerFactory;
import io.tabular.iceberg.connect.kafka.Factory;
import io.tabular.iceberg.connect.kafka.KafkaUtils;
import io.tabular.iceberg.connect.kafka.TransactionalProducerFactory;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pretty similar to the existing commit functionality except it adds zombie fencing support via
 * consumer generation based fencing.
 */
public class CommitterImpl implements Committer {
  private static final Logger LOG = LoggerFactory.getLogger(CommitterImpl.class);
  private final IcebergSinkConfig config;
  private final Set<TopicPartition> topicPartitions;
  private final Catalog catalog;
  private final CoordinatorThread coordinatorThread;
  private final CommitRequestListener commitRequestListener;
  private final ConsumerGroupMetadata consumerGroupMetadata;
  private final UUID producerId;
  private final Producer<String, byte[]> producer;

  public CommitterImpl(
      SinkTaskContext context, IcebergSinkConfig config, Set<TopicPartition> topicPartitions) {
    this(
        context,
        config,
        topicPartitions,
        Utilities.loadCatalog(config),
        new AdminFactory(),
        new ConsumerFactory(),
        new TransactionalProducerFactory());
  }

  @VisibleForTesting
  CommitterImpl(
      SinkTaskContext context,
      IcebergSinkConfig configArg,
      Set<TopicPartition> topicPartitionsArg,
      Catalog catalogArg,
      Factory<Admin> adminFactory,
      Factory<Consumer<String, byte[]>> consumerFactory,
      Factory<Producer<String, byte[]>> producerFactory) {
    this.config = configArg;
    this.topicPartitions = topicPartitionsArg;
    this.catalog = catalogArg;

    // TODO: this is a breaking change in single-cluster-mode
    // TODO: avoid this until 1.0 release
    // TODO: use controlGroupId to be backwards compatible for now
    this.consumerGroupMetadata = extractConsumer(context).groupMetadata();

    this.coordinatorThread = startCoordinator(adminFactory, consumerFactory, producerFactory);
    this.commitRequestListener = new ControlTopicCommitRequestListener(config, consumerFactory);

    this.producerId = UUID.randomUUID();
    Map<String, String> producerProps = Maps.newHashMap(config.controlClusterKafkaProps());
    // use a random transactional-id to avoid producer generation based fencing
    // we rely exclusively on consumer generation based fencing
    // TODO: issue is that in order to be backwards compatible, I have to commit offset to the
    // control-group-id so I cant actually use consumer generation based fencing here either
    // until we're ready to make breaking changes
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerId.toString());
    this.producer = producerFactory.create(producerProps);

    // TODO: TBH not sure if I need to rewind context with consumer based fencing
    // probably not.
  }

  private boolean isLeader() {
    return Objects.equals(config.taskId(), 0);
  }

  private CoordinatorThread startCoordinator(
      Factory<Admin> adminFactory,
      Factory<Consumer<String, byte[]>> consumerFactory,
      Factory<Producer<String, byte[]>> producerFactory) {
    final CoordinatorThread thread;

    if (isLeader()) {
      LOG.info("Task elected leader");
      try (Admin admin = adminFactory.create(config.sourceClusterKafkaProps())) {
        ConsumerGroupDescription groupDesc =
            KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin);
        if (groupDesc.state() == ConsumerGroupState.STABLE) {
          LOG.info("Group is stable, starting commit coordinator");
          Collection<MemberDescription> members = groupDesc.members();
          thread =
              new CoordinatorThread(catalog, config, members, consumerFactory, producerFactory);
          thread.start();
          LOG.info("Started commit coordinator");
        } else {
          throw new RuntimeException("Could not start coordinator");
        }
      }
    } else {
      thread = null;
    }

    return thread;
  }

  private static Consumer<?, ?> extractConsumer(SinkTaskContext context) {
    try {
      WorkerSinkTaskContext workerContext = (WorkerSinkTaskContext) context;
      Field field = workerContext.getClass().getDeclaredField("consumer");
      field.setAccessible(true);
      return (Consumer<?, ?>) field.get(workerContext);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private void throwExceptionIfCoordinatorIsTerminated() {
    if (isLeader() && (coordinatorThread == null || coordinatorThread.isTerminated())) {
      throw new RuntimeException("Coordinator unexpectedly terminated");
    }
  }

  @Override
  public void commit(CommittableSupplier committableSupplier) {
    throwExceptionIfCoordinatorIsTerminated();

    Optional<UUID> maybeCommitId = commitRequestListener.getCommitId();
    if (maybeCommitId.isPresent()) {
      UUID commitId = maybeCommitId.get();

      committableSupplier
          .committables()
          .forEach(
              committable -> {
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

                Event commitReady =
                    new Event(
                        config.controlGroupId(),
                        EventType.COMMIT_READY,
                        new CommitReadyPayload(
                            commitId,
                            ImmutableList.of(
                                new TopicPartitionOffset(
                                    committable.topicPartition().topic(),
                                    committable.topicPartition().partition(),
                                    committable.offset().offset(),
                                    committable.offset().timestamp()))));
                events.add(commitReady);

                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit =
                    Optional.ofNullable(committable.offset().offset())
                        .map(
                            offset ->
                                ImmutableMap.of(
                                    committable.topicPartition(), new OffsetAndMetadata(offset)))
                        .orElseGet(ImmutableMap::of);

                KafkaUtils.sendAndCommitOffsets(
                    producer,
                    producerId,
                    config.controlTopic(),
                    events,
                    offsetsToCommit,
                    consumerGroupMetadata);
              });
    }
  }

  @Override
  public void close() throws IOException {
    commitRequestListener.close();

    if (coordinatorThread != null) {
      coordinatorThread.terminate();
    }

    if (catalog != null) {
      if (catalog instanceof AutoCloseable) {
        try {
          ((AutoCloseable) catalog).close();
        } catch (Exception e) {
          LOG.warn("An error occurred closing catalog instance, ignoring...", e);
        }
      }
    }

    producer.close();
  }
}
