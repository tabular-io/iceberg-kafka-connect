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
package io.tabular.iceberg.connect.committer.v2;

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
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
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
 * This committer implementation does a few things differently compared to {@link
 * io.tabular.iceberg.connect.committer.v1.CommitterImpl}"
 *
 * <ul>
 *   <li>Uses producer and consumer fencing
 *   <li>Support a separate control cluster i.e. it will create control topic on the control cluster
 *       and will also commit consumer offsets to the control cluster
 * </ul>
 */
public class CommitterImpl implements Committer {
  private static final Logger LOG = LoggerFactory.getLogger(CommitterImpl.class);
  private IcebergSinkConfig config;
  private Set<TopicPartition> topicPartitions;
  private Catalog catalog;
  private CoordinatorThread coordinatorThread;
  private CommitRequestListener commitRequestListener;
  private ConsumerGroupMetadata consumerGroupMetadata;
  private Map<TopicPartition, Pair<UUID, Producer<String, byte[]>>> producersByPartition;

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

  CommitterImpl(
      SinkTaskContext context,
      IcebergSinkConfig config,
      Set<TopicPartition> topicPartitions,
      Catalog catalogArg,
      Factory<Admin> adminFactory,
      Factory<Consumer<String, byte[]>> consumerFactory,
      Factory<Producer<String, byte[]>> producerFactory) {
    this.config = config;
    this.topicPartitions = topicPartitions;
    this.catalog = catalogArg;
    this.consumerGroupMetadata = getConsumerGroupMetadata(context, this.config);

    if (isLeader()) {
      LOG.info("Task elected leader");
      if (this.config.controlClusterMode()) {
        createControlClusterTopics(adminFactory);
      }
      startCoordinator(adminFactory, consumerFactory, producerFactory);
    }

    this.commitRequestListener =
        new ControlTopicCommitRequestListener(this.config, consumerFactory);

    this.producersByPartition = Maps.newHashMap();
    this.topicPartitions.forEach(
        topicPartition -> {
          UUID producerId = UUID.randomUUID();
          Map<String, String> producerProps =
              Maps.newHashMap(this.config.controlClusterKafkaProps());
          producerProps.put(
              ProducerConfig.TRANSACTIONAL_ID_CONFIG,
              // TODO: think carefully about what groupId to use here
              String.format("%s-%s", consumerGroupMetadata.groupId(), topicPartition));
          Producer<String, byte[]> producer = producerFactory.create(producerProps);

          producersByPartition.put(topicPartition, Pair.of(producerId, producer));
        });

    // All producers have been initialized now
    // Which means all zombies have been fenced at this point
    // Which means we can retrieve stable consumer offsets now
    Map<TopicPartition, Long> stableGroupOffsets =
        getStableGroupOffsets(
            adminFactory, this.config, consumerGroupMetadata, this.topicPartitions);

    // Rewind kafka-connect to read from safe offsets
    context.offset(stableGroupOffsets);
  }

  private boolean isLeader() {
    return Objects.equals(config.taskId(), 0);
  }

  private void createControlClusterTopics(Factory<Admin> adminFactory) {
    try (Admin sourceAdmin = adminFactory.create(config.sourceClusterKafkaProps());
        Admin controlAdmin = adminFactory.create(config.controlClusterKafkaProps())) {
      // TODO a few options here:
      // - Create a corresponding topics for each topic we're reading from in control cluster
      //  - this handles topics.regex case well
      // - Create a user-specified control-cluster-source-topic which has at least as many
      // partitions as topics we're reading from here (this minimizes number of topics to create)
      //  - does this even work for multi-topic connectors? No.
      //  - This means I now to have plumb this special topic name all the way through to our
      // PartitionWorker (not too bad but damn it's ugly)
      // - Create a user-specific-control-cluster-source-topic and then have a different
      // consumer-group-id that we commit to for each topic
      //  - now the loadOffsets command also has to change to understand this ... it has to know
      // all the consumer-group-ids that existed before
      //  - you want to reset offsets, welcome to nightmare
      //  - hacky hacky hacky hacky hacky

      ConsumerGroupDescription groupDesc =
          KafkaUtils.consumerGroupDescription(config.connectGroupId(), sourceAdmin);
      if (groupDesc.state() == ConsumerGroupState.STABLE) {
        // TODO: support creating new topics with a prefix
        List<NewTopic> newTopics =
            groupDesc.members().stream()
                .flatMap(member -> member.assignment().topicPartitions().stream())
                .collect(Collectors.groupingBy(TopicPartition::topic, Collectors.toSet()))
                .entrySet()
                .stream()
                .map(e -> new NewTopic(e.getKey(), e.getValue().size(), (short) 1))
                .collect(Collectors.toList());

        // TODO: what happens if topic already exists? :)
        try {
          controlAdmin.createTopics(newTopics).all().get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      } else {
        throw new RuntimeException("Group not stable");
      }
    }
  }

  private void startCoordinator(
      Factory<Admin> adminFactory,
      Factory<Consumer<String, byte[]>> consumerFactory,
      Factory<Producer<String, byte[]>> producerFactory) {
    try (Admin admin = adminFactory.create(config.sourceClusterKafkaProps())) {
      ConsumerGroupDescription groupDesc =
          KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin);
      if (groupDesc.state() == ConsumerGroupState.STABLE) {
        LOG.info("Group is stable, starting commit coordinator");
        Collection<MemberDescription> members = groupDesc.members();
        coordinatorThread =
            new CoordinatorThread(catalog, config, members, consumerFactory, producerFactory);
        coordinatorThread.start();
        LOG.info("Started commit coordinator");
      } else {
        throw new RuntimeException("Could not start coordinator");
      }
    }
  }

  private static ConsumerGroupMetadata getConsumerGroupMetadata(
      SinkTaskContext context, IcebergSinkConfig config) {
    final ConsumerGroupMetadata consumerGroupMetadata;
    if (config.controlClusterMode()) {
      // TODO: we could also just the connect-consumer-group-id here
      consumerGroupMetadata = new ConsumerGroupMetadata(config.controlGroupId());
    } else {
      // TODO: this is a breaking change in single-cluster-mode, avoid until 1.0 release
      // should really use controlGroupId to be backwards compatible (for now)
      consumerGroupMetadata = extractConsumer(context).groupMetadata();
    }

    return consumerGroupMetadata;
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

  private static Map<TopicPartition, Long> getStableGroupOffsets(
      Factory<Admin> adminFactory,
      IcebergSinkConfig config,
      ConsumerGroupMetadata consumerGroupMetadata,
      Set<TopicPartition> topicPartitions) {
    Map<String, String> adminProps = Maps.newHashMap(config.controlClusterKafkaProps());
    try (Admin admin = adminFactory.create(adminProps)) {
      return admin
          .listConsumerGroupOffsets(
              consumerGroupMetadata.groupId(),
              // TODO: test admin client respects requireStable or not
              new ListConsumerGroupOffsetsOptions().requireStable(true))
          .partitionsToOffsetAndMetadata().get().entrySet().stream()
          .filter(entry -> entry.getValue() != null)
          .filter(entry -> topicPartitions.contains(entry.getKey()))
          .collect(Collectors.toMap(Map.Entry::getKey, (entry) -> entry.getValue().offset()));

    } catch (ExecutionException | InterruptedException e) {
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

                final Long newConsumerOffset = committable.offset().offset();
                final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit;
                if (newConsumerOffset != null) {
                  offsetsToCommit =
                      ImmutableMap.of(
                          committable.topicPartition(), new OffsetAndMetadata(newConsumerOffset));
                } else {
                  offsetsToCommit = ImmutableMap.of();
                }

                Pair<UUID, Producer<String, byte[]>> pair =
                    producersByPartition.get(committable.topicPartition());
                UUID producerId = pair.first();
                Producer<String, byte[]> producer = pair.second();

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
    config = null;
    topicPartitions = null;
    consumerGroupMetadata = null;

    if (coordinatorThread != null) {
      coordinatorThread.terminate();
      coordinatorThread = null;
    }

    if (catalog != null) {
      if (catalog instanceof AutoCloseable) {
        try {
          ((AutoCloseable) catalog).close();
        } catch (Exception e) {
          LOG.warn("An error occurred closing catalog instance, ignoring...", e);
        }
      }
      catalog = null;
    }

    commitRequestListener.close();
    commitRequestListener = null;

    producersByPartition.values().forEach(pair -> pair.second().close());
    producersByPartition = null;
  }
}
