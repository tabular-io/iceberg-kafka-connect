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
package io.tabular.iceberg.connect;

import io.tabular.iceberg.connect.internal.coordinator.Coordinator;
import io.tabular.iceberg.connect.internal.coordinator.CoordinatorThread;
import io.tabular.iceberg.connect.internal.data.Utilities;
import io.tabular.iceberg.connect.internal.kafka.AdminFactory;
import io.tabular.iceberg.connect.internal.kafka.ConsumerFactory;
import io.tabular.iceberg.connect.internal.kafka.Factory;
import io.tabular.iceberg.connect.internal.kafka.KafkaUtils;
import io.tabular.iceberg.connect.internal.kafka.TransactionalProducerFactory;
import io.tabular.iceberg.connect.internal.worker.WorkerManager;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkTask extends SinkTask {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkTask.class);

  private IcebergSinkConfig config;
  private Catalog catalog;
  private CoordinatorThread coordinatorThread;
  private WorkerManager workerManager;

  @Override
  public String version() {
    return IcebergSinkConfig.version();
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new IcebergSinkConfig(props);
  }

  private boolean isLeader() {
    return Objects.equals(config.taskId(), 0);
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    // destroy any state if KC re-uses object
    clearObjectState();

    // TODO: are catalogs thread-safe objects? Both coordinator and worker could use catalog at the
    // same time.
    catalog = Utilities.loadCatalog(config);
    AdminFactory adminFactory = new AdminFactory();
    ConsumerFactory consumerFactory = new ConsumerFactory();
    TransactionalProducerFactory producerFactory = new TransactionalProducerFactory();

    if (isLeader()) {
      LOG.info("Task elected leader");

      if (config.controlClusterMode()) {
        createControlClusterTopic(adminFactory);
      }

      startCoordinator(adminFactory, consumerFactory, producerFactory);
    }

    LOG.info("Starting worker manager");
    this.workerManager =
        new WorkerManager(
            context, config, partitions, catalog, consumerFactory, producerFactory, adminFactory);
    LOG.info("Started worker manager");
  }

  private void createControlClusterTopic(Factory<Admin> adminFactory) {
    try (Admin sourceAdmin = adminFactory.create(config.sourceClusterKafkaProps())) {
      try (Admin controlAdmin = adminFactory.create(config.controlClusterKafkaProps())) {
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
          throw new NotRunningException("Group not stable");
        }
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
        Coordinator coordinator =
            new Coordinator(catalog, config, members, consumerFactory, producerFactory);
        coordinatorThread = new CoordinatorThread(coordinator);
        coordinatorThread.start();
        LOG.info("Started commit coordinator");
      } else {
        throw new NotRunningException("Could not start coordinator");
      }
    }
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    clearObjectState();
  }

  private void clearObjectState() {
    if (workerManager != null) {
      try {
        workerManager.close();
      } catch (IOException e) {
        LOG.warn("An error occurred closing worker manager instance, ignoring...", e);
      }
      workerManager = null;
    }

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
  }

  private void throwExceptionIfCoordinatorIsTerminated() {
    if (isLeader() && (coordinatorThread == null || coordinatorThread.isTerminated())) {
      throw new NotRunningException("Coordinator unexpectedly terminated");
    }
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    throwExceptionIfCoordinatorIsTerminated();
    workerManager.save(sinkRecords);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    throwExceptionIfCoordinatorIsTerminated();
    workerManager.commit();
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    return ImmutableMap.of();
  }

  @Override
  public void stop() {
    clearObjectState();
  }
}
