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
package io.tabular.iceberg.connect.internal.worker;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.internal.kafka.Factory;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MultiPartitionWorker implements Worker {
  private static final Logger LOG = LoggerFactory.getLogger(MultiPartitionWorker.class);
  private final Map<TopicPartition, Worker> workers;

  MultiPartitionWorker(
      SinkTaskContext context,
      IcebergSinkConfig config,
      Collection<TopicPartition> topicPartitions,
      PartitionWorkerFactory partitionWorkerFactory,
      Factory<Admin> adminFactory) {
    this(
        context,
        getConsumerGroupMetadata(context, config),
        config,
        topicPartitions,
        partitionWorkerFactory,
        adminFactory);
  }

  private static ConsumerGroupMetadata getConsumerGroupMetadata(
      SinkTaskContext context, IcebergSinkConfig config) {
    final ConsumerGroupMetadata consumerGroupMetadata;
    if (config.controlClusterMode()) {
      // TODO: we could also just the connect-consumer-group-id here
      consumerGroupMetadata = new ConsumerGroupMetadata(config.controlGroupId());
    } else {
      // TODO: this is a breaking change, avoid until 1.0 release
      consumerGroupMetadata = extractConsumer(context).groupMetadata();
    }

    return consumerGroupMetadata;
  }

  private static Consumer<byte[], byte[]> extractConsumer(SinkTaskContext context) {
    try {
      WorkerSinkTaskContext workerContext = (WorkerSinkTaskContext) context;
      Field field = workerContext.getClass().getDeclaredField("consumer");
      field.setAccessible(true);
      return (Consumer<byte[], byte[]>) field.get(workerContext);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  MultiPartitionWorker(
      SinkTaskContext context,
      ConsumerGroupMetadata consumerGroupMetadata,
      IcebergSinkConfig config,
      Collection<TopicPartition> topicPartitions,
      PartitionWorkerFactory partitionWorkerFactory,
      Factory<Admin> adminFactory) {
    this.workers = Maps.newHashMap();

    topicPartitions.forEach(
        topicPartition -> {
          final Worker worker =
              partitionWorkerFactory.createWorker(consumerGroupMetadata, topicPartition);
          LOG.info("Created worker to handle topic-partition={}", topicPartition.toString());
          workers.put(topicPartition, worker);
        });

    // All workers have been initialized
    // Which means all zombies have been fenced at this point
    // Which means we can safely retrieve committed consumer offsets now
    Map<TopicPartition, Long> safeOffsets;
    // TODO: test admin client respects requireStable or not
    Map<String, String> adminProps = Maps.newHashMap(config.controlClusterKafkaProps());
    try (Admin admin = adminFactory.create(adminProps)) {
      safeOffsets =
          admin
              .listConsumerGroupOffsets(
                  // TODO: This is a breaking change, just for demonstration purposes
                  // should really use controlGroupId to be backwards compatible (for now)
                  consumerGroupMetadata.groupId(),
                  new ListConsumerGroupOffsetsOptions().requireStable(true))
              .partitionsToOffsetAndMetadata().get().entrySet().stream()
              .filter(entry -> entry.getValue() != null)
              .filter(entry -> topicPartitions.contains(entry.getKey()))
              .collect(Collectors.toMap(Map.Entry::getKey, (entry) -> entry.getValue().offset()));
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    // And rewind kafka-connect to read from those offsets
    context.offset(safeOffsets);
  }

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    // TODO: double check this doesn't mess up ordering, add tests for it
    sinkRecords.stream()
        .collect(
            Collectors.groupingBy(
                r -> new TopicPartition(r.topic(), r.kafkaPartition()), Collectors.toList()))
        .forEach(
            ((topicPartition, records) -> {
              Worker worker = workers.get(topicPartition);
              worker.save(records);
            }));
  }

  @Override
  public void commit(UUID commitId) {
    // TODO: commit can throw errors, should work through each worker and throw error
    // only afterwards
    workers.values().forEach(worker -> worker.commit(commitId));
  }

  @Override
  public void close() {
    workers.values().forEach(Worker::close);
    workers.clear();
  }
}
