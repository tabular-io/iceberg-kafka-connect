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

import io.tabular.iceberg.connect.channel.Coordinator;
import io.tabular.iceberg.connect.channel.IcebergWriterFactory;
import io.tabular.iceberg.connect.channel.KafkaClientFactory;
import io.tabular.iceberg.connect.channel.Worker;
import io.tabular.iceberg.connect.data.Utilities;
import java.util.Collection;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkTask extends SinkTask {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkTask.class);

  private IcebergSinkConfig config;
  private Coordinator coordinator;
  private Worker worker;

  @Override
  public String version() {
    return IcebergSinkConfig.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new IcebergSinkConfig(props);
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    Catalog catalog = Utilities.loadCatalog(config);
    KafkaClientFactory clientFactory = new KafkaClientFactory(config.getKafkaProps());

    if (isLeader(partitions)) {
      LOG.info("Task elected leader, starting commit coordinator");
      coordinator = new Coordinator(catalog, config, clientFactory);
      coordinator.start();
    }

    LOG.info("Starting commit worker");
    IcebergWriterFactory writerFactory = new IcebergWriterFactory(catalog, config);
    worker = new Worker(catalog, config, clientFactory, writerFactory, context);
    worker.syncCommitOffsets();
    worker.start();
  }

  @VisibleForTesting
  boolean isLeader(Collection<TopicPartition> partitions) {
    // there should only be one worker assigned partition 0 of the first
    // topic, so elect that one the leader
    String firstTopic = config.getTopics().first();
    return partitions.stream()
        .filter(tp -> tp.topic().equals(firstTopic))
        .anyMatch(tp -> tp.partition() == 0);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    if (worker != null) {
      worker.stop();
      worker = null;
    }
    if (coordinator != null) {
      coordinator.stop();
      coordinator = null;
    }
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty() && worker != null) {
      worker.save(sinkRecords);
    }
    coordinate();
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    coordinate();
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    if (worker == null) {
      return ImmutableMap.of();
    }
    return worker.getCommitOffsets();
  }

  private void coordinate() {
    if (worker != null) {
      worker.process();
    }
    if (coordinator != null) {
      coordinator.process();
    }
  }

  @Override
  public void stop() {
    if (worker != null) {
      worker.stop();
    }
    if (coordinator != null) {
      coordinator.stop();
    }
  }
}
