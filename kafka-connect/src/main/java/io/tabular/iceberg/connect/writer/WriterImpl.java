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

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.api.Committable;
import io.tabular.iceberg.connect.api.Writer;
import io.tabular.iceberg.connect.data.IcebergWriterFactoryImpl;
import io.tabular.iceberg.connect.data.Utilities;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: refactor so only one record-writer-per-table like before? Is that possible given
// zombie-fencing with producer-generation.
public class WriterImpl implements Writer {
  private static final Logger LOG = LoggerFactory.getLogger(WriterImpl.class);
  private final Map<TopicPartition, PartitionWorker> partitionWorkers;

  public WriterImpl(IcebergSinkConfig config, Set<TopicPartition> topicPartitions) {
    this(config, topicPartitions, Utilities.loadCatalog(config));
  }

  @VisibleForTesting
  WriterImpl(IcebergSinkConfig config, Set<TopicPartition> topicPartitions, Catalog catalog) {
    this.partitionWorkers = Maps.newHashMap();
    topicPartitions.forEach(
        topicPartition -> {
          PartitionWorker worker =
              new PartitionWorker(
                  config, topicPartition, new IcebergWriterFactoryImpl(catalog, config));
          LOG.info("Created worker to handle topic-partition={}", topicPartition.toString());
          partitionWorkers.put(topicPartition, worker);
        });
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    // TODO: double check this doesn't mess up ordering, add tests for it
    sinkRecords.stream()
        .collect(
            Collectors.groupingBy(
                r -> new TopicPartition(r.topic(), r.kafkaPartition()), Collectors.toList()))
        .forEach(
            ((topicPartition, records) -> {
              PartitionWorker worker = partitionWorkers.get(topicPartition);
              worker.put(records);
            }));
  }

  @Override
  public List<Committable> committables() {
    List<Committable> committables =
        partitionWorkers.values().stream()
            .map(PartitionWorker::getCommittable)
            .collect(Collectors.toList());

    // TODO: clear partitionWorkers?

    return committables;
  }

  @Override
  public void close() {
    partitionWorkers.values().forEach(PartitionWorker::close);
    partitionWorkers.clear();
  }
}
