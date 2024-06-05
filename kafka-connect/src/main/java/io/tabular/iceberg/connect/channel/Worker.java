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


import io.tabular.iceberg.connect.IcebergSinkConfig;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.Offset;
import io.tabular.iceberg.connect.data.RecordRouter;
import io.tabular.iceberg.connect.data.WriterManager;
import io.tabular.iceberg.connect.data.WriterResult;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

// TODO: rename to WriterImpl later, minimize changes for clearer commit history for now
class Worker implements Writer, AutoCloseable {
    private final WriterManager writers;

    private final Map<TopicPartition, Offset> sourceOffsets;
    private final RecordRouter recordRouter;

  Worker(SinkTaskContext context, IcebergSinkConfig config, Catalog catalog) {
    this(context, config, new IcebergWriterFactory(catalog, config));
  }

  @VisibleForTesting
  Worker(SinkTaskContext context, IcebergSinkConfig config, IcebergWriterFactory writerFactory) {
    this.writers = new WriterManager(writerFactory);
    this.sourceOffsets = Maps.newHashMap();
    this.recordRouter =
              RecordRouter.from(writers, config, this.getClass().getClassLoader(), context);

  }

  @Override
  public Committable committable() {
    List<WriterResult> writeResults = writers.writeResults();
    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    writers.clear();
    sourceOffsets.clear();

    return new Committable(offsets, writeResults);
  }

  @Override
  public void close() throws IOException {
    writers.stop();
    writers.clear();
    sourceOffsets.clear();
  }

  @Override
  public void write(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      sinkRecords.forEach(this::save);
    }
  }

  private void save(SinkRecord record) {
    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    sourceOffsets.put(
        new TopicPartition(record.topic(), record.kafkaPartition()),
        new Offset(record.kafkaOffset() + 1, record.timestamp()));

    recordRouter.write(record);
  }
}
