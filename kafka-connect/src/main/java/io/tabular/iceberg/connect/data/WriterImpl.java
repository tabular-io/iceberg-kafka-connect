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
package io.tabular.iceberg.connect.data;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.api.Committable;
import io.tabular.iceberg.connect.api.Writer;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterImpl implements Writer, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(WriterImpl.class);
  private final IcebergSinkConfig config;
  private final IcebergWriterFactory writerFactory;
  private final Map<String, RecordWriter> writersByTable;
  private final Map<TopicPartition, Offset> offsetsByTopicPartition;

  public WriterImpl(IcebergSinkConfig config) {
    this(config, new IcebergWriterFactory(config));
  }

  @VisibleForTesting
  WriterImpl(IcebergSinkConfig config, IcebergWriterFactory writerFactory) {
    this.config = config;
    this.writerFactory = writerFactory;
    this.writersByTable = Maps.newHashMap();
    this.offsetsByTopicPartition = Maps.newHashMap();
  }

  @Override
  public void write(Collection<SinkRecord> sinkRecords) {
    sinkRecords.forEach(this::put);
  }

  private void put(SinkRecord record) {
    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    offsetsByTopicPartition.put(
        new TopicPartition(record.topic(), record.kafkaPartition()),
        new Offset(record.kafkaOffset() + 1, record.timestamp()));

    if (config.dynamicTablesEnabled()) {
      routeRecordDynamically(record);
    } else {
      routeRecordStatically(record);
    }
  }

  private void routeRecordStatically(SinkRecord record) {
    String routeField = config.tablesRouteField();

    if (routeField == null) {
      // route to all tables
      config
          .tables()
          .forEach(
              tableName -> {
                writerForTable(tableName, record, false).write(record);
              });

    } else {
      String routeValue = extractRouteValue(record.value(), routeField);
      if (routeValue != null) {
        config
            .tables()
            .forEach(
                tableName ->
                    config
                        .tableConfig(tableName)
                        .routeRegex()
                        .ifPresent(
                            regex -> {
                              if (regex.matcher(routeValue).matches()) {
                                writerForTable(tableName, record, false).write(record);
                              }
                            }));
      }
    }
  }

  private void routeRecordDynamically(SinkRecord record) {
    String routeField = config.tablesRouteField();
    Preconditions.checkNotNull(routeField, "Route field cannot be null with dynamic routing");

    String routeValue = extractRouteValue(record.value(), routeField);
    if (routeValue != null) {
      String tableName = routeValue.toLowerCase();
      writerForTable(tableName, record, true).write(record);
    }
  }

  private String extractRouteValue(Object recordValue, String routeField) {
    if (recordValue == null) {
      return null;
    }
    Object routeValue = Utilities.extractFromRecordValue(recordValue, routeField);
    return routeValue == null ? null : routeValue.toString();
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writersByTable.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }

  @Override
  public Committable committable() {
    Committable committable =
        new Committable(
            offsetsByTopicPartition,
            writersByTable.values().stream()
                .flatMap(writer -> writer.complete().stream())
                .collect(Collectors.toList()));

    clearWriters();
    clearOffsets();

    return committable;
  }

  private void clearWriters() {
    for (Map.Entry<String, RecordWriter> entry : writersByTable.entrySet()) {
      String tableName = entry.getKey();
      RecordWriter writer = entry.getValue();
      try {
        writer.close();
      } catch (Exception e) {
        LOG.warn(
            "An error occurred closing RecordWriter instance for table={}, ignoring...",
            tableName,
            e);
      }
    }
    writersByTable.clear();
  }

  private void clearOffsets() {
    offsetsByTopicPartition.clear();
  }

  @Override
  public void close() throws IOException {
    clearWriters();
    clearOffsets();
    Utilities.close(writerFactory);
  }
}
