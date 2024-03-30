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

import static java.util.stream.Collectors.toList;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.api.Committable;
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.Offset;
import io.tabular.iceberg.connect.data.RecordWriter;
import io.tabular.iceberg.connect.data.Utilities;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PartitionWorker implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionWorker.class);
  private final IcebergSinkConfig config;
  private final TopicPartition topicPartition;
  private final IcebergWriterFactory writerFactory;
  private final Map<String, RecordWriter> writersByTable;
  private Offset sourceOffset;

  /**
   * It is illegal to attempt to use a Worker after the close method has been called. To avoid any
   * runtime penalties, we perform no checking. It is the caller's responsibility to ensure that a
   * WorkerImpl instance is not reused after it has been closed.
   */
  PartitionWorker(
      IcebergSinkConfig config, TopicPartition topicPartition, IcebergWriterFactory writerFactory) {
    this.config = config;
    this.topicPartition = topicPartition;
    this.writerFactory = writerFactory;
    this.writersByTable = Maps.newHashMap();
    this.sourceOffset = Offset.NULL_OFFSET;
  }

  /**
   * Writes the given record to a file.
   *
   * <p>Assumes records will be received in increasing offset order.
   */
  public void put(Collection<SinkRecord> sinkRecords) {
    sinkRecords.forEach(
        record -> {
          // the consumer stores the offsets that corresponds to the next record to consume,
          // so increment the record offset by one
          sourceOffset = new Offset(record.kafkaOffset() + 1, record.timestamp());

          if (config.dynamicTablesEnabled()) {
            routeRecordDynamically(record);
          } else {
            routeRecordStatically(record);
          }
        });
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
        tableName, tblName -> writerFactory.createWriter(tblName, sample, ignoreMissingTable));
  }

  public Committable getCommittable() {
    Committable committable =
        new Committable(
            topicPartition,
            sourceOffset,
            writersByTable.values().stream()
                .flatMap(writer -> writer.complete().stream())
                .collect(toList()));

    clearWriters();
    // TODO: not convinced I need to clear offset if this worker is going to be reused
    clearOffset();

    return committable;
  }

  private void clearWriters() {
    for (Map.Entry<String, RecordWriter> entry : writersByTable.entrySet()) {
      String tableName = entry.getKey();
      RecordWriter writer = entry.getValue();
      try {
        writer.close();
      } catch (IOException e) {
        LOG.warn(
            "An error occurred closing RecordWriter instance for table={}, ignoring...",
            tableName,
            e);
      }
    }
    writersByTable.clear();
  }

  private void clearOffset() {
    sourceOffset = Offset.NULL_OFFSET;
  }

  public void close() {
    clearWriters();
    clearOffset();
  }
}
