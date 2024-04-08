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
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.Offset;
import io.tabular.iceberg.connect.data.RecordWriter;
import io.tabular.iceberg.connect.data.Utilities;
import io.tabular.iceberg.connect.data.WriterResult;
import io.tabular.iceberg.connect.deadletter.DeadLetterUtils;
import io.tabular.iceberg.connect.events.CommitReadyPayload;
import io.tabular.iceberg.connect.events.CommitRequestPayload;
import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventType;
import io.tabular.iceberg.connect.events.TableName;
import io.tabular.iceberg.connect.events.TopicPartitionOffset;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class Worker extends Channel {

  private final IcebergSinkConfig config;
  private final SinkTaskContext context;
  private final String controlGroupId;
  private final Map<String, RecordWriter> writers;
  private final Map<TopicPartition, Offset> sourceOffsets;
  private final RecordRouter recordRouter;

  public interface WriterForTable {
    void write(String tableName, SinkRecord sample, boolean ignoreMissingTable);

    void writeFailed(String namespace, SinkRecord sample, String location, Throwable error);
  }

  private static class BaseWriterForTable implements WriterForTable {

    private final IcebergWriterFactory writerFactory;
    private final Map<String, RecordWriter> writers;

    BaseWriterForTable(IcebergWriterFactory writerFactory, Map<String, RecordWriter> writers) {
      this.writerFactory = writerFactory;
      this.writers = writers;
    }

    @Override
    public void write(String tableName, SinkRecord sample, boolean ignoreMissingTable) {
      writers
          .computeIfAbsent(
              tableName,
              notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable))
          .write(sample);
    }

    @Override
    public void writeFailed(String namespace, SinkRecord sample, String location, Throwable error) {
      throw new IllegalArgumentException("BaseWriterForTable cannot write failed records", error);
    }
  }

  public static class DeadLetterWriterForTable implements WriterForTable {
    private static final String PAYLOAD_KEY = "transformed";
    private static final String ORIGINAL_BYTES_KEY = "original";
    private static final String KEY_BYTES = "key";
    private static final String VALUE_BYTES = "value";
    private static final String HEADERS = "headers";

    private static final String ICEBERG_TRANSFORMATION_LOCATION = "ICEBERG_TRANSFORM";
    private final IcebergWriterFactory writerFactory;
    private final Map<String, RecordWriter> writers;
    private final String deadLetterTableSuffix;

    private final String deadLetterTopicNamespace;

    DeadLetterWriterForTable(
        IcebergWriterFactory writerFactory,
        Map<String, RecordWriter> writers,
        IcebergSinkConfig config) {
      this.writerFactory = writerFactory;
      this.writers = writers;
      this.deadLetterTableSuffix = config.deadLetterTableSuffix();
      this.deadLetterTopicNamespace = config.deadLetterTableSuffix();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(String tableName, SinkRecord sample, boolean ignoreMissingTable) {
      if (sample.value() != null) {
        if (sample.value() instanceof Map) {
          RecordWriter writer;
          Map<String, Object> payload = (Map<String, Object>) sample.value();
          SinkRecord transformed = (SinkRecord) payload.get(PAYLOAD_KEY);
          if (isFailed(transformed)) {
            String deadLetterTableName = deadLetterTableName(tableName);
            Struct transformedStruct = (Struct) transformed.value();
            transformedStruct.put("target_table", tableName);

            writer =
                writers.computeIfAbsent(
                    deadLetterTableName,
                    notUsed ->
                        writerFactory.createWriter(
                            deadLetterTableName, transformed, ignoreMissingTable));
          } else {
            writer =
                writers.computeIfAbsent(
                    tableName,
                    notUsed ->
                        writerFactory.createWriter(tableName, transformed, ignoreMissingTable));
          }
          try {
            writer.write(sample);
          } catch (Exception e) {
            String deadLetterTableName = deadLetterTableName(tableName);
            SinkRecord newRecord =
                DeadLetterUtils.mapToFailedRecord(
                    tableName, sample, ICEBERG_TRANSFORMATION_LOCATION, e);
            writers
                .computeIfAbsent(
                    deadLetterTableName,
                    notUsed ->
                        writerFactory.createWriter(
                            deadLetterTableName, newRecord, ignoreMissingTable))
                .write(newRecord);
          }
        } else {
          throw new IllegalArgumentException("Record not in format expected for dead letter table");
        }
      }
    }

    @Override
    public void writeFailed(String namespace, SinkRecord sample, String location, Throwable error) {
      String deadLetterTableName = deadLetterTableName(namespace, sample);
      SinkRecord newRecord = DeadLetterUtils.mapToFailedRecord(null, sample, location, error);
      writers
          .computeIfAbsent(
              deadLetterTableName,
              notUsed -> writerFactory.createWriter(deadLetterTableName, newRecord, false))
          .write(newRecord);
    }

    private boolean isFailed(SinkRecord record) {
      return "true".equals(record.valueSchema().parameters().get("isFailed"));
    }

    private String deadLetterTableName(String namespace, SinkRecord record) {
      return String.format("%s.%s_%s", namespace, record.topic(), deadLetterTableSuffix);
    }

    private String deadLetterTableName(String originalTableName) {
      return originalTableName + deadLetterTableSuffix;
    }
  }

  private static class RouteExtractor {
    public static class RouteValueResult {
      private String value = null;
      private Throwable error = null;

      RouteValueResult(String value) {
        this.value = value;
      }

      RouteValueResult(Throwable error) {
        this.error = error;
      }

      String getValue() {
        return this.value;
      }

      Throwable getError() {
        return error;
      }
    }

    RouteValueResult extract(Object recordValue, String routeField) {
      if (recordValue == null) {
        return null;
      }
      Object routeValue = Utilities.extractFromRecordValue(recordValue, routeField);
      return routeValue == null ? null : new RouteValueResult(routeValue.toString());
    }
  }

  private static class ErrorHandlingRouteExtractor extends RouteExtractor {
    @Override
    RouteValueResult extract(Object recordValue, String routeField) {
      RouteValueResult result;
      try {
        result = super.extract(recordValue, routeField);
      } catch (Exception error) {
        result = new RouteValueResult(error);
      }
      return result;
    }
  }

  private abstract static class RecordRouter {

    void write(SinkRecord record) {}
  }

  private class ConfigRecordRouter extends RecordRouter {
    private final WriterForTable writerForTable;

    ConfigRecordRouter(WriterForTable writerForTable) {
      this.writerForTable = writerForTable;
    }

    @Override
    public void write(SinkRecord record) {
      // route to all tables
      config
          .tables()
          .forEach(
              tableName -> {
                writerForTable.write(tableName, record, false);
              });
    }
  }

  private class StaticRecordRouter extends RecordRouter {
    private final WriterForTable writerForTable;
    private final String routeField;

    private final RouteExtractor extractor;

    private final String deadLetterNamespace;

    StaticRecordRouter(
        WriterForTable writerForTable,
        String routeField,
        RouteExtractor extractor,
        String deadLetterNamespace) {
      this.writerForTable = writerForTable;
      this.routeField = routeField;
      this.extractor = extractor;
      this.deadLetterNamespace = deadLetterNamespace;
    }

    @Override
    public void write(SinkRecord record) {
      RouteExtractor.RouteValueResult routeValue = extractor.extract(record.value(), routeField);
      if (routeValue != null) {
        String route = routeValue.getValue();
        if (route != null) {
          config
              .tables()
              .forEach(
                  tableName ->
                      config
                          .tableConfig(tableName)
                          .routeRegex()
                          .ifPresent(
                              regex -> {
                                if (regex.matcher(route).matches()) {
                                  writerForTable.write(tableName, record, false);
                                }
                              }));
        } else if (routeValue.getError() != null) {
          writerForTable.writeFailed(
              deadLetterNamespace, record, "ROUTE_EXTRACT", routeValue.getError());
        }
      }
    }
  }

  private static class DynamicRecordRouter extends RecordRouter {
    private final WriterForTable writerForTable;
    private final String routeField;
    private final RouteExtractor extractor;

    private final String deadLetterNamespace;

    DynamicRecordRouter(
        WriterForTable writerForTable,
        String routeField,
        RouteExtractor extractor,
        String deadLetterNamespace) {
      this.writerForTable = writerForTable;
      this.routeField = routeField;
      this.extractor = extractor;
      this.deadLetterNamespace = deadLetterNamespace;
    }

    @Override
    public void write(SinkRecord record) {
      RouteExtractor.RouteValueResult routeValue = extractor.extract(record.value(), routeField);
      if (routeValue != null) {
        String route = routeValue.getValue();
        if (route != null) {
          String tableName = route.toLowerCase();
          writerForTable.write(tableName, record, true);
        } else if (routeValue.getError() != null) {
          writerForTable.writeFailed(
              deadLetterNamespace, record, "ROUTE_EXTRACT", routeValue.getError());
        }
      }
    }
  }

  public Worker(
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      IcebergWriterFactory writerFactory,
      SinkTaskContext context) {
    // pass transient consumer group ID to which we never commit offsets
    super(
        "worker",
        IcebergSinkConfig.DEFAULT_CONTROL_GROUP_PREFIX + UUID.randomUUID(),
        config,
        clientFactory);

    this.config = config;
    this.context = context;
    this.controlGroupId = config.controlGroupId();
    this.writers = Maps.newHashMap();

    RouteExtractor routeExtractor;

    WriterForTable writerForTable;
    if (config.deadLetterTableEnabled()) {
      writerForTable = new DeadLetterWriterForTable(writerFactory, this.writers, config);
      routeExtractor = new ErrorHandlingRouteExtractor();
    } else {
      writerForTable = new BaseWriterForTable(writerFactory, this.writers);
      routeExtractor = new RouteExtractor();
    }

    if (config.dynamicTablesEnabled()) {
      Preconditions.checkNotNull(
          config.tablesRouteField(), "Route field cannot be null with dynamic routing");
      recordRouter =
          new DynamicRecordRouter(
              writerForTable,
              config.tablesRouteField(),
              routeExtractor,
              config.deadLetterTopicNamespace());
    } else {
      if (config.tablesRouteField() == null) {
        recordRouter = new ConfigRecordRouter(writerForTable);
      } else {
        recordRouter =
            new StaticRecordRouter(
                writerForTable,
                config.tablesRouteField(),
                routeExtractor,
                config.deadLetterTopicNamespace());
      }
    }

    this.sourceOffsets = Maps.newHashMap();
  }

  public void syncCommitOffsets() {
    Map<TopicPartition, Long> offsets =
        commitOffsets().entrySet().stream()
            .collect(toMap(Entry::getKey, entry -> entry.getValue().offset()));
    context.offset(offsets);
  }

  public Map<TopicPartition, OffsetAndMetadata> commitOffsets() {
    try {
      ListConsumerGroupOffsetsResult response = admin().listConsumerGroupOffsets(controlGroupId);
      return response.partitionsToOffsetAndMetadata().get().entrySet().stream()
          .filter(entry -> context.assignment().contains(entry.getKey()))
          .collect(toMap(Entry::getKey, Entry::getValue));
    } catch (InterruptedException | ExecutionException e) {
      throw new ConnectException(e);
    }
  }

  public void process() {
    consumeAvailable(Duration.ZERO);
  }

  @Override
  protected boolean receive(Envelope envelope) {
    Event event = envelope.event();
    if (event.type() != EventType.COMMIT_REQUEST) {
      return false;
    }

    List<WriterResult> writeResults =
        writers.values().stream().flatMap(writer -> writer.complete().stream()).collect(toList());
    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    writers.clear();
    sourceOffsets.clear();

    // include all assigned topic partitions even if no messages were read
    // from a partition, as the coordinator will use that to determine
    // when all data for a commit has been received
    List<TopicPartitionOffset> assignments =
        context.assignment().stream()
            .map(
                tp -> {
                  Offset offset = offsets.get(tp);
                  if (offset == null) {
                    offset = Offset.NULL_OFFSET;
                  }
                  return new TopicPartitionOffset(
                      tp.topic(), tp.partition(), offset.offset(), offset.timestamp());
                })
            .collect(toList());

    UUID commitId = ((CommitRequestPayload) event.payload()).commitId();

    List<Event> events =
        writeResults.stream()
            .map(
                writeResult ->
                    new Event(
                        config.controlGroupId(),
                        EventType.COMMIT_RESPONSE,
                        new CommitResponsePayload(
                            writeResult.partitionStruct(),
                            commitId,
                            TableName.of(writeResult.tableIdentifier()),
                            writeResult.dataFiles(),
                            writeResult.deleteFiles())))
            .collect(toList());

    Event readyEvent =
        new Event(
            config.controlGroupId(),
            EventType.COMMIT_READY,
            new CommitReadyPayload(commitId, assignments));
    events.add(readyEvent);

    send(events, offsets);
    context.requestCommit();

    return true;
  }

  @Override
  public void stop() {
    super.stop();
    writers.values().forEach(RecordWriter::close);
  }

  public void save(Collection<SinkRecord> sinkRecords) {
    sinkRecords.forEach(this::save);
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
