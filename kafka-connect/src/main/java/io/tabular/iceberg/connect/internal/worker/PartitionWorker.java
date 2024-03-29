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

import static java.util.stream.Collectors.toList;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.events.CommitReadyPayload;
import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventType;
import io.tabular.iceberg.connect.events.TableName;
import io.tabular.iceberg.connect.events.TopicPartitionOffset;
import io.tabular.iceberg.connect.internal.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.internal.data.Offset;
import io.tabular.iceberg.connect.internal.data.RecordWriter;
import io.tabular.iceberg.connect.internal.data.Utilities;
import io.tabular.iceberg.connect.internal.data.WriterResult;
import io.tabular.iceberg.connect.internal.kafka.Factory;
import io.tabular.iceberg.connect.internal.kafka.KafkaUtils;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PartitionWorker implements Worker {

  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
  private final IcebergSinkConfig config;
  private final TopicPartition topicPartition;
  private final ConsumerGroupMetadata consumerGroupMetadata;
  private final IcebergWriterFactory writerFactory;
  private final Map<String, RecordWriter> writers;
  private Offset sourceOffset;
  private final Producer<String, byte[]> producer;
  private final UUID producerId;

  /**
   * It is illegal to attempt to use a Worker after the close method has been called. To avoid any
   * runtime penalties, we perform no checking. It is the caller's responsibility to ensure that a
   * WorkerImpl instance is not reused after it has been closed.
   */
  PartitionWorker(
      IcebergSinkConfig config,
      TopicPartition topicPartition,
      ConsumerGroupMetadata consumerGroupMetadata,
      IcebergWriterFactory writerFactory,
      Factory<Producer<String, byte[]>> transactionalProducerFactory) {
    this.config = config;
    this.topicPartition = topicPartition;
    this.consumerGroupMetadata = consumerGroupMetadata;
    this.writerFactory = writerFactory;
    this.writers = Maps.newHashMap();
    this.sourceOffset = Offset.NULL_OFFSET;

    Map<String, String> producerProps = Maps.newHashMap(config.controlClusterKafkaProps());
    producerProps.put(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
        // TODO: think about what groupId to use here
        String.format("%s-%s", config.controlGroupId(), this.topicPartition));
    this.producer = transactionalProducerFactory.create(producerProps);
    this.producerId = UUID.randomUUID();
  }

  /**
   * Writes the given record to a file.
   *
   * <p>Assumes records will be received in increasing offset order.
   */
  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
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
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }

  @Override
  public void commit(UUID commitId) {
    List<WriterResult> writeResults =
        writers.values().stream().flatMap(writer -> writer.complete().stream()).collect(toList());

    // include assigned topic partition even if no messages were read
    // as the coordinator will use that to determine when all data for
    // a commit has been received
    TopicPartitionOffset assignment =
        new TopicPartitionOffset(
            topicPartition.topic(),
            topicPartition.partition(),
            sourceOffset.offset(),
            sourceOffset.timestamp());

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
            new CommitReadyPayload(commitId, ImmutableList.of(assignment)));
    events.add(readyEvent);

    sendEventsAndCommitCurrentOffset(events);

    clearWriters();
    clearOffset();
  }

  private void sendEventsAndCommitCurrentOffset(List<Event> events) {
    final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit;
    if (Objects.equals(sourceOffset, Offset.NULL_OFFSET)) {
      offsetsToCommit = ImmutableMap.of();
    } else {
      offsetsToCommit =
          ImmutableMap.of(topicPartition, new OffsetAndMetadata(sourceOffset.offset()));
    }

    KafkaUtils.sendAndCommitOffsets(
        producer,
        producerId,
        config.controlTopic(),
        events,
        offsetsToCommit,
        consumerGroupMetadata);
  }

  private void clearWriters() {
    // this is unnecessary in the commit case, .complete() will close writer anyway
    // TODO: make it safe so that all other writers close even if one fails to close
    writers.values().forEach(RecordWriter::close);
    writers.clear();
  }

  private void clearOffset() {
    sourceOffset = Offset.NULL_OFFSET;
  }

  @Override
  public void close() {
    clearWriters();
    clearOffset();
    producer.close();
  }
}
