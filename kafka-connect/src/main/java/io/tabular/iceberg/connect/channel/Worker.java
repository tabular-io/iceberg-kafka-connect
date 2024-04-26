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
import io.tabular.iceberg.connect.data.RecordRouter;
import io.tabular.iceberg.connect.data.WriterManager;
import io.tabular.iceberg.connect.data.WriterResult;
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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class Worker extends Channel {
  private final IcebergSinkConfig config;
  private final SinkTaskContext context;
  private final String controlGroupId;
  private final WriterManager writers;
  private final Map<TopicPartition, Offset> sourceOffsets;

  private final RecordRouter recordRouter;

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
    this.writers = new WriterManager(writerFactory);
    this.context = context;
    this.controlGroupId = config.controlGroupId();
    this.sourceOffsets = Maps.newHashMap();
    this.recordRouter =
        RecordRouter.from(writers, config, this.getClass().getClassLoader(), context);
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

    List<WriterResult> writeResults = writers.writeResults();
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
    writers.stop();
  }

  public void save(Collection<SinkRecord> sinkRecords) {
    sinkRecords.forEach(this::save);
  }

  private void save(SinkRecord record) {

    sourceOffsets.put(
        new TopicPartition(record.topic(), record.kafkaPartition()),
        new Offset(record.kafkaOffset() + 1, record.timestamp()));

    recordRouter.write(record);
  }
}
