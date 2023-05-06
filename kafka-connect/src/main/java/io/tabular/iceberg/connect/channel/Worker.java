// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.channel.events.CommitRequestPayload;
import io.tabular.iceberg.connect.channel.events.CommitResponsePayload;
import io.tabular.iceberg.connect.channel.events.Event;
import io.tabular.iceberg.connect.channel.events.EventType;
import io.tabular.iceberg.connect.channel.events.TableName;
import io.tabular.iceberg.connect.channel.events.TopicPartitionOffset;
import io.tabular.iceberg.connect.data.IcebergWriter;
import io.tabular.iceberg.connect.data.WriterResult;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class Worker extends Channel {
  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final Map<String, IcebergWriter> writers;
  private final SinkTaskContext context;
  private final String controlGroupId;

  public Worker(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context) {
    super("worker", config);
    this.catalog = catalog;
    this.config = config;
    this.writers = new HashMap<>();
    this.context = context;
    this.controlGroupId = config.getControlGroupId();
  }

  public void syncCommitOffsets() {
    Map<TopicPartition, Long> offsets =
        getCommitOffsets().entrySet().stream()
            .collect(toMap(Entry::getKey, entry -> entry.getValue().offset()));
    context.offset(offsets);
  }

  public Map<TopicPartition, OffsetAndMetadata> getCommitOffsets() {
    try {
      ListConsumerGroupOffsetsResult response = admin().listConsumerGroupOffsets(controlGroupId);
      return response.partitionsToOffsetAndMetadata().get().entrySet().stream()
          .filter(entry -> context.assignment().contains(entry.getKey()))
          .collect(toMap(Entry::getKey, Entry::getValue));
    } catch (InterruptedException | ExecutionException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  protected void receive(Envelope envelope) {
    Event event = envelope.getEvent();
    if (event.getType() == EventType.COMMIT_REQUEST) {
      List<WriterResult> writeResults =
          writers.values().stream().map(IcebergWriter::complete).collect(toList());

      Map<TopicPartition, Long> offsets = new HashMap<>();
      writeResults.stream()
          .flatMap(writerResult -> writerResult.getOffsets().entrySet().stream())
          .forEach(entry -> offsets.merge(entry.getKey(), entry.getValue(), Long::max));

      // include all assigned topic partitions even if no messages were read
      // from a partition, as the coordinator will use that to determine
      // when all data for a commit has been received
      List<TopicPartitionOffset> assignments =
          context.assignment().stream()
              .map(
                  tp -> {
                    Long offset = offsets.get(tp);
                    return new TopicPartitionOffset(tp.topic(), tp.partition(), offset);
                  })
              .collect(toList());

      UUID commitId = ((CommitRequestPayload) event.getPayload()).getCommitId();

      List<Event> events =
          writeResults.stream()
              .map(
                  writeResult ->
                      new Event(
                          EventType.COMMIT_RESPONSE,
                          new CommitResponsePayload(
                              writeResult.getPartitionStruct(),
                              commitId,
                              TableName.of(writeResult.getTableIdentifier()),
                              writeResult.getDataFiles(),
                              writeResult.getDeleteFiles(),
                              assignments)))
              .collect(toList());

      send(events, offsets);
      context.requestCommit();
    }
  }

  @Override
  public void stop() {
    super.stop();
    writers.values().forEach(IcebergWriter::close);
  }

  public void save(Collection<SinkRecord> sinkRecords) {
    sinkRecords.forEach(this::save);
  }

  private void save(SinkRecord record) {
    String routeField = config.getTablesRouteField();

    if (routeField == null) {
      // route to all tables
      config
          .getTables()
          .forEach(
              tableName -> {
                getWriterForTable(tableName).write(record);
              });

    } else {
      String routeValue = extractRouteValue(record.value(), routeField);
      if (routeValue != null) {
        config
            .getTables()
            .forEach(
                tableName -> {
                  Pattern tableRouteValues = config.getTableRouteValues(tableName);
                  if (tableRouteValues != null && tableRouteValues.matcher(routeValue).matches()) {
                    getWriterForTable(tableName).write(record);
                  }
                });
      }
    }
  }

  private String extractRouteValue(Object recordValue, String routeField) {
    if (recordValue == null) {
      return null;
    }

    Object routeValue;
    if (recordValue instanceof Struct) {
      routeValue = ((Struct) recordValue).get(routeField).toString();
    } else if (recordValue instanceof Map) {
      routeValue = ((Map<?, ?>) recordValue).get(routeField).toString();
    } else {
      throw new UnsupportedOperationException(
          "Cannot extract value from type: " + recordValue.getClass().getName());
    }

    return routeValue == null ? null : routeValue.toString();
  }

  private IcebergWriter getWriterForTable(String tableName) {
    return writers.computeIfAbsent(
        tableName, notUsed -> new IcebergWriter(catalog, tableName, config));
  }
}
