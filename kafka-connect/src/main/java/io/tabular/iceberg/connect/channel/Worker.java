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
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class Worker extends Channel {

  private final IcebergWriter writer;
  private final SinkTaskContext context;
  private final String controlGroupId;

  public Worker(
      Catalog catalog,
      TableIdentifier tableIdentifier,
      IcebergSinkConfig config,
      SinkTaskContext context) {
    super("worker", config);
    this.writer = new IcebergWriter(catalog, tableIdentifier);
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
  protected void receive(Event event) {
    if (event.getType() == EventType.COMMIT_REQUEST) {
      WriterResult writeResult = writer.complete();

      // include all assigned topic partitions even if no messages were read
      // from a partition, as the coordinator will use that to determine
      // when all data for a commit has been received
      List<TopicPartitionOffset> assignments =
          context.assignment().stream()
              .map(
                  tp -> {
                    Long offset = writeResult.getOffsets().get(tp);
                    return new TopicPartitionOffset(tp.topic(), tp.partition(), offset);
                  })
              .collect(toList());

      UUID commitId = ((CommitRequestPayload) event.getPayload()).getCommitId();

      Event filesEvent =
          new Event(
              EventType.COMMIT_RESPONSE,
              new CommitResponsePayload(
                  writeResult.getPartitionStruct(),
                  commitId,
                  TableName.of(writeResult.getTableIdentifier()),
                  writeResult.getDataFiles(),
                  assignments));

      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      writeResult.getOffsets().forEach((k, v) -> offsets.put(k, new OffsetAndMetadata(v)));

      send(filesEvent, offsets);
      context.requestCommit();
    }
  }

  @Override
  public void stop() {
    super.stop();
    writer.close();
  }

  public void save(Collection<SinkRecord> sinkRecords) {
    writer.write(sinkRecords);
  }
}
