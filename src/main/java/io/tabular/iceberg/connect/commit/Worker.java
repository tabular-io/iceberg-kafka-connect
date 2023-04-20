// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.commit;

import static java.util.stream.Collectors.toMap;

import io.tabular.iceberg.connect.IcebergWriter;
import io.tabular.iceberg.connect.commit.Message.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.SneakyThrows;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class Worker extends Channel {

  private static final String COMMIT_GROUP_ID_PROP = "iceberg.commit.group.id";

  private final IcebergWriter writer;
  private final SinkTaskContext context;
  private final String commitGroupId;
  private final Admin admin;

  public Worker(
      Catalog catalog,
      TableIdentifier tableIdentifier,
      Map<String, String> props,
      SinkTaskContext context) {
    super(props);
    this.writer = new IcebergWriter(catalog, tableIdentifier);
    this.context = context;
    this.commitGroupId = props.get(COMMIT_GROUP_ID_PROP);

    Map<String, Object> adminCliProps = new HashMap<>(kafkaProps);
    this.admin = Admin.create(adminCliProps);
  }

  public void syncCommitOffsets() {
    Map<TopicPartition, Long> offsets =
        getCommitOffsets().entrySet().stream()
            .collect(toMap(Entry::getKey, entry -> entry.getValue().offset()));
    context.offset(offsets);
  }

  @SneakyThrows
  public Map<TopicPartition, OffsetAndMetadata> getCommitOffsets() {
    ListConsumerGroupOffsetsResult response = admin.listConsumerGroupOffsets(commitGroupId);
    return response.partitionsToOffsetAndMetadata().get().entrySet().stream()
        .filter(entry -> context.assignment().contains(entry.getKey()))
        .collect(toMap(Entry::getKey, Entry::getValue));
  }

  @Override
  protected void receive(Message message) {
    if (message.getType() == Type.BEGIN_COMMIT) {
      Pair<List<DataFile>, Map<TopicPartition, Long>> commitResult = writer.commit();
      Message filesMessage =
          Message.builder()
              .type(Type.DATA_FILES)
              .dataFiles(commitResult.first())
              .offsets(commitResult.second())
              .assignments(context.assignment())
              .build();
      send(filesMessage);
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
