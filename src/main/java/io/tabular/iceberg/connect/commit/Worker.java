// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.commit;

import static java.util.stream.Collectors.toMap;

import io.tabular.iceberg.connect.IcebergWriter;
import io.tabular.iceberg.connect.commit.Message.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

@Log4j
public class Worker extends Channel {

  private final IcebergWriter writer;
  private final SinkTaskContext context;

  public Worker(
      Catalog catalog,
      TableIdentifier tableIdentifier,
      Map<String, String> props,
      SinkTaskContext context) {
    super(props);
    this.writer = new IcebergWriter(catalog, tableIdentifier);
    this.context = context;
  }

  public void syncCommitOffsets() {
    Map<TopicPartition, Long> offsets =
        getCommitOffsets().entrySet().stream()
            .collect(toMap(Entry::getKey, entry -> entry.getValue().offset()));
    context.offset(offsets);
  }

  @SneakyThrows
  public Map<TopicPartition, OffsetAndMetadata> getCommitOffsets() {
    ListConsumerGroupOffsetsResult response = admin().listConsumerGroupOffsets(commitGroupId());
    return response.partitionsToOffsetAndMetadata().get().entrySet().stream()
        .filter(entry -> context.assignment().contains(entry.getKey()))
        .collect(toMap(Entry::getKey, Entry::getValue));
  }

  @Override
  protected void receive(Message message) {
    if (message.getType() == Type.BEGIN_COMMIT) {
      IcebergWriter.Result writeResult = writer.complete();
      Message filesMessage =
          Message.builder()
              .type(Type.DATA_FILES)
              .dataFiles(writeResult.getDataFiles())
              .assignments(context.assignment())
              .build();
      send(filesMessage);

      // FIXME: if worker goes down before offsets are set, could cause dupes

      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      writeResult.getOffsets().forEach((k, v) -> offsets.put(k, new OffsetAndMetadata(v)));
      admin().alterConsumerGroupOffsets(commitGroupId(), offsets);
      log.info("Worker offsets committed");
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
