// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.commit;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import io.tabular.iceberg.connect.Utilities;
import io.tabular.iceberg.connect.commit.Message.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.log4j.Log4j;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Log4j
public class Coordinator extends Channel {

  private static final String COMMIT_INTERVAL_MS_PROP = "iceberg.table.commitIntervalMs";
  private static final int COMMIT_INTERVAL_MS_DEFAULT = 60_000;
  private static final String COMMIT_GROUP_ID_PROP = "iceberg.commit.group.id";

  private final Table table;
  private final List<Message> commitBuffer;
  private final int commitIntervalMs;
  private final Set<String> topics;
  private final String commitGroupId;
  private final Admin admin;
  private long startTime;
  private boolean commitInProgress;

  public Coordinator(Catalog catalog, TableIdentifier tableIdentifier, Map<String, String> props) {
    super(props);
    this.table = catalog.loadTable(tableIdentifier);
    this.commitBuffer = new ArrayList<>();
    this.commitIntervalMs =
        PropertyUtil.propertyAsInt(props, COMMIT_INTERVAL_MS_PROP, COMMIT_INTERVAL_MS_DEFAULT);
    this.topics = Utilities.getTopics(props);
    this.commitGroupId = props.get(COMMIT_GROUP_ID_PROP);

    Map<String, Object> adminCliProps = new HashMap<>(kafkaProps);
    this.admin = Admin.create(adminCliProps);
  }

  public void process() {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }

    // send out begin commit
    if (!commitInProgress && System.currentTimeMillis() - startTime >= commitIntervalMs) {
      commitInProgress = true;
      send(Message.builder().type(Type.BEGIN_COMMIT).build());
      startTime = System.currentTimeMillis();
    }

    super.process();
  }

  @Override
  protected void receive(Message message) {
    if (message.getType() == Type.DATA_FILES) {
      if (!commitInProgress) {
        throw new IllegalStateException("Received data files when no commit in progress");
      }
      commitBuffer.add(message);
      commitIfComplete();
    }
  }

  private int getTotalPartitionCount() {
    return admin.describeTopics(topics).topicNameValues().values().stream()
        .mapToInt(
            value -> {
              try {
                return value.get().partitions().size();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            })
        .sum();
  }

  private void commitIfComplete() {
    // TODO: avoid getting total number of partitions so often
    int totalPartitions = getTotalPartitionCount();
    int receivedPartitions =
        commitBuffer.stream().mapToInt(message -> message.getAssignments().size()).sum();
    if (receivedPartitions < totalPartitions) {
      log.info(
          format(
              "Commit not ready, waiting for more results, expected: %d, actual %d",
              totalPartitions, receivedPartitions));
      return;
    }

    log.info(format("Commit ready, received results for all %d partitions", receivedPartitions));
    List<DataFile> dataFiles =
        commitBuffer.stream()
            .flatMap(message -> message.getDataFiles().stream())
            .filter(dataFile -> dataFile.recordCount() > 0)
            .collect(toList());
    if (dataFiles.isEmpty()) {
      log.info("Nothing to commit");
    } else {
      table.refresh();
      AppendFiles appendOp = table.newAppend();
      dataFiles.forEach(appendOp::appendFile);
      appendOp.commit();
      log.info("Iceberg commit complete");

      // the consumer stores the offset that corresponds to the next record to consume,
      // so increment the offset by one
      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      commitBuffer.stream()
          .flatMap(message -> message.getOffsets().entrySet().stream())
          .forEach(
              entry -> offsets.put(entry.getKey(), new OffsetAndMetadata(entry.getValue() + 1)));
      admin.alterConsumerGroupOffsets(commitGroupId, offsets);
      log.info("Kafka offset commit complete");

      // TODO: trigger offset commit on workers? won't be saved until flush()
    }

    commitBuffer.clear();
    commitInProgress = false;
  }

  @Override
  public void stop() {
    super.stop();
    admin.close();
  }
}
