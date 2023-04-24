// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.commit;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.Utilities;
import io.tabular.iceberg.connect.commit.Message.Type;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.PropertyUtil;

@Log4j
public class Coordinator extends Channel {

  private static final String COMMIT_INTERVAL_MS_PROP = "iceberg.table.commitIntervalMs";
  private static final int COMMIT_INTERVAL_MS_DEFAULT = 60_000;
  private static final String COMMIT_TIMEOUT_MS_PROP = "iceberg.table.commitTimeoutMs";
  private static final int COMMIT_TIMEOUT_MS_DEFAULT = 30_000;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String CHANNEL_OFFSETS_SNAPSHOT_PROP = "kafka.connect.channel.offsets";

  private final Table table;
  private final List<Message> commitBuffer;
  private final int commitIntervalMs;
  private final int commitTimeoutMs;
  private final Set<String> topics;
  private long startTime;
  private boolean commitInProgress;

  public Coordinator(Catalog catalog, TableIdentifier tableIdentifier, Map<String, String> props) {
    super("coordinator", props);
    this.table = catalog.loadTable(tableIdentifier);
    this.commitBuffer = new LinkedList<>();
    this.commitIntervalMs =
        PropertyUtil.propertyAsInt(props, COMMIT_INTERVAL_MS_PROP, COMMIT_INTERVAL_MS_DEFAULT);
    this.commitTimeoutMs =
        PropertyUtil.propertyAsInt(props, COMMIT_TIMEOUT_MS_PROP, COMMIT_TIMEOUT_MS_DEFAULT);
    this.topics = Utilities.getTopics(props);
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
      if (isCommitComplete()) {
        commit(commitBuffer);
      }
    }
  }

  private int getTotalPartitionCount() {
    return admin().describeTopics(topics).topicNameValues().values().stream()
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

  private boolean isCommitComplete() {
    if (System.currentTimeMillis() - startTime > commitTimeoutMs) {
      // commit whatever we have received
      return true;
    }

    // TODO: avoid getting total number of partitions so often
    int totalPartitions = getTotalPartitionCount();
    int receivedPartitions =
        commitBuffer.stream().mapToInt(message -> message.getAssignments().size()).sum();
    if (receivedPartitions < totalPartitions) {
      log.info(
          format(
              "Commit not ready, waiting for more results, expected: %d, actual %d",
              totalPartitions, receivedPartitions));
      return false;
    }

    log.info(format("Commit ready, received results for all %d partitions", receivedPartitions));
    return true;
  }

  @SneakyThrows
  private void commit(List<Message> buffer) {
    List<DataFile> dataFiles =
        buffer.stream()
            .flatMap(message -> message.getDataFiles().stream())
            .filter(dataFile -> dataFile.recordCount() > 0)
            .collect(toList());
    if (dataFiles.isEmpty()) {
      log.info("Nothing to commit");
    } else {
      String offsetsStr = MAPPER.writeValueAsString(channelOffsets());
      table.refresh();
      AppendFiles appendOp = table.newAppend();
      appendOp.set(CHANNEL_OFFSETS_SNAPSHOT_PROP, offsetsStr);
      dataFiles.forEach(appendOp::appendFile);
      appendOp.commit();

      log.info("Iceberg commit complete");
    }

    buffer.clear();
    commitInProgress = false;
  }

  public void start() {
    super.start();
    Map<Integer, Long> channelOffsets = getLastCommittedOffsets();
    if (channelOffsets != null) {
      channelSeekToOffsets(channelOffsets);
      List<Message> buffer = new LinkedList<>();
      consumeAvailable(
          message -> {
            if (message.getType() == Type.DATA_FILES) {
              buffer.add(message);
            }
          },
          1000L);
      commit(buffer);
    }
  }

  @SneakyThrows
  private Map<Integer, Long> getLastCommittedOffsets() {
    // TODO: support branches
    // TODO: verify offsets for job
    Snapshot snapshot = table.currentSnapshot();
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String value = summary.get(CHANNEL_OFFSETS_SNAPSHOT_PROP);
      if (value != null) {
        TypeReference<Map<Integer, Long>> typeRef = new TypeReference<>() {};
        return MAPPER.readValue(value, typeRef);
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }
    return null;
  }
}
