// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.channel.events.Event;
import io.tabular.iceberg.connect.channel.events.EventType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.log4j.Logger;

public class Coordinator extends Channel {

  private static final Logger LOG = Logger.getLogger(Coordinator.class);

  private static final String COMMIT_INTERVAL_MS_PROP = "iceberg.table.commitIntervalMs";
  private static final int COMMIT_INTERVAL_MS_DEFAULT = 60_000;
  private static final String COMMIT_TIMEOUT_MS_PROP = "iceberg.table.commitTimeoutMs";
  private static final int COMMIT_TIMEOUT_MS_DEFAULT = 30_000;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String CHANNEL_OFFSETS_SNAPSHOT_PROP = "kafka.connect.channel.offsets";
  private static final String TOPICS_PROP = "topics";

  private final Table table;
  private final List<Event> commitBuffer = new LinkedList<>();
  private final int commitIntervalMs;
  private final int commitTimeoutMs;
  private final Set<String> topics;
  private long startTime;
  private UUID currentCommitId;

  public Coordinator(Catalog catalog, TableIdentifier tableIdentifier, Map<String, String> props) {
    super("coordinator", props);
    this.table = catalog.loadTable(tableIdentifier);
    this.commitIntervalMs =
        PropertyUtil.propertyAsInt(props, COMMIT_INTERVAL_MS_PROP, COMMIT_INTERVAL_MS_DEFAULT);
    this.commitTimeoutMs =
        PropertyUtil.propertyAsInt(props, COMMIT_TIMEOUT_MS_PROP, COMMIT_TIMEOUT_MS_DEFAULT);
    this.topics =
        Arrays.stream(props.get(TOPICS_PROP).split(","))
            .map(String::trim)
            .collect(Collectors.toCollection(TreeSet::new));
  }

  public void process() {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }

    // send out begin commit
    if (currentCommitId == null && System.currentTimeMillis() - startTime >= commitIntervalMs) {
      currentCommitId = UUID.randomUUID();
      Event event =
          new Event(table.spec().partitionType(), currentCommitId, EventType.BEGIN_COMMIT);
      send(event);
      startTime = System.currentTimeMillis();
    }

    super.process();
  }

  @Override
  protected void receive(Event event) {
    if (event.getType() == EventType.DATA_FILES) {
      commitBuffer.add(event);
      if (currentCommitId == null) {
        LOG.warn("Received data files when no commit in progress, this can happen during recovery");
      } else if (isCommitComplete()) {
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

    // TODO: avoid getting total number of partitions so often, use better algorithm
    int totalPartitions = getTotalPartitionCount();
    int receivedPartitions =
        commitBuffer.stream()
            .filter(event -> event.getCommitId().equals(currentCommitId))
            .mapToInt(event -> event.getAssignments().size())
            .sum();
    if (receivedPartitions < totalPartitions) {
      LOG.info(
          format(
              "Commit not ready, waiting for more results, expected: %d, actual %d",
              totalPartitions, receivedPartitions));
      return false;
    }

    LOG.info(format("Commit ready, received results for all %d partitions", receivedPartitions));
    return true;
  }

  private void commit(List<Event> buffer) {
    List<DataFile> dataFiles =
        buffer.stream()
            .flatMap(event -> event.getDataFiles().stream())
            .filter(dataFile -> dataFile.recordCount() > 0)
            .collect(toList());
    if (dataFiles.isEmpty()) {
      LOG.info("Nothing to commit");
    } else {
      String offsetsStr;
      try {
        offsetsStr = MAPPER.writeValueAsString(channelOffsets());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      table.refresh();
      AppendFiles appendOp = table.newAppend();
      appendOp.set(CHANNEL_OFFSETS_SNAPSHOT_PROP, offsetsStr);
      dataFiles.forEach(appendOp::appendFile);
      appendOp.commit();

      LOG.info("Iceberg commit complete");
    }

    buffer.clear();
    currentCommitId = null;
  }

  public void start() {
    super.start();
    Map<Integer, Long> channelOffsets = getLastCommittedOffsets();
    if (channelOffsets != null) {
      channelSeekToOffsets(channelOffsets);
    }
  }

  private Map<Integer, Long> getLastCommittedOffsets() {
    // TODO: support branches
    // TODO: verify offsets for job
    Snapshot snapshot = table.currentSnapshot();
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String value = summary.get(CHANNEL_OFFSETS_SNAPSHOT_PROP);
      if (value != null) {
        TypeReference<Map<Integer, Long>> typeRef = new TypeReference<>() {};
        try {
          return MAPPER.readValue(value, typeRef);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }
    return null;
  }
}
