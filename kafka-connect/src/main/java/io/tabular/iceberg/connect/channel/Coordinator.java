// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.channel.events.CommitRequestPayload;
import io.tabular.iceberg.connect.channel.events.CommitResponsePayload;
import io.tabular.iceberg.connect.channel.events.Event;
import io.tabular.iceberg.connect.channel.events.EventType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator extends Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String CONTROL_OFFSETS_SNAPSHOT_PROP = "kafka.connect.control.offsets";

  private final Table table;
  private final List<Event> commitBuffer = new LinkedList<>();
  private final int commitIntervalMs;
  private final int commitTimeoutMs;
  private final Set<String> topics;
  private long startTime;
  private UUID currentCommitId;
  private final int totalPartitionCount;

  public Coordinator(Catalog catalog, TableIdentifier tableIdentifier, IcebergSinkConfig config) {
    super("coordinator", config);
    this.table = catalog.loadTable(tableIdentifier);
    this.commitIntervalMs = config.getCommitIntervalMs();
    this.commitTimeoutMs = config.getCommitTimeoutMs();
    this.topics = config.getTopics();
    this.totalPartitionCount = getTotalPartitionCount();
  }

  @Override
  public void process() {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }

    // send out begin commit
    if (currentCommitId == null && System.currentTimeMillis() - startTime >= commitIntervalMs) {
      currentCommitId = UUID.randomUUID();
      Event event = new Event(EventType.COMMIT_REQUEST, new CommitRequestPayload(currentCommitId));
      send(event);
      startTime = System.currentTimeMillis();
    }

    super.process();

    // TODO: reduce frequency of commit check
    if (currentCommitId != null && isCommitComplete()) {
      commit(commitBuffer);
    }
  }

  @Override
  protected void receive(Event event) {
    if (event.getType() == EventType.COMMIT_RESPONSE) {
      commitBuffer.add(event);
      if (currentCommitId == null) {
        LOG.warn(
            "Received commit response when no commit in progress, this can happen during recovery");
      }
    }
  }

  private int getTotalPartitionCount() {
    return admin().describeTopics(topics).topicNameValues().values().stream()
        .mapToInt(
            value -> {
              try {
                return value.get().partitions().size();
              } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
              }
            })
        .sum();
  }

  private boolean isCommitComplete() {
    int receivedPartitionCount =
        commitBuffer.stream()
            .map(event -> (CommitResponsePayload) event.getPayload())
            .filter(payload -> payload.getCommitId().equals(currentCommitId))
            .mapToInt(payload -> payload.getAssignments().size())
            .sum();

    if (receivedPartitionCount >= totalPartitionCount) {
      LOG.info("Commit ready, received responses for all {} partitions", receivedPartitionCount);
      return true;
    }

    if (System.currentTimeMillis() - startTime > commitTimeoutMs) {
      LOG.info(
          "Commit timeout reached, committing data for {} of {} partitions",
          receivedPartitionCount,
          totalPartitionCount);
      return true;
    }

    LOG.info(
        "Commit not ready, received responses for {} of {} partitions, waiting for more",
        receivedPartitionCount,
        totalPartitionCount);

    return false;
  }

  private void commit(List<Event> buffer) {
    List<DataFile> dataFiles =
        buffer.stream()
            .flatMap(event -> ((CommitResponsePayload) event.getPayload()).getDataFiles().stream())
            .filter(dataFile -> dataFile.recordCount() > 0)
            .collect(toList());

    table.refresh();

    if (dataFiles.isEmpty()) {
      LOG.info("Nothing to commit");
    } else {
      String offsetsStr;
      try {
        offsetsStr = MAPPER.writeValueAsString(controlTopicOffsets());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      AppendFiles appendOp = table.newAppend();
      appendOp.set(CONTROL_OFFSETS_SNAPSHOT_PROP, offsetsStr);
      dataFiles.forEach(appendOp::appendFile);
      appendOp.commit();

      LOG.info("Iceberg commit complete");
    }

    buffer.clear();
    currentCommitId = null;
  }

  @Override
  protected void initConsumerOffsets(Collection<TopicPartition> partitions) {
    super.initConsumerOffsets(partitions);
    Map<Integer, Long> controlTopicOffsets = getLastCommittedOffsets();
    if (controlTopicOffsets != null) {
      setControlTopicOffsets(controlTopicOffsets);
    }
  }

  private Map<Integer, Long> getLastCommittedOffsets() {
    // TODO: support branches
    // TODO: verify offsets for job
    Snapshot snapshot = table.currentSnapshot();
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String value = summary.get(CONTROL_OFFSETS_SNAPSHOT_PROP);
      if (value != null) {
        TypeReference<Map<Integer, Long>> typeRef = new TypeReference<Map<Integer, Long>>() {};
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
