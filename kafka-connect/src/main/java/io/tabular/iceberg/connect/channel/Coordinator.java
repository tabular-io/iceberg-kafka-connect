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

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.channel.events.CommitReadyPayload;
import io.tabular.iceberg.connect.channel.events.CommitRequestPayload;
import io.tabular.iceberg.connect.channel.events.CommitResponsePayload;
import io.tabular.iceberg.connect.channel.events.Event;
import io.tabular.iceberg.connect.channel.events.EventType;
import io.tabular.iceberg.connect.data.Utilities;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator extends Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String CONTROL_OFFSETS_SNAPSHOT_PREFIX = "kafka.connect.control.offsets.";

  private final Catalog catalog;
  private final Map<TableIdentifier, Table> tables;
  private final IcebergSinkConfig config;
  private final List<Envelope> commitBuffer = new LinkedList<>();
  private final List<CommitReadyPayload> readyBuffer = new LinkedList<>();
  private long startTime;
  private UUID currentCommitId;
  private final int totalPartitionCount;
  private final ExecutorService exec;

  public Coordinator(Catalog catalog, IcebergSinkConfig config) {
    super("coordinator", config);
    this.catalog = catalog;
    this.tables = new HashMap<>();
    this.config = config;
    this.totalPartitionCount = getTotalPartitionCount();
    this.exec = ThreadPools.newWorkerPool("iceberg-committer", config.getCommitThreads());
  }

  @Override
  public void process() {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }

    // send out begin commit
    if (currentCommitId == null
        && System.currentTimeMillis() - startTime >= config.getCommitIntervalMs()) {
      currentCommitId = UUID.randomUUID();
      Event event = new Event(EventType.COMMIT_REQUEST, new CommitRequestPayload(currentCommitId));
      send(event);
      startTime = System.currentTimeMillis();
    }

    super.process();

    if (currentCommitId != null && isCommitTimedOut()) {
      commit();
    }
  }

  @Override
  protected void receive(Envelope envelope) {
    switch (envelope.getEvent().getType()) {
      case COMMIT_RESPONSE:
        commitBuffer.add(envelope);
        if (currentCommitId == null) {
          LOG.warn(
              "Received commit response when no commit in progress, this can happen during recovery");
        }
        break;
      case COMMIT_READY:
        readyBuffer.add((CommitReadyPayload) envelope.getEvent().getPayload());
        if (isCommitComplete()) {
          commit();
        }
        break;
    }
  }

  @SuppressWarnings("deprecation")
  private int getTotalPartitionCount() {
    // use deprecated values() for backwards compatibility
    return admin().describeTopics(config.getTopics()).values().values().stream()
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

  private boolean isCommitTimedOut() {
    if (System.currentTimeMillis() - startTime > config.getCommitTimeoutMs()) {
      LOG.info("Commit timeout reached");
      return true;
    }
    return false;
  }

  private boolean isCommitComplete() {
    int receivedPartitionCount =
        readyBuffer.stream()
            .filter(payload -> payload.getCommitId().equals(currentCommitId))
            .mapToInt(payload -> payload.getAssignments().size())
            .sum();

    if (receivedPartitionCount >= totalPartitionCount) {
      LOG.info("Commit ready, received responses for all {} partitions", receivedPartitionCount);
      return true;
    }

    LOG.info(
        "Commit not ready, received responses for {} of {} partitions, waiting for more",
        receivedPartitionCount,
        totalPartitionCount);

    return false;
  }

  private void commit() {
    try {
      doCommit();
    } catch (Exception e) {
      LOG.warn("Commit failed, will try again next cycle", e);
    }
  }

  private void doCommit() {
    Map<TableIdentifier, List<Envelope>> commitMap =
        commitBuffer.stream()
            .collect(
                groupingBy(
                    envelope ->
                        ((CommitResponsePayload) envelope.getEvent().getPayload())
                            .getTableName()
                            .toIdentifier()));

    Tasks.foreach(commitMap.entrySet())
        .executeWith(exec)
        .stopOnFailure()
        .run(
            entry -> {
              commitToTable(entry.getKey(), entry.getValue());
            });

    commitBuffer.clear();
    readyBuffer.clear();
    currentCommitId = null;
  }

  private void commitToTable(TableIdentifier tableIdentifier, List<Envelope> envelopeList) {
    Table table = getTable(tableIdentifier);
    table.refresh();
    Map<Integer, Long> commitedOffsets = getLastCommittedOffsetsForTable(tableIdentifier);

    List<CommitResponsePayload> payloads =
        envelopeList.stream()
            .filter(
                envelope -> {
                  Long minOffset = commitedOffsets.get(envelope.getPartition());
                  return minOffset == null || envelope.getOffset() >= minOffset;
                })
            .map(envelope -> (CommitResponsePayload) envelope.getEvent().getPayload())
            .collect(toList());

    List<DataFile> dataFiles =
        payloads.stream()
            .filter(payload -> payload.getDataFiles() != null)
            .flatMap(payload -> payload.getDataFiles().stream())
            .filter(dataFile -> dataFile.recordCount() > 0)
            .collect(toList());

    List<DeleteFile> deleteFiles =
        payloads.stream()
            .filter(payload -> payload.getDeleteFiles() != null)
            .flatMap(payload -> payload.getDeleteFiles().stream())
            .filter(deleteFile -> deleteFile.recordCount() > 0)
            .collect(toList());

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.info("Nothing to commit");
    } else {
      String offsetsStr;
      try {
        offsetsStr = MAPPER.writeValueAsString(controlTopicOffsets());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      String offsetsProp = CONTROL_OFFSETS_SNAPSHOT_PREFIX + config.getControlTopic();

      if (deleteFiles.isEmpty()) {
        AppendFiles appendOp = table.newAppend();
        appendOp.set(offsetsProp, offsetsStr);
        dataFiles.forEach(appendOp::appendFile);
        appendOp.commit();
      } else {
        RowDelta deltaOp = table.newRowDelta();
        deltaOp.set(offsetsProp, offsetsStr);
        dataFiles.forEach(deltaOp::addRows);
        deleteFiles.forEach(deltaOp::addDeletes);
        deltaOp.commit();
      }

      LOG.info("Iceberg commit complete");
    }
  }

  @Override
  protected void initConsumerOffsets(Collection<TopicPartition> partitions) {
    super.initConsumerOffsets(partitions);
    Map<Integer, Long> controlTopicOffsets = getLowWatermarkOffsets();
    if (!controlTopicOffsets.isEmpty()) {
      setControlTopicOffsets(controlTopicOffsets);
    }
  }

  private Map<Integer, Long> getLowWatermarkOffsets() {
    Collection<String> tables;
    if (config.getDynamicTablesPrefix() != null) {
      tables = Utilities.getDynamicTableSet(catalog, config.getDynamicTablesPrefix());
    } else {
      tables = config.getTables();
    }

    Map<Integer, Long> offsets = new ConcurrentHashMap<>();
    Tasks.foreach(tables)
        .executeWith(exec)
        .stopOnFailure()
        .run(
            tableName -> {
              TableIdentifier tableIdentifier = TableIdentifier.parse(tableName);
              getLastCommittedOffsetsForTable(tableIdentifier)
                  .forEach((k, v) -> offsets.merge(k, v, Long::min));
            });
    return offsets;
  }

  private Map<Integer, Long> getLastCommittedOffsetsForTable(TableIdentifier tableIdentifier) {
    // TODO: support branches

    String offsetsProp = CONTROL_OFFSETS_SNAPSHOT_PREFIX + config.getControlTopic();
    Table table = getTable(tableIdentifier);
    Snapshot snapshot = table.currentSnapshot();

    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String value = summary.get(offsetsProp);
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
    return ImmutableMap.of();
  }

  private Table getTable(TableIdentifier tableIdentifier) {
    return tables.computeIfAbsent(tableIdentifier, notUsed -> catalog.loadTable(tableIdentifier));
  }
}
