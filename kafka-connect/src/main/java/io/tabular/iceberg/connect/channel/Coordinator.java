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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.events.CommitCompletePayload;
import io.tabular.iceberg.connect.events.CommitRequestPayload;
import io.tabular.iceberg.connect.events.CommitTablePayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventType;
import io.tabular.iceberg.connect.events.TableName;
import io.tabular.iceberg.connect.kafka.Envelope;
import io.tabular.iceberg.connect.kafka.Factory;
import io.tabular.iceberg.connect.kafka.KafkaUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Coordinator {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String OFFSETS_SNAPSHOT_PROP_FMT = "kafka.connect.offsets.%s.%s";
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String VTTS_SNAPSHOT_PROP = "kafka.connect.vtts";
  private static final Duration POLL_DURATION = Duration.ofMillis(1000);

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final int totalPartitionCount;
  private final String snapshotOffsetsProp;
  private final ExecutorService exec;
  private final CommitState commitState;
  private final Map<Integer, Long> controlTopicOffsetsByPartition = Maps.newHashMap();
  private final Consumer<String, byte[]> consumer;
  private final Producer<String, byte[]> producer;
  private final UUID producerId;

  Coordinator(
      Catalog catalog,
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      Factory<Consumer<String, byte[]>> consumerFactory,
      Factory<Producer<String, byte[]>> transactionalProducerFactory) {
    this.catalog = catalog;
    this.config = config;
    this.totalPartitionCount =
        members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
    this.snapshotOffsetsProp =
        String.format(OFFSETS_SNAPSHOT_PROP_FMT, config.controlTopic(), config.controlGroupId());
    this.exec = ThreadPools.newWorkerPool("iceberg-committer", config.commitThreads());
    this.commitState = new CommitState(config);

    Map<String, String> consumerProps = Maps.newHashMap(config.controlClusterKafkaProps());
    // TODO: avoid any breaking changes here for now
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, config.controlGroupId() + "-coord");
    this.consumer = consumerFactory.create(consumerProps);
    // TODO: think about the read multiple times bug
    consumer.subscribe(ImmutableList.of(config.controlTopic()));

    // TODO: think hard about this, do we really need this to be transactional
    // That's what they were doing before, valuable because it makes all sends blocking i guess
    Map<String, String> producerProps = Maps.newHashMap(config.controlClusterKafkaProps());
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, config.connectGroupId());
    this.producer = transactionalProducerFactory.create(producerProps); // TODO: better value
    this.producerId = UUID.randomUUID();
  }

  public void process() {
    if (commitState.isCommitIntervalReached()) {
      // send out begin commit
      commitState.startNewCommit();
      Event event =
          new Event(
              config.controlGroupId(),
              EventType.COMMIT_REQUEST,
              new CommitRequestPayload(commitState.currentCommitId()));
      // not transactional on purpose; commit-request events are triggered based time-based
      KafkaUtils.send(producer, producerId, config.controlTopic(), ImmutableList.of(event));
      LOG.info("Started new commit with commit-id={}", commitState.currentCommitId().toString());
    }

    consumeAvailable();

    if (commitState.isCommitTimedOut()) {
      commit(true);
    }
  }

  private void consumeAvailable() {
    KafkaUtils.consumeAvailable(
        consumer,
        Coordinator.POLL_DURATION,
        envelope -> {
          // the consumer stores the offsets that corresponds to the next record to consume,
          // so increment the record offset by one
          controlTopicOffsetsByPartition.put(envelope.partition(), envelope.offset() + 1);

          // TODO: this will be a breaking change unless I'm careful, I think I'm good here
          if (envelope.event().groupId().equals(config.controlGroupId())) {
            LOG.debug("Received event of type: {}", envelope.event().type().name());
            if (receive(envelope)) {
              LOG.info("Handled event of type: {}", envelope.event().type().name());
            }
          }
        });
  }

  protected boolean receive(Envelope envelope) {
    switch (envelope.event().type()) {
      case COMMIT_RESPONSE:
        commitState.addResponse(envelope);
        return true;
      case COMMIT_READY:
        commitState.addReady(envelope);
        if (commitState.isCommitReady(totalPartitionCount)) {
          commit(false);
        }
        return true;
    }
    return false;
  }

  private void commit(boolean partialCommit) {
    try {
      doCommit(partialCommit);
    } catch (Exception e) {
      LOG.warn("Commit failed, will try again next cycle", e);
    } finally {
      commitState.endCurrentCommit();
    }
  }

  private void doCommit(boolean partialCommit) {
    Map<TableIdentifier, List<Envelope>> commitMap = commitState.tableCommitMap();

    String offsetsJson = offsetsJson();
    Long vtts = commitState.vtts(partialCommit);

    Tasks.foreach(commitMap.entrySet())
        .executeWith(exec)
        .stopOnFailure()
        .run(
            entry -> {
              commitToTable(entry.getKey(), entry.getValue(), offsetsJson, vtts);
            });

    commitState.clearResponses();

    // i would prefer a transactional send here to guarantee that we always produce a commitTable
    // event.
    KafkaUtils.send(
        producer,
        producerId,
        config.controlTopic(),
        ImmutableList.of(
            new Event(
                config.controlGroupId(),
                EventType.COMMIT_COMPLETE,
                new CommitCompletePayload(commitState.currentCommitId(), vtts))));

    consumer.commitSync(
        controlTopicOffsetsByPartition.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> new TopicPartition(config.controlTopic(), e.getKey()),
                    e -> new OffsetAndMetadata(e.getValue()))));

    LOG.info(
        "Commit {} complete, committed to {} table(s), vtts {}",
        commitState.currentCommitId(),
        commitMap.size(),
        vtts);
  }

  private String offsetsJson() {
    try {
      return MAPPER.writeValueAsString(controlTopicOffsetsByPartition);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void commitToTable(
      TableIdentifier tableIdentifier, List<Envelope> envelopeList, String offsetsJson, Long vtts) {
    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      LOG.warn("Table not found, skipping commit: {}", tableIdentifier);
      return;
    }

    Optional<String> branch = config.tableConfig(tableIdentifier.toString()).commitBranch();

    Map<Integer, Long> committedOffsets = lastCommittedOffsetsForTable(table, branch.orElse(null));

    List<Envelope> filteredEnvelopeList =
        envelopeList.stream()
            .filter(
                envelope -> {
                  Long minOffset = committedOffsets.get(envelope.partition());
                  return minOffset == null || envelope.offset() >= minOffset;
                })
            .collect(toList());

    List<DataFile> dataFiles =
        Deduplicated.dataFiles(commitState.currentCommitId(), tableIdentifier, filteredEnvelopeList)
            .stream()
            .filter(dataFile -> dataFile.recordCount() > 0)
            .collect(toList());

    List<DeleteFile> deleteFiles =
        Deduplicated.deleteFiles(
                commitState.currentCommitId(), tableIdentifier, filteredEnvelopeList)
            .stream()
            .filter(deleteFile -> deleteFile.recordCount() > 0)
            .collect(toList());

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.info("Nothing to commit to table {}, skipping", tableIdentifier);
    } else {
      if (deleteFiles.isEmpty()) {
        Transaction transaction = table.newTransaction();

        Map<Integer, List<DataFile>> filesBySpec =
            dataFiles.stream()
                .collect(Collectors.groupingBy(DataFile::specId, Collectors.toList()));

        List<List<DataFile>> list = Lists.newArrayList(filesBySpec.values());
        int lastIdx = list.size() - 1;
        for (int i = 0; i <= lastIdx; i++) {
          AppendFiles appendOp = transaction.newAppend();
          branch.ifPresent(appendOp::toBranch);

          list.get(i).forEach(appendOp::appendFile);
          appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
          if (i == lastIdx) {
            appendOp.set(snapshotOffsetsProp, offsetsJson);
            if (vtts != null) {
              appendOp.set(VTTS_SNAPSHOT_PROP, Long.toString(vtts));
            }
          }

          appendOp.commit();
        }

        transaction.commitTransaction();
      } else {
        RowDelta deltaOp = table.newRowDelta();
        branch.ifPresent(deltaOp::toBranch);
        deltaOp.set(snapshotOffsetsProp, offsetsJson);
        deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
        if (vtts != null) {
          deltaOp.set(VTTS_SNAPSHOT_PROP, Long.toString(vtts));
        }
        dataFiles.forEach(deltaOp::addRows);
        deleteFiles.forEach(deltaOp::addDeletes);
        deltaOp.commit();
      }

      Long snapshotId = latestSnapshot(table, branch.orElse(null)).snapshotId();
      Event event =
          new Event(
              config.controlGroupId(),
              EventType.COMMIT_TABLE,
              new CommitTablePayload(
                  commitState.currentCommitId(), TableName.of(tableIdentifier), snapshotId, vtts));
      // best effort send (i.e. not transactional) because we can't commit offsets safely here
      KafkaUtils.send(producer, producerId, config.controlTopic(), ImmutableList.of(event));

      LOG.info(
          "Commit complete to table {}, snapshot {}, commit ID {}, vtts {}",
          tableIdentifier,
          snapshotId,
          commitState.currentCommitId(),
          vtts);
    }
  }

  private Snapshot latestSnapshot(Table table, String branch) {
    if (branch == null) {
      return table.currentSnapshot();
    }
    return table.snapshot(branch);
  }

  private Map<Integer, Long> lastCommittedOffsetsForTable(Table table, String branch) {
    Snapshot snapshot = latestSnapshot(table, branch);
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String value = summary.get(snapshotOffsetsProp);
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

  public void close() {
    LOG.info("Closing Coordinator");
    exec.shutdownNow();
    consumer.close();
    producer.close();
    LOG.info("Closed Coordinator");
  }
}
