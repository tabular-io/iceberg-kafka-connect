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
import io.tabular.iceberg.connect.data.Utilities;
import io.tabular.iceberg.connect.events.CommitCompletePayload;
import io.tabular.iceberg.connect.events.CommitRequestPayload;
import io.tabular.iceberg.connect.events.CommitTablePayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventType;
import io.tabular.iceberg.connect.events.TableName;
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
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
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

class Coordinator implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String OFFSETS_SNAPSHOT_PROP_FMT = "kafka.connect.offsets.%s.%s";
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String VTTS_SNAPSHOT_PROP = "kafka.connect.vtts";

  private final IcebergSinkConfig config;
  private final Catalog catalog;
  private final int totalPartitionCount;
  private final UUID producerId;
  private final Producer<String, byte[]> producer;
  private final Consumer<String, byte[]> consumer;
  private boolean isFirstPoll = true;
  private final Map<Integer, Long> controlTopicOffsetsByPartition = Maps.newHashMap();
  private final String snapshotOffsetsProp;
  private final ExecutorService exec;
  private final CommitState commitState;

  Coordinator(
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      ConsumerFactory consumerFactory,
      ProducerFactory producerFactory) {
    this(config, members, Utilities.loadCatalog(config), consumerFactory, producerFactory);
  }

  @VisibleForTesting
  Coordinator(
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      Catalog catalog,
      ConsumerFactory consumerFactory,
      ProducerFactory producerFactory) {

    this.config = config;
    this.catalog = catalog;
    this.totalPartitionCount =
        members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();

    Map<String, String> producerProps = Maps.newHashMap(config.kafkaProps());
    // use a random transactional-id to avoid producer generation based fencing
    producerProps.put(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
        String.join(
            "-",
            config.connectGroupId(),
            config.taskId().toString(),
            "coordinator",
            UUID.randomUUID().toString()));
    Pair<UUID, Producer<String, byte[]>> uuidProducerPair = producerFactory.create(producerProps);
    this.producerId = uuidProducerPair.first();
    this.producer = uuidProducerPair.second();
    producer.initTransactions();

    Map<String, String> consumerProps = Maps.newHashMap(config.kafkaProps());
    // use consumer group ID to which we commit low watermark offsets
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, config.controlGroupId() + "-coord");
    this.consumer = consumerFactory.create(consumerProps);
    consumer.subscribe(ImmutableList.of(config.controlTopic()));

    this.snapshotOffsetsProp =
        String.format(OFFSETS_SNAPSHOT_PROP_FMT, config.controlTopic(), config.controlGroupId());
    this.exec = ThreadPools.newWorkerPool("iceberg-committer", config.commitThreads());
    this.commitState = new CommitState(config);
  }

  // initial poll with longer duration so the consumer will initialize...
  private Duration pollDuration() {
    final Duration duration;
    if (isFirstPoll) {
      isFirstPoll = false;
      duration = Duration.ofMillis(1000);
    } else {
      duration = Duration.ZERO;
    }
    return duration;
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
      KafkaUtils.send(producerId, producer, config.controlTopic(), ImmutableList.of(event));
      LOG.info("Started new commit with commit-id={}", commitState.currentCommitId().toString());
    }

    KafkaUtils.consumeAvailable(
        consumer,
        pollDuration(),
        record -> {
          // the consumer stores the offsets that corresponds to the next record to consume,
          // so increment the record offset by one
          controlTopicOffsetsByPartition.put(record.partition(), record.offset() + 1);

          Event event = Event.decode(record.value());

          if (event.groupId().equals(config.controlGroupId())) {
            LOG.debug("Received event of type: {}", event.type().name());
            if (receive(new Envelope(event, record.partition(), record.offset()))) {
              LOG.info("Handled event of type: {}", event.type().name());
            }
          }
        });

    if (commitState.isCommitTimedOut()) {
      commit(true);
    }
  }

  private boolean receive(Envelope envelope) {
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

    // we should only get here if all tables committed successfully...
    commitConsumerOffsets();
    commitState.clearResponses();

    Event event =
        new Event(
            config.controlGroupId(),
            EventType.COMMIT_COMPLETE,
            new CommitCompletePayload(commitState.currentCommitId(), vtts));
    KafkaUtils.send(producerId, producer, config.controlTopic(), ImmutableList.of(event));

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
      KafkaUtils.send(producerId, producer, config.controlTopic(), ImmutableList.of(event));

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

  private void commitConsumerOffsets() {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = Maps.newHashMap();
    controlTopicOffsetsByPartition.forEach(
        (k, v) ->
            offsetsToCommit.put(
                new TopicPartition(config.controlTopic(), k), new OffsetAndMetadata(v)));
    consumer.commitSync(offsetsToCommit);
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing Coordinator");
    exec.shutdownNow();
    producer.close();
    consumer.close();
    Utilities.close(catalog);
  }
}
