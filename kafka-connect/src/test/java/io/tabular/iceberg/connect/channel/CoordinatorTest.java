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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.TableSinkConfig;
import io.tabular.iceberg.connect.events.CommitCompletePayload;
import io.tabular.iceberg.connect.events.CommitReadyPayload;
import io.tabular.iceberg.connect.events.CommitRequestPayload;
import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.CommitTablePayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventTestUtil;
import io.tabular.iceberg.connect.events.EventType;
import io.tabular.iceberg.connect.events.TableName;
import io.tabular.iceberg.connect.events.TopicPartitionOffset;
import io.tabular.iceberg.connect.kafka.Factory;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CoordinatorTest {

  private static final String SRC_TOPIC_NAME = "src-topic";
  private static final String CTL_TOPIC_NAME = "ctl-topic";
  private static final String CONTROL_CONSUMER_GROUP_ID = "cg-connector";
  private InMemoryCatalog catalog;
  private Table table;
  private IcebergSinkConfig config;
  private MockProducer<String, byte[]> producer;
  private MockConsumer<String, byte[]> consumer;
  private final Factory<Consumer<String, byte[]>> consumerFactory = consumerGroupId -> consumer;
  private final Factory<Producer<String, byte[]>> producerFactory = transactionalId -> producer;

  private InMemoryCatalog initInMemoryCatalog() {
    InMemoryCatalog inMemoryCatalog = new InMemoryCatalog();
    inMemoryCatalog.initialize(null, ImmutableMap.of());
    return inMemoryCatalog;
  }

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, TABLE_NAME);
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          required(3, "date", Types.StringType.get()));

  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String OFFSETS_SNAPSHOT_PROP =
      String.format("kafka.connect.offsets.%s.%s", CTL_TOPIC_NAME, CONTROL_CONSUMER_GROUP_ID);
  private static final String VTTS_SNAPSHOT_PROP = "kafka.connect.vtts";

  @BeforeEach
  public void before() {
    catalog = initInMemoryCatalog();
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA);

    config = mock(IcebergSinkConfig.class);
    when(config.controlTopic()).thenReturn(CTL_TOPIC_NAME);
    when(config.commitThreads()).thenReturn(1);
    when(config.controlGroupId()).thenReturn(CONTROL_CONSUMER_GROUP_ID);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));

    consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
    producer.initTransactions();
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  protected void initConsumer() {
    TopicPartition tp = new TopicPartition(CTL_TOPIC_NAME, 0);
    consumer.rebalance(ImmutableList.of(tp));
    consumer.updateEndOffsets(ImmutableMap.of(tp, 0L));
  }

  @Test
  public void testCommitAppend() {
    Assertions.assertEquals(0, ImmutableList.copyOf(table.snapshots().iterator()).size());

    long ts = System.currentTimeMillis();
    UUID commitId =
        coordinatorTest(ImmutableList.of(EventTestUtil.createDataFile()), ImmutableList.of(), ts);
    table.refresh();

    assertThat(producer.history()).hasSize(3);
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.APPEND, snapshot.operation());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(0, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());

    Map<String, String> summary = snapshot.summary();
    Assertions.assertEquals(commitId.toString(), summary.get(COMMIT_ID_SNAPSHOT_PROP));
    Assertions.assertEquals("{\"0\":3}", summary.get(OFFSETS_SNAPSHOT_PROP));
    Assertions.assertEquals(Long.toString(ts), summary.get(VTTS_SNAPSHOT_PROP));
  }

  @Test
  public void testCommitDelta() {
    long ts = System.currentTimeMillis();
    UUID commitId =
        coordinatorTest(
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(EventTestUtil.createDeleteFile()),
            ts);

    assertThat(producer.history()).hasSize(3);
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.OVERWRITE, snapshot.operation());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());

    Map<String, String> summary = snapshot.summary();
    Assertions.assertEquals(commitId.toString(), summary.get(COMMIT_ID_SNAPSHOT_PROP));
    Assertions.assertEquals("{\"0\":3}", summary.get(OFFSETS_SNAPSHOT_PROP));
    Assertions.assertEquals(Long.toString(ts), summary.get(VTTS_SNAPSHOT_PROP));
  }

  @Test
  public void testCommitNoFiles() {
    long ts = System.currentTimeMillis();
    UUID commitId = coordinatorTest(ImmutableList.of(), ImmutableList.of(), ts);

    assertThat(producer.history()).hasSize(2);
    assertCommitComplete(1, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(0, snapshots.size());
  }

  @Test
  public void testCommitError() {
    // this spec isn't registered with the table
    PartitionSpec badPartitionSpec =
        PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();
    DataFile badDataFile =
        DataFiles.builder(badPartitionSpec)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(5)
            .build();

    coordinatorTest(ImmutableList.of(badDataFile), ImmutableList.of(), 0L);

    // no commit messages sent
    assertThat(producer.history()).hasSize(1);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(0, snapshots.size());
  }

  @Test
  public void testShouldDeduplicateDataFilesBeforeAppending() {
    long ts = System.currentTimeMillis();
    DataFile dataFile = EventTestUtil.createDataFile();

    UUID commitId =
        coordinatorTest(
            currentCommitId -> {
              Event commitResponse =
                  new Event(
                      config.controlGroupId(),
                      EventType.COMMIT_RESPONSE,
                      new CommitResponsePayload(
                          StructType.of(),
                          currentCommitId,
                          new TableName(ImmutableList.of("db"), "tbl"),
                          ImmutableList.of(dataFile, dataFile), // duplicated data files
                          ImmutableList.of()));

              return ImmutableList.of(
                  commitResponse,
                  commitResponse, // duplicate commit response
                  new Event(
                      config.controlGroupId(),
                      EventType.COMMIT_READY,
                      new CommitReadyPayload(
                          currentCommitId,
                          ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts)))));
            });

    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.APPEND, snapshot.operation());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(0, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());
  }

  @Test
  public void testShouldDeduplicateDeleteFilesBeforeAppending() {
    long ts = System.currentTimeMillis();
    DeleteFile deleteFile = EventTestUtil.createDeleteFile();

    UUID commitId =
        coordinatorTest(
            currentCommitId -> {
              Event duplicateCommitResponse =
                  new Event(
                      config.controlGroupId(),
                      EventType.COMMIT_RESPONSE,
                      new CommitResponsePayload(
                          StructType.of(),
                          currentCommitId,
                          new TableName(ImmutableList.of("db"), "tbl"),
                          ImmutableList.of(),
                          ImmutableList.of(deleteFile, deleteFile))); // duplicate delete files

              return ImmutableList.of(
                  duplicateCommitResponse,
                  duplicateCommitResponse, // duplicate commit response
                  new Event(
                      config.controlGroupId(),
                      EventType.COMMIT_READY,
                      new CommitReadyPayload(
                          currentCommitId,
                          ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts)))));
            });

    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.OVERWRITE, snapshot.operation());
    Assertions.assertEquals(0, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());
  }

  private void validateAddedFiles(
      Snapshot snapshot, Set<String> expectedDataFilePaths, PartitionSpec expectedSpec) {
    final List<DataFile> addedDataFiles = ImmutableList.copyOf(snapshot.addedDataFiles(table.io()));
    final List<DeleteFile> addedDeleteFiles =
        ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io()));

    Assertions.assertEquals(
        expectedDataFilePaths,
        addedDataFiles.stream().map(ContentFile::path).collect(Collectors.toSet()));

    Assertions.assertEquals(
        ImmutableSet.of(expectedSpec.specId()),
        Stream.concat(addedDataFiles.stream(), addedDeleteFiles.stream())
            .map(ContentFile::specId)
            .collect(Collectors.toSet()));
  }

  /**
   *
   *
   * <ul>
   *   <li>Sets up an empty table with 2 partition specs
   *   <li>Starts a coordinator with 2 worker assignment each handling a different topic-partition
   *   <li>Sends a commit request to workers
   *   <li>Each worker writes datafiles with a different partition spec
   *   <li>The coordinator receives datafiles from both workers eventually and commits them to the
   *       table
   * </ul>
   */
  @Test
  public void testCommitMultiPartitionSpecAppendDataFiles() {
    final PartitionSpec spec1 = table.spec();
    assert spec1.isUnpartitioned();

    // evolve spec to partition by date
    final PartitionSpec partitionByDate = PartitionSpec.builderFor(SCHEMA).identity("date").build();
    table.updateSpec().addField(partitionByDate.fields().get(0).name()).commit();
    final PartitionSpec spec2 = table.spec();
    assert spec2.isPartitioned();

    // pretend we have two workers each handling 1 topic partition
    final List<MemberDescription> members = Lists.newArrayList();
    for (int i : ImmutableList.of(0, 1)) {
      members.add(
          new MemberDescription(
              "memberId" + i,
              "clientId" + i,
              "host" + i,
              new MemberAssignment(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, i)))));
    }

    final Coordinator coordinator =
        new Coordinator(catalog, config, members, consumerFactory, producerFactory);
    initConsumer();

    // start a new commit immediately and wait for all workers to respond infinitely
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    coordinator.process();

    // retrieve commitId from commit request produced by coordinator
    final byte[] bytes = producer.history().get(0).value();
    final Event commitRequest = Event.decode(bytes);
    assert commitRequest.type().equals(EventType.COMMIT_REQUEST);
    final UUID commitId = ((CommitRequestPayload) commitRequest.payload()).commitId();

    // each worker sends its responses for the commit request
    Map<Integer, PartitionSpec> workerIdToSpecMap =
        ImmutableMap.of(
            1, spec1, // worker 1 produces datafiles with the old partition spec
            2, spec2 // worker 2 produces datafiles with the new partition spec
            );

    int currentControlTopicOffset = 1;
    for (Map.Entry<Integer, PartitionSpec> entry : workerIdToSpecMap.entrySet()) {
      Integer workerId = entry.getKey();
      PartitionSpec spec = entry.getValue();

      final DataFile dataFile =
          DataFiles.builder(spec)
              .withPath(String.format("file%d.parquet", workerId))
              .withFileSizeInBytes(100)
              .withRecordCount(5)
              .build();

      consumer.addRecord(
          new ConsumerRecord<>(
              CTL_TOPIC_NAME,
              0,
              currentControlTopicOffset,
              "key",
              Event.encode(
                  new Event(
                      config.controlGroupId(),
                      EventType.COMMIT_RESPONSE,
                      new CommitResponsePayload(
                          spec.partitionType(),
                          commitId,
                          TableName.of(TABLE_IDENTIFIER),
                          ImmutableList.of(dataFile),
                          ImmutableList.of())))));
      currentControlTopicOffset += 1;

      consumer.addRecord(
          new ConsumerRecord<>(
              CTL_TOPIC_NAME,
              0,
              currentControlTopicOffset,
              "key",
              Event.encode(
                  new Event(
                      config.controlGroupId(),
                      EventType.COMMIT_READY,
                      new CommitReadyPayload(
                          commitId,
                          ImmutableList.of(
                              new TopicPartitionOffset(SRC_TOPIC_NAME, 0, 100L, 100L)))))));
      currentControlTopicOffset += 1;
    }

    // all workers have responded so coordinator can process responses now
    coordinator.process();

    // assertions
    table.refresh();
    final List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(2, snapshots.size(), "Expected 2 snapshots, one for each spec.");

    final Snapshot firstSnapshot = snapshots.get(0);
    final Snapshot secondSnapshot = snapshots.get(1);

    validateAddedFiles(firstSnapshot, ImmutableSet.of("file1.parquet"), spec1);
    validateAddedFiles(secondSnapshot, ImmutableSet.of("file2.parquet"), spec2);

    Assertions.assertEquals(
        commitId.toString(),
        firstSnapshot.summary().get(COMMIT_ID_SNAPSHOT_PROP),
        "All snapshots should be tagged with a commit-id");
    Assertions.assertNull(
        firstSnapshot.summary().getOrDefault(OFFSETS_SNAPSHOT_PROP, null),
        "Earlier snapshots should not include control-topic-offsets in their summary");
    Assertions.assertNull(
        firstSnapshot.summary().getOrDefault(VTTS_SNAPSHOT_PROP, null),
        "Earlier snapshots should not include vtts in their summary");

    Assertions.assertEquals(
        commitId.toString(),
        secondSnapshot.summary().get(COMMIT_ID_SNAPSHOT_PROP),
        "All snapshots should be tagged with a commit-id");
    Assertions.assertEquals(
        "{\"0\":5}",
        secondSnapshot.summary().get(OFFSETS_SNAPSHOT_PROP),
        "Only the most recent snapshot should include control-topic-offsets in it's summary");
    Assertions.assertEquals(
        "100",
        secondSnapshot.summary().get(VTTS_SNAPSHOT_PROP),
        "Only the most recent snapshot should include vtts in it's summary");
  }

  private void assertCommitTable(int idx, UUID commitId, long ts) {
    byte[] bytes = producer.history().get(idx).value();
    Event commitTable = Event.decode(bytes);
    assertThat(commitTable.type()).isEqualTo(EventType.COMMIT_TABLE);
    CommitTablePayload commitTablePayload = (CommitTablePayload) commitTable.payload();
    assertThat(commitTablePayload.commitId()).isEqualTo(commitId);
    assertThat(commitTablePayload.tableName().toIdentifier().toString())
        .isEqualTo(TABLE_IDENTIFIER.toString());
    assertThat(commitTablePayload.vtts()).isEqualTo(ts);
  }

  private void assertCommitComplete(int idx, UUID commitId, long ts) {
    byte[] bytes = producer.history().get(idx).value();
    Event commitComplete = Event.decode(bytes);
    assertThat(commitComplete.type()).isEqualTo(EventType.COMMIT_COMPLETE);
    CommitCompletePayload commitCompletePayload = (CommitCompletePayload) commitComplete.payload();
    assertThat(commitCompletePayload.commitId()).isEqualTo(commitId);
    assertThat(commitCompletePayload.vtts()).isEqualTo(ts);
  }

  private UUID coordinatorTest(List<DataFile> dataFiles, List<DeleteFile> deleteFiles, long ts) {
    return coordinatorTest(
        currentCommitId -> {
          Event commitResponse =
              new Event(
                  config.controlGroupId(),
                  EventType.COMMIT_RESPONSE,
                  new CommitResponsePayload(
                      StructType.of(),
                      currentCommitId,
                      new TableName(ImmutableList.of("db"), "tbl"),
                      dataFiles,
                      deleteFiles));

          Event commitReady =
              new Event(
                  config.controlGroupId(),
                  EventType.COMMIT_READY,
                  new CommitReadyPayload(
                      currentCommitId,
                      ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts))));

          return ImmutableList.of(commitResponse, commitReady);
        });
  }

  private UUID coordinatorTest(Function<UUID, List<Event>> eventsFn) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    final Coordinator coordinator =
        new Coordinator(catalog, config, ImmutableList.of(), consumerFactory, producerFactory);

    // init consumer after subscribe()
    initConsumer();

    coordinator.process();

    assertThat(producer.transactionCommitted()).isTrue();
    assertThat(producer.history()).hasSize(1);

    byte[] bytes = producer.history().get(0).value();
    Event commitRequest = Event.decode(bytes);
    assertThat(commitRequest.type()).isEqualTo(EventType.COMMIT_REQUEST);

    UUID commitId = ((CommitRequestPayload) commitRequest.payload()).commitId();

    int currentOffset = 1;
    for (Event event : eventsFn.apply(commitId)) {
      bytes = Event.encode(event);
      consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, currentOffset, "key", bytes));
      currentOffset += 1;
    }

    when(config.commitIntervalMs()).thenReturn(0);

    coordinator.process();

    return commitId;
  }
}
