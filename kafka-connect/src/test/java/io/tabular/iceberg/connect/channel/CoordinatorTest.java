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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CoordinatorTest extends ChannelTestBase {

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

    assertThat(memoryAppender.getWarnOrHigher())
        .as("Expected no warn or higher log messages")
        .hasSize(0);
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

    assertThat(memoryAppender.getWarnOrHigher())
        .as("Expected no warn or higher log messages")
        .hasSize(0);
  }

  @Test
  public void testCommitNoFiles() {
    long ts = System.currentTimeMillis();
    UUID commitId = coordinatorTest(ImmutableList.of(), ImmutableList.of(), ts);

    assertThat(producer.history()).hasSize(2);
    assertCommitComplete(1, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(0, snapshots.size());

    assertThat(memoryAppender.getWarnOrHigher())
        .as("Expected no warn or higher log messages")
        .hasSize(0);
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

    List<String> warnOrHigherLogMessages = memoryAppender.getWarnOrHigher();
    assertThat(warnOrHigherLogMessages).as("Expected 1 log message").hasSize(1);
    assertThat(warnOrHigherLogMessages.get(0))
        .as("Expected commit failed message warning")
        .isEqualTo("Commit failed, will try again next cycle");
  }

  @Test
  public void testShouldDeduplicateDataFilesAcrossBatchBeforeAppending() {
    long ts = System.currentTimeMillis();
    DataFile dataFile = EventTestUtil.createDataFile();

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
                          ImmutableList.of(dataFile),
                          ImmutableList.of()));

              return ImmutableList.of(
                  duplicateCommitResponse,
                  duplicateCommitResponse,
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

    List<String> warnOrHigherLogMessages = memoryAppender.getWarnOrHigher();
    assertThat(warnOrHigherLogMessages).as("Expected 1 log message").hasSize(1);
    assertThat(warnOrHigherLogMessages.get(0))
        .as("Expected duplicates detected message warning")
        .matches(
            "Detected 2 data files with the same path=.*\\.parquet across payloads during commitId=.* for table=db\\.tbl");
  }

  @Test
  public void testShouldDeduplicateDeleteFilesAcrossBatchBeforeAppending() {
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
                          ImmutableList.of(deleteFile)));

              return ImmutableList.of(
                  duplicateCommitResponse,
                  duplicateCommitResponse,
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

    List<String> warnOrHigherLogMessages = memoryAppender.getWarnOrHigher();
    assertThat(warnOrHigherLogMessages).as("Expected 1 log message").hasSize(1);
    assertThat(warnOrHigherLogMessages.get(0))
        .as("Expected duplicates detected message warning")
        .matches(
            "Detected 2 delete files with the same path=.*\\.parquet across payloads during commitId=.* for table=db\\.tbl");
  }

  @Test
  public void testShouldDeduplicateDataFilesInPayloadBeforeAppending() {
    long ts = System.currentTimeMillis();
    DataFile duplicateDataFile = EventTestUtil.createDataFile();

    UUID commitId =
        coordinatorTest(
            currentCommitId ->
                ImmutableList.of(
                    new Event(
                        config.controlGroupId(),
                        EventType.COMMIT_RESPONSE,
                        new CommitResponsePayload(
                            StructType.of(),
                            currentCommitId,
                            new TableName(ImmutableList.of("db"), "tbl"),
                            ImmutableList.of(duplicateDataFile, duplicateDataFile),
                            ImmutableList.of())),
                    new Event(
                        config.controlGroupId(),
                        EventType.COMMIT_READY,
                        new CommitReadyPayload(
                            currentCommitId,
                            ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts))))));

    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.APPEND, snapshot.operation());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(0, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());

    List<String> warnOrHigherLogMessages = memoryAppender.getWarnOrHigher();
    assertThat(warnOrHigherLogMessages).as("Expected 1 log message").hasSize(1);
    assertThat(warnOrHigherLogMessages.get(0))
        .as("Expected duplicates detected message warning")
        .matches(
            "Detected 2 data files with the same path=.*\\.parquet in payload with commitId=.* for table=db\\.tbl at partition=0 and offset=1");
  }

  @Test
  public void testShouldDeduplicateDeleteFilesInPayloadBeforeAppending() {
    long ts = System.currentTimeMillis();
    DeleteFile duplicateDeleteFile = EventTestUtil.createDeleteFile();

    UUID commitId =
        coordinatorTest(
            currentCommitId ->
                ImmutableList.of(
                    new Event(
                        config.controlGroupId(),
                        EventType.COMMIT_RESPONSE,
                        new CommitResponsePayload(
                            StructType.of(),
                            currentCommitId,
                            new TableName(ImmutableList.of("db"), "tbl"),
                            ImmutableList.of(),
                            ImmutableList.of(duplicateDeleteFile, duplicateDeleteFile))),
                    new Event(
                        config.controlGroupId(),
                        EventType.COMMIT_READY,
                        new CommitReadyPayload(
                            currentCommitId,
                            ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts))))));

    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    Assertions.assertEquals(1, snapshots.size());

    Snapshot snapshot = snapshots.get(0);
    Assertions.assertEquals(DataOperations.OVERWRITE, snapshot.operation());
    Assertions.assertEquals(0, ImmutableList.copyOf(snapshot.addedDataFiles(table.io())).size());
    Assertions.assertEquals(1, ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io())).size());

    List<String> warnOrHigherLogMessages = memoryAppender.getWarnOrHigher();
    assertThat(warnOrHigherLogMessages).as("Expected 1 log message").hasSize(1);
    assertThat(warnOrHigherLogMessages.get(0))
        .as("Expected duplicates detected message warning")
        .matches(
            "Detected 2 delete files with the same path=.*\\.parquet in payload with commitId=.* for table=db\\.tbl at partition=0 and offset=1");
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
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    Coordinator coordinator = new Coordinator(catalog, config, ImmutableList.of(), clientFactory);
    coordinator.start();

    // init consumer after subscribe()
    initConsumer();

    coordinator.process();

    assertThat(producer.transactionCommitted()).isTrue();
    assertThat(producer.history()).hasSize(1);

    byte[] bytes = producer.history().get(0).value();
    Event commitRequest = Event.decode(bytes);
    assertThat(commitRequest.type()).isEqualTo(EventType.COMMIT_REQUEST);

    UUID commitId = ((CommitRequestPayload) commitRequest.payload()).commitId();

    Event commitResponse =
        new Event(
            config.controlGroupId(),
            EventType.COMMIT_RESPONSE,
            new CommitResponsePayload(
                StructType.of(),
                commitId,
                new TableName(ImmutableList.of("db"), "tbl"),
                dataFiles,
                deleteFiles));
    bytes = Event.encode(commitResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    Event commitReady =
        new Event(
            config.controlGroupId(),
            EventType.COMMIT_READY,
            new CommitReadyPayload(
                commitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts))));
    bytes = Event.encode(commitReady);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", bytes));

    when(config.commitIntervalMs()).thenReturn(0);

    coordinator.process();

    return commitId;
  }

  private UUID coordinatorTest(Function<UUID, List<Event>> eventsFn) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    Coordinator coordinator = new Coordinator(catalog, config, ImmutableList.of(), clientFactory);
    coordinator.start();

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
