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
package io.tabular.iceberg.connect.internal.worker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class WorkerManagerTest {

  private static final String SOURCE_TOPIC = "source-topic-name";
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(SOURCE_TOPIC, 0);

  private static class RecordingWorker implements Worker {
    private final List<Collection<SinkRecord>> savedRecordBatches = Lists.newArrayList();

    private final List<UUID> commits = Lists.newArrayList();

    private boolean isClosed = false;

    @Override
    public void save(Collection<SinkRecord> sinkRecords) {
      savedRecordBatches.add(sinkRecords);
    }

    @Override
    public void commit(UUID commitId) {
      commits.add(commitId);
    }

    @Override
    public void close() {
      isClosed = true;
    }
  }

  private static class MockCommitRequestListener implements CommitRequestListener {

    private final Iterator<UUID> commitIds;

    MockCommitRequestListener(Iterator<UUID> commitIds) {
      this.commitIds = commitIds;
    }

    @Override
    public Optional<UUID> getCommitId() {
      return commitIds.hasNext() ? Optional.of(commitIds.next()) : Optional.empty();
    }

    @Override
    public void close() {}
  }

  private SinkRecord makeSinkRecord(long offset, Long timestamp) {
    return new SinkRecord(
        TOPIC_PARTITION.topic(),
        TOPIC_PARTITION.partition(),
        null,
        null,
        null,
        ImmutableMap.of("field_name", "field_value"),
        offset,
        timestamp,
        TimestampType.LOG_APPEND_TIME);
  }

  @Test
  public void testCallingSavePassesEmptyBatchOfRecordsToWorker() {
    final RecordingWorker worker = new RecordingWorker();
    final MockCommitRequestListener commitRequestListener =
        new MockCommitRequestListener(ImmutableList.<UUID>of().stream().iterator());
    final WorkerManager workerManager = new WorkerManager(worker, commitRequestListener);

    final ImmutableList<SinkRecord> emptyBatch = ImmutableList.of();
    workerManager.save(emptyBatch);

    assertThat(worker.savedRecordBatches).isEqualTo(ImmutableList.of(emptyBatch));
  }

  @Test
  public void testCallingSavePassesBatchOfSingleRecordToWorker() {
    final RecordingWorker worker = new RecordingWorker();
    final MockCommitRequestListener commitRequestListener =
        new MockCommitRequestListener(ImmutableList.<UUID>of().stream().iterator());
    final WorkerManager workerManager = new WorkerManager(worker, commitRequestListener);

    final ImmutableList<SinkRecord> batch = ImmutableList.of(makeSinkRecord(0L, 0L));
    workerManager.save(batch);

    assertThat(worker.savedRecordBatches).isEqualTo(ImmutableList.of(batch));
  }

  @Test
  public void testCallingSavePassesBatchOfRecordsToWorker() {
    final RecordingWorker worker = new RecordingWorker();
    final MockCommitRequestListener commitRequestListener =
        new MockCommitRequestListener(ImmutableList.<UUID>of().stream().iterator());
    final WorkerManager workerManager = new WorkerManager(worker, commitRequestListener);

    final ImmutableList<SinkRecord> batch =
        ImmutableList.of(makeSinkRecord(0L, 0L), makeSinkRecord(1L, 1L));
    workerManager.save(batch);

    assertThat(worker.savedRecordBatches).isEqualTo(ImmutableList.of(batch));
  }

  @Test
  public void testCallingSavePassesRecordsToWorkerAndPerformsCommitIfACommitRequestIsPresent() {
    // TODO
  }

  @Test
  public void testCallingCommitPerformsACommitIfACommitRequestIsPresent() {
    // TODO
  }

  @Test
  public void testCallingCommitDoesNotPerformACommitIfACommitRequestIsNotPresent() {
    // TODO
  }

  @Test
  public void testForErrorHandling() {
    // TODO: tests for error handling
  }

  @Test
  public void testCallingCloseClosesBothTheWorkerAndCommitRequestListener() {
    // TODO
  }
}
