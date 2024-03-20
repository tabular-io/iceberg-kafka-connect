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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.events.CommitReadyPayload;
import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventType;
import io.tabular.iceberg.connect.events.TopicPartitionOffset;
import io.tabular.iceberg.connect.internal.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.internal.data.IcebergWriterFactoryImpl;
import io.tabular.iceberg.connect.internal.data.RecordWriter;
import io.tabular.iceberg.connect.internal.data.WriterResult;
import io.tabular.iceberg.connect.internal.kafka.Factory;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PartitionWorkerTest {

  // TODO: tests for dynamic route writes
  // TODO: tests for table doesn't exist but auto-create on
  // TODO: tests for table doesn't exist and auto-create off (should use no-op writer)
  // TODO: test for missing namespace
  // TODO: test producer fails to send what happens?

  private static final String SOURCE_TOPIC = "source-topic-name";
  private static final String CONTROL_TOPIC = "control-topic-name";
  private InMemoryCatalog inMemoryCatalog;
  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final String TABLE_1_NAME = "db.tbl1";
  private static final String TABLE_2_NAME = "db.tbl2";
  private static final TableIdentifier TABLE_1_IDENTIFIER = TableIdentifier.parse(TABLE_1_NAME);
  private static final TableIdentifier TABLE_2_IDENTIFIER = TableIdentifier.parse(TABLE_2_NAME);
  private static final String ID_FIELD_NAME = "id";
  private static final Schema SCHEMA =
      new Schema(required(1, ID_FIELD_NAME, Types.StringType.get()));

  private static final String CONNECTOR_NAME = "connector-name";
  private static final String CONNECTOR_GROUP_ID = String.format("connect-%s", CONNECTOR_NAME);
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(SOURCE_TOPIC, 0);

  private static final IcebergSinkConfig BASIC_CONFIGS =
      new IcebergSinkConfig(
          ImmutableMap.of(
              "name", CONNECTOR_NAME,
              "iceberg.catalog.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog",
              "iceberg.tables", TABLE_1_NAME,
              "iceberg.control.topic", CONTROL_TOPIC));

  private static class RecordingProducerFactory implements Factory<Producer<String, byte[]>> {
    private final List<MockProducer<String, byte[]>> transactionalProducers = Lists.newArrayList();

    @Override
    public Producer<String, byte[]> create(Map<String, String> kafkaProps) {
      final MockProducer<String, byte[]> producer =
          new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
      producer.initTransactions();
      transactionalProducers.add(producer);
      return producer;
    }
  }

  private static class RecordingRecordWriter implements RecordWriter {
    private final RecordWriter underlying;
    private boolean isClosed = false;
    private final List<SinkRecord> records = Lists.newArrayList();

    RecordingRecordWriter(RecordWriter recordWriter) {
      underlying = recordWriter;
    }

    @Override
    public void write(SinkRecord record) {
      underlying.write(record);
      records.add(record);
    }

    @Override
    public List<WriterResult> complete() {
      return underlying.complete();
    }

    @Override
    public void close() {
      underlying.close();
      records.clear();
      isClosed = true;
    }
  }

  private static class RecordingWriterFactory implements IcebergWriterFactory {

    private final IcebergWriterFactoryImpl writerFactory;
    private final List<RecordingRecordWriter> writers = Lists.newArrayList();

    RecordingWriterFactory(Catalog catalog, IcebergSinkConfig config) {
      writerFactory = new IcebergWriterFactoryImpl(catalog, config);
    }

    @Override
    public RecordWriter createWriter(
        String tableName, SinkRecord sample, boolean ignoreMissingTable) {
      final RecordingRecordWriter writer =
          new RecordingRecordWriter(
              writerFactory.createWriter(tableName, sample, ignoreMissingTable));
      writers.add(writer);
      return writer;
    }
  }

  private void assertWriterResult(
      WriterResult writerResult,
      TableIdentifier expectedTableIdentifier,
      int expectedNumRecordsInDataFiles,
      int expectedNumRecordsInDeleteFiles) {
    assertThat(writerResult.tableIdentifier()).isEqualTo(expectedTableIdentifier);
    assertThat(writerResult.dataFiles().stream().map(DataFile::recordCount).reduce(0L, Long::sum))
        .isEqualTo(expectedNumRecordsInDataFiles);
    assertThat(
            writerResult.deleteFiles().stream().map(DeleteFile::recordCount).reduce(0L, Long::sum))
        .isEqualTo(expectedNumRecordsInDeleteFiles);
    // TODO: assertThat(writerResult.partitionStruct()).isEqualTo(expectedNumRecordsInDeleteFiles);
  }

  private void assertProducerRecordIsCommitReady(
      ProducerRecord<String, byte[]> producerRecord,
      String expectedTopic,
      String expectedGroupId,
      UUID expectedCommitId,
      TopicPartition expectedTopicPartition,
      Long expectedOffset,
      Long expectedTimestamp) {
    assertThat(producerRecord.topic()).isEqualTo(expectedTopic);

    // TODO: could do better
    assertThatNoException().isThrownBy(() -> UUID.fromString(producerRecord.key()));

    final Event actualEvent = Event.decode(producerRecord.value());
    assertThat(actualEvent.groupId()).isEqualTo(expectedGroupId);
    assertThat(actualEvent.type()).isEqualTo(EventType.COMMIT_READY);
    assertThat(actualEvent.payload()).isInstanceOf(CommitReadyPayload.class);

    final CommitReadyPayload actualCommitReadyPayload = (CommitReadyPayload) actualEvent.payload();
    assertThat(actualCommitReadyPayload.commitId()).isEqualTo(expectedCommitId);
    assertThat(actualCommitReadyPayload.assignments()).hasSize(1);
    final TopicPartitionOffset actualAssignment = actualCommitReadyPayload.assignments().get(0);
    assertThat(actualAssignment.topic()).isEqualTo(expectedTopicPartition.topic());
    assertThat(actualAssignment.partition()).isEqualTo(expectedTopicPartition.partition());
    assertThat(actualAssignment.offset()).isEqualTo(expectedOffset);
    assertThat(actualAssignment.timestamp()).isEqualTo(expectedTimestamp);
  }

  private void assertProducerRecordIsCommitResponse(
      ProducerRecord<String, byte[]> producerRecord,
      String expectedTopic,
      String expectedGroupId,
      UUID expectedCommitId,
      TableIdentifier expectedTableIdentifier,
      long expectedNumRecordsInDataFiles,
      long expectedNumRecordsInDeleteFiles) {
    assertThat(producerRecord.topic()).isEqualTo(expectedTopic);

    // TODO: could do better
    assertThatNoException().isThrownBy(() -> UUID.fromString(producerRecord.key()));

    final Event actualEvent = Event.decode(producerRecord.value());
    assertThat(actualEvent.groupId()).isEqualTo(expectedGroupId);
    assertThat(actualEvent.type()).isEqualTo(EventType.COMMIT_RESPONSE);
    assertThat(actualEvent.payload()).isInstanceOf(CommitResponsePayload.class);

    final CommitResponsePayload actualCommitResponsePayload =
        (CommitResponsePayload) actualEvent.payload();
    assertThat(actualCommitResponsePayload.commitId()).isEqualTo(expectedCommitId);
    assertThat(actualCommitResponsePayload.tableName().toIdentifier())
        .isEqualTo(expectedTableIdentifier);
    assertThat(
            actualCommitResponsePayload.dataFiles().stream()
                .map(DataFile::recordCount)
                .reduce(0L, Long::sum))
        .isEqualTo(expectedNumRecordsInDataFiles);
    assertThat(
            actualCommitResponsePayload.deleteFiles().stream()
                .map(DeleteFile::recordCount)
                .reduce(0L, Long::sum))
        .isEqualTo(expectedNumRecordsInDeleteFiles);
  }

  @BeforeEach
  public void before() {
    inMemoryCatalog = new InMemoryCatalog();
    inMemoryCatalog.initialize(null, ImmutableMap.of());
    inMemoryCatalog.createNamespace(NAMESPACE);
  }

  @AfterEach
  public void after() throws IOException {
    inMemoryCatalog.close();
  }

  private SinkRecord makeSinkRecord(long offset, Long timestamp) {
    return new SinkRecord(
        TOPIC_PARTITION.topic(),
        TOPIC_PARTITION.partition(),
        null,
        null,
        null,
        ImmutableMap.of(ID_FIELD_NAME, "val1"),
        offset,
        timestamp,
        TimestampType.LOG_APPEND_TIME);
  }

  @Test
  public void testShouldFenceZombiesOnWorkerCreation() {
    final RecordingProducerFactory producerFactory = new RecordingProducerFactory();
    final RecordingWriterFactory writerFactory =
        new RecordingWriterFactory(inMemoryCatalog, BASIC_CONFIGS);

    assertThat(producerFactory.transactionalProducers)
        .as("No producers should be initialized yet")
        .isEmpty();

    new PartitionWorker(BASIC_CONFIGS, TOPIC_PARTITION, writerFactory, producerFactory);

    assertThat(producerFactory.transactionalProducers)
        .as("Worker should have created one producer")
        .hasSize(1);
    final MockProducer<String, byte[]> producer = producerFactory.transactionalProducers.get(0);
    assertThat(producer.transactionInitialized())
        .as("Worker should have initialized transactions to fence out any zombies")
        .isTrue();
  }

  @Test
  public void testSavesRecordToStaticTable() {
    inMemoryCatalog.createTable(TABLE_1_IDENTIFIER, SCHEMA);

    final RecordingProducerFactory producerFactory = new RecordingProducerFactory();
    final RecordingWriterFactory writerFactory =
        new RecordingWriterFactory(inMemoryCatalog, BASIC_CONFIGS);

    final PartitionWorker worker =
        new PartitionWorker(BASIC_CONFIGS, TOPIC_PARTITION, writerFactory, producerFactory);

    assertThat(writerFactory.writers).as("No writers should be initialized yet").isEmpty();

    final SinkRecord sinkRecord = makeSinkRecord(10L, 100L);
    worker.save(ImmutableList.of(sinkRecord));

    assertThat(writerFactory.writers).as("Worker should have created a writer").hasSize(1);
    RecordingRecordWriter writer = writerFactory.writers.get(0);
    assertThat(writer.records).containsExactly(sinkRecord);
    assertThat(writer.isClosed).isFalse();

    // TODO: close worker after each test?
  }

  @Test
  public void testSavesMultipleRecordsForStaticTable() {
    inMemoryCatalog.createTable(TABLE_1_IDENTIFIER, SCHEMA);

    final RecordingProducerFactory producerFactory = new RecordingProducerFactory();
    final RecordingWriterFactory writerFactory =
        new RecordingWriterFactory(inMemoryCatalog, BASIC_CONFIGS);

    final PartitionWorker worker =
        new PartitionWorker(BASIC_CONFIGS, TOPIC_PARTITION, writerFactory, producerFactory);

    assertThat(writerFactory.writers).as("No writers should be initialized yet").isEmpty();

    final SinkRecord sinkRecord1 = makeSinkRecord(10L, 100L);
    final SinkRecord sinkRecord2 = makeSinkRecord(11L, 101L);
    worker.save(ImmutableList.of(sinkRecord1));
    worker.save(ImmutableList.of(sinkRecord2));

    assertThat(writerFactory.writers).as("Worker should have created a single writer").hasSize(1);
    final RecordingRecordWriter writer = writerFactory.writers.get(0);
    assertThat(writer.records).containsExactly(sinkRecord1, sinkRecord2);
    assertThat(writer.isClosed).isFalse();
  }

  @Test
  public void testSavesMultipleRecordsForMultipleStaticTables() {
    inMemoryCatalog.createTable(TABLE_1_IDENTIFIER, SCHEMA);
    inMemoryCatalog.createTable(TABLE_2_IDENTIFIER, SCHEMA);

    final RecordingProducerFactory producerFactory = new RecordingProducerFactory();
    final RecordingWriterFactory writerFactory =
        new RecordingWriterFactory(inMemoryCatalog, BASIC_CONFIGS);

    final PartitionWorker worker =
        new PartitionWorker(
            new IcebergSinkConfig(
                ImmutableMap.of(
                    "name",
                    "connector-name",
                    "iceberg.catalog.catalog-impl",
                    "org.apache.iceberg.inmemory.InMemoryCatalog",
                    "iceberg.tables",
                    String.format("%s,%s", TABLE_1_NAME, TABLE_2_NAME),
                    "iceberg.control.topic",
                    CONTROL_TOPIC)),
            TOPIC_PARTITION,
            writerFactory,
            producerFactory);

    assertThat(writerFactory.writers).as("No writers should be initialized yet").isEmpty();

    final SinkRecord sinkRecord1 = makeSinkRecord(10L, 100L);
    final SinkRecord sinkRecord2 = makeSinkRecord(11L, 101L);
    worker.save(ImmutableList.of(sinkRecord1));
    worker.save(ImmutableList.of(sinkRecord2));

    assertThat(writerFactory.writers)
        .as("Worker should have created two writers, one for each table")
        .hasSize(2);
    final RecordingRecordWriter writer1 = writerFactory.writers.get(0);
    assertThat(writer1.records).containsExactly(sinkRecord1, sinkRecord2);
    assertThat(writer1.isClosed).isFalse();
    final RecordingRecordWriter writer2 = writerFactory.writers.get(1);
    assertThat(writer2.records).containsExactly(sinkRecord1, sinkRecord2);
    assertThat(writer2.isClosed).isFalse();
  }

  @Test
  public void testCommitShouldSendCommitReadyEventIfNoRecordsHaveBeenProcessed() {
    inMemoryCatalog.createTable(TABLE_1_IDENTIFIER, SCHEMA);

    final RecordingProducerFactory producerFactory = new RecordingProducerFactory();
    final RecordingWriterFactory writerFactory =
        new RecordingWriterFactory(inMemoryCatalog, BASIC_CONFIGS);

    final PartitionWorker worker =
        new PartitionWorker(BASIC_CONFIGS, TOPIC_PARTITION, writerFactory, producerFactory);

    assertThat(producerFactory.transactionalProducers)
        .as("Worker should have created a producer")
        .hasSize(1);

    final UUID commitId = UUID.randomUUID();
    worker.commit(commitId);

    assertThat(producerFactory.transactionalProducers)
        .as("No new producers should have been created")
        .hasSize(1);
    final MockProducer<String, byte[]> producer = producerFactory.transactionalProducers.get(0);

    assertThat(producer.history()).as("Should have sent one message").hasSize(1);
    final ProducerRecord<String, byte[]> actualProducerRecord = producer.history().get(0);
    assertProducerRecordIsCommitReady(
        actualProducerRecord,
        CONTROL_TOPIC,
        BASIC_CONFIGS.controlGroupId(),
        commitId,
        TOPIC_PARTITION,
        null, // null because no records were processed
        null // null because no records were processed
        );

    assertThat(producer.transactionCommitted()).isTrue();
    assertThat(producer.consumerGroupOffsetsHistory())
        .as("Should have no consumer group offset history because no messages were processed")
        .hasSize(0);
  }

  @Test
  public void testCommitShouldSendResponseAndReadyEventAfterARecordHasBeenProcessed() {
    inMemoryCatalog.createTable(TABLE_1_IDENTIFIER, SCHEMA);

    final RecordingProducerFactory producerFactory = new RecordingProducerFactory();
    final RecordingWriterFactory writerFactory =
        new RecordingWriterFactory(inMemoryCatalog, BASIC_CONFIGS);

    final PartitionWorker worker =
        new PartitionWorker(BASIC_CONFIGS, TOPIC_PARTITION, writerFactory, producerFactory);

    assertThat(producerFactory.transactionalProducers)
        .as("Worker should have created a producer")
        .hasSize(1);

    final SinkRecord sinkRecord = makeSinkRecord(10L, 100L);
    worker.save(ImmutableList.of(sinkRecord));

    final UUID commitId = UUID.randomUUID();
    worker.commit(commitId);

    assertThat(producerFactory.transactionalProducers)
        .as("No new producers should have been created")
        .hasSize(1);
    final MockProducer<String, byte[]> producer = producerFactory.transactionalProducers.get(0);

    assertThat(producer.history()).as("Should have sent two messages").hasSize(2);
    assertProducerRecordIsCommitResponse(
        producer.history().get(0),
        CONTROL_TOPIC,
        BASIC_CONFIGS.controlGroupId(),
        commitId,
        TABLE_1_IDENTIFIER,
        1,
        0);
    assertProducerRecordIsCommitReady(
        producer.history().get(1),
        CONTROL_TOPIC,
        BASIC_CONFIGS.controlGroupId(),
        commitId,
        TOPIC_PARTITION,
        11L,
        100L);

    assertThat(producer.transactionCommitted()).isTrue();
    assertThat(producer.consumerGroupOffsetsHistory())
        .as("Should have consumer group offset history because messages were processed")
        .hasSize(1);
    assertThat(producer.consumerGroupOffsetsHistory().get(0))
        .isEqualTo(
            ImmutableMap.of(
                CONNECTOR_GROUP_ID, ImmutableMap.of(TOPIC_PARTITION, new OffsetAndMetadata(11L))));

    // calling commit again to make sure state was cleared correctly
    UUID newCommitId = UUID.randomUUID();
    worker.commit(newCommitId);

    assertThat(producerFactory.transactionalProducers)
        .as("No new producers should have been created")
        .hasSize(1);
    assertThat(producer.history()).as("Should have one new message").hasSize(3);
    assertProducerRecordIsCommitReady(
        producer.history().get(2),
        CONTROL_TOPIC,
        BASIC_CONFIGS.controlGroupId(),
        newCommitId,
        TOPIC_PARTITION,
        null, // no new messages processed since last commit so offset is null
        null); // no new messages processed since last commit so timestamp is null

    assertThat(producer.transactionCommitted()).isTrue();
    assertThat(producer.consumerGroupOffsetsHistory())
        .as("Should still have only one entry in consumer group offset history")
        .hasSize(1);
  }

  @Test
  public void testCommitAfterMultipleRecordsSaved() {
    // TODO:
    // should send event
    // should close writers

    // calling commit again should do nothing
  }

  @Test
  public void testClose() {
    inMemoryCatalog.createTable(TABLE_1_IDENTIFIER, SCHEMA);

    final RecordingProducerFactory producerFactory = new RecordingProducerFactory();
    final RecordingWriterFactory writerFactory =
        new RecordingWriterFactory(inMemoryCatalog, BASIC_CONFIGS);

    final Worker worker =
        new PartitionWorker(BASIC_CONFIGS, TOPIC_PARTITION, writerFactory, producerFactory);
    worker.save(ImmutableList.of(makeSinkRecord(10L, 100L)));

    assertThat(producerFactory.transactionalProducers)
        .as("Worker should have created one producer")
        .hasSize(1);
    final MockProducer<String, byte[]> producer = producerFactory.transactionalProducers.get(0);

    assertThat(writerFactory.writers).as("Worker should have created on wrier").hasSize(1);
    final RecordingRecordWriter writer = writerFactory.writers.get(0);

    assertThat(producer.closed()).isFalse();
    assertThat(writer.isClosed).isFalse();

    worker.close();

    assertThat(producer.closed()).isTrue();
    assertThat(writer.isClosed).isTrue();
  }
}
