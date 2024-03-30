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
package io.tabular.iceberg.connect.writer;

// import static org.apache.iceberg.types.Types.NestedField.required;
// import static org.assertj.core.api.Assertions.assertThat;
//
// import io.tabular.iceberg.connect.IcebergSinkConfig;
// import io.tabular.iceberg.connect.internal.impl.shared.data.IcebergWriterFactory;
// import io.tabular.iceberg.connect.internal.impl.shared.data.IcebergWriterFactoryImpl;
// import io.tabular.iceberg.connect.internal.impl.shared.data.Offset;
// import io.tabular.iceberg.connect.internal.impl.shared.data.RecordWriter;
// import io.tabular.iceberg.connect.internal.impl.shared.data.WriterResult;
// import java.io.IOException;
// import java.util.List;
// import java.util.Objects;
// import java.util.stream.Collectors;
// import org.apache.iceberg.DataFile;
// import org.apache.iceberg.DeleteFile;
// import org.apache.iceberg.PartitionSpec;
// import org.apache.iceberg.Schema;
// import org.apache.iceberg.catalog.Namespace;
// import org.apache.iceberg.catalog.TableIdentifier;
// import org.apache.iceberg.inmemory.InMemoryCatalog;
// import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
// import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
// import org.apache.iceberg.relocated.com.google.common.collect.Lists;
// import org.apache.iceberg.types.Types;
// import org.apache.kafka.common.TopicPartition;
// import org.apache.kafka.common.record.TimestampType;
// import org.apache.kafka.connect.sink.SinkRecord;
// import org.junit.jupiter.api.AfterEach;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;
//
// public class PartitionWorkerTest {
//
//  // TODO: tests for dynamic route writes
//  // TODO: tests for table doesn't exist but auto-create on
//  // TODO: tests for table doesn't exist and auto-create off (should use no-op writer)
//  // TODO: test for missing namespace
//  // TODO: maybe a test for some partitioned table
//
//  private static final Namespace NAMESPACE = Namespace.of("db");
//  private static final String TABLE_1_NAME = "db.tbl1";
//  private static final String TABLE_2_NAME = "db.tbl2";
//  private static final TableIdentifier TABLE_1_IDENTIFIER = TableIdentifier.parse(TABLE_1_NAME);
//  private static final TableIdentifier TABLE_2_IDENTIFIER = TableIdentifier.parse(TABLE_2_NAME);
//  private static final String ID_FIELD_NAME = "id";
//  private static final Schema SCHEMA =
//      new Schema(required(1, ID_FIELD_NAME, Types.StringType.get()));
//  private static final TopicPartition TOPIC_PARTITION = new TopicPartition("topic-name", 0);
//  private static final IcebergSinkConfig BASIC_CONFIGS =
//      new IcebergSinkConfig(
//          ImmutableMap.of(
//              "iceberg.catalog.catalog-impl",
//              "org.apache.iceberg.inmemory.InMemoryCatalog",
//              "iceberg.tables",
//              TABLE_1_NAME));
//
//  private InMemoryCatalog inMemoryCatalog;
//  private RecordingWriterFactory writerFactory;
//
//  private static class RecordingRecordWriter implements RecordWriter {
//    private final RecordWriter underlying;
//    private boolean isClosed = false;
//    private final List<SinkRecord> records = Lists.newArrayList();
//
//    RecordingRecordWriter(RecordWriter recordWriter) {
//      underlying = recordWriter;
//    }
//
//    @Override
//    public void write(SinkRecord record) {
//      underlying.write(record);
//      records.add(record);
//    }
//
//    @Override
//    public List<WriterResult> complete() {
//      return underlying.complete();
//    }
//
//    @Override
//    public void close() throws IOException {
//      underlying.close();
//      records.clear();
//      isClosed = true;
//    }
//  }
//
//  private static class RecordingWriterFactory implements IcebergWriterFactory {
//
//    private final IcebergWriterFactory underlying;
//    private final List<RecordingRecordWriter> writers = Lists.newArrayList();
//
//    RecordingWriterFactory(IcebergWriterFactory icebergWriterFactory) {
//      underlying = icebergWriterFactory;
//    }
//
//    @Override
//    public RecordWriter createWriter(
//        String tableName, SinkRecord sample, boolean ignoreMissingTable) {
//      final RecordingRecordWriter writer =
//          new RecordingRecordWriter(underlying.createWriter(tableName, sample,
// ignoreMissingTable));
//      writers.add(writer);
//      return writer;
//    }
//
//    public boolean allWritersClosed() {
//      return writers.stream().allMatch(w -> w.isClosed);
//    }
//  }
//
//  private static class ComparableCommittable {
//
//    private final TopicPartition topicPartition;
//    private final Offset offset;
//    private final List<ComparableTableCommittable> comparableTableCommittables;
//
//    ComparableCommittable(Committable committable) {
//      this.topicPartition = committable.topicPartition();
//      this.offset = committable.offset();
//      this.comparableTableCommittables =
//          committable.tableCommittables().stream()
//              .map(ComparableTableCommittable::new)
//              .collect(Collectors.toList());
//    }
//
//    ComparableCommittable(
//        TopicPartition topicPartition,
//        Offset offset,
//        List<ComparableTableCommittable> comparableTableCommittables) {
//      this.topicPartition = topicPartition;
//      this.offset = offset;
//      this.comparableTableCommittables = comparableTableCommittables;
//    }
//
//    @Override
//    public boolean equals(Object o) {
//      if (this == o) {
//        return true;
//      }
//      if (o == null || getClass() != o.getClass()) {
//        return false;
//      }
//      ComparableCommittable that = (ComparableCommittable) o;
//      return Objects.equals(topicPartition, that.topicPartition)
//          && Objects.equals(offset, that.offset)
//          && Objects.equals(comparableTableCommittables, that.comparableTableCommittables);
//    }
//
//    @Override
//    public int hashCode() {
//      return Objects.hash(topicPartition, offset, comparableTableCommittables);
//    }
//
//    @Override
//    public String toString() {
//      return "ComparableCommittable{"
//          + "topicPartition="
//          + topicPartition
//          + ", offset="
//          + offset
//          + ", comparableTableCommittables="
//          + comparableTableCommittables
//          + '}';
//    }
//  }
//
//  private static class ComparableTableCommittable {
//
//    private final TableIdentifier tableIdentifier;
//    private final Types.StructType partitionType;
//    private final Long numRecordsInDataFiles;
//    private final Long numRecordsInDeleteFiles;
//
//    ComparableTableCommittable(PartitionCommittable partitionCommittable) {
//      this.tableIdentifier = partitionCommittable.tableIdentifier();
//      this.partitionType = partitionCommittable.partitionType();
//      this.numRecordsInDataFiles =
//          partitionCommittable.dataFiles().stream()
//              .map(DataFile::recordCount)
//              .reduce(0L, Long::sum);
//      this.numRecordsInDeleteFiles =
//          partitionCommittable.deleteFiles().stream()
//              .map(DeleteFile::recordCount)
//              .reduce(0L, Long::sum);
//    }
//
//    ComparableTableCommittable(
//        TableIdentifier tableIdentifier,
//        Types.StructType partitionType,
//        Long numRecordsInDataFiles,
//        Long numRecordsInDeleteFiles) {
//      this.tableIdentifier = tableIdentifier;
//      this.partitionType = partitionType;
//      this.numRecordsInDataFiles = numRecordsInDataFiles;
//      this.numRecordsInDeleteFiles = numRecordsInDeleteFiles;
//    }
//
//    @Override
//    public boolean equals(Object o) {
//      if (this == o) {
//        return true;
//      }
//      if (o == null || getClass() != o.getClass()) {
//        return false;
//      }
//      ComparableTableCommittable that = (ComparableTableCommittable) o;
//      return Objects.equals(tableIdentifier, that.tableIdentifier)
//          && Objects.equals(partitionType, that.partitionType)
//          && Objects.equals(numRecordsInDataFiles, that.numRecordsInDataFiles)
//          && Objects.equals(numRecordsInDeleteFiles, that.numRecordsInDeleteFiles);
//    }
//
//    @Override
//    public int hashCode() {
//      return Objects.hash(
//          tableIdentifier, partitionType, numRecordsInDataFiles, numRecordsInDeleteFiles);
//    }
//
//    @Override
//    public String toString() {
//      return "ComparableTableCommittable{"
//          + "tableIdentifier="
//          + tableIdentifier
//          + ", partitionType="
//          + partitionType
//          + ", numRecordsInDataFiles="
//          + numRecordsInDataFiles
//          + ", numRecordsInDeleteFiles="
//          + numRecordsInDeleteFiles
//          + '}';
//    }
//  }
//
//  private List<ComparableCommittable> toComparable(List<Committable> committables) {
//    return committables.stream().map(ComparableCommittable::new).collect(Collectors.toList());
//  }
//
//  @BeforeEach
//  public void before() {
//    inMemoryCatalog = new InMemoryCatalog();
//    inMemoryCatalog.initialize(null, ImmutableMap.of());
//    inMemoryCatalog.createNamespace(NAMESPACE);
//
//    writerFactory =
//        new RecordingWriterFactory(new IcebergWriterFactoryImpl(inMemoryCatalog, BASIC_CONFIGS));
//  }
//
//  @AfterEach
//  public void after() throws IOException {
//    inMemoryCatalog.close();
//  }
//
//  private SinkRecord makeSinkRecord(long offset, Long timestamp) {
//    return new SinkRecord(
//        TOPIC_PARTITION.topic(),
//        TOPIC_PARTITION.partition(),
//        null,
//        null,
//        null,
//        ImmutableMap.of(ID_FIELD_NAME, "val1"),
//        offset,
//        timestamp,
//        TimestampType.LOG_APPEND_TIME);
//  }
//
//  @Test
//  public void testNoPuts() {
//    inMemoryCatalog.createTable(TABLE_1_IDENTIFIER, SCHEMA);
//
//    try (PartitionWorker partitionWorker =
//        new PartitionWorker(BASIC_CONFIGS, TOPIC_PARTITION, writerFactory)) {
//      final List<Committable> committables = partitionWorker.getCommittable();
//      assertThat(toComparable(committables))
//          .containsExactly(
//              new ComparableCommittable(TOPIC_PARTITION, Offset.NULL_OFFSET, ImmutableList.of()));
//    }
//  }
//
//  @Test
//  public void testPutRecordForStaticTable() {
//    inMemoryCatalog.createTable(TABLE_1_IDENTIFIER, SCHEMA);
//
//    try (PartitionWorker partitionWorker =
//        new PartitionWorker(BASIC_CONFIGS, TOPIC_PARTITION, writerFactory)) {
//      final SinkRecord sinkRecord = makeSinkRecord(10L, 100L);
//
//      partitionWorker.put(ImmutableList.of(sinkRecord));
//      final List<Committable> committables = partitionWorker.getCommittable();
//
//      assertThat(toComparable(committables))
//          .containsExactly(
//              new ComparableCommittable(
//                  TOPIC_PARTITION,
//                  new Offset(11L, 100L),
//                  ImmutableList.of(
//                      new ComparableTableCommittable(
//                          TABLE_1_IDENTIFIER,
//                          PartitionSpec.unpartitioned().partitionType(),
//                          1L,
//                          0L))));
//
//      assertThat(toComparable(partitionWorker.getCommittable()))
//          .as("Calling getCommittables again should return an empty list of table committables")
//          .containsExactly(
//              new ComparableCommittable(TOPIC_PARTITION, Offset.NULL_OFFSET, ImmutableList.of()));
//    }
//  }
//
//  @Test
//  public void testPutMultipleRecordsForStaticTable() {
//    inMemoryCatalog.createTable(TABLE_1_IDENTIFIER, SCHEMA);
//
//    try (PartitionWorker partitionWorker =
//        new PartitionWorker(BASIC_CONFIGS, TOPIC_PARTITION, writerFactory)) {
//      final SinkRecord sinkRecord1 = makeSinkRecord(10L, 100L);
//      final SinkRecord sinkRecord2 = makeSinkRecord(11L, 101L);
//
//      partitionWorker.put(ImmutableList.of(sinkRecord1));
//      partitionWorker.put(ImmutableList.of(sinkRecord2));
//      final List<Committable> committables = partitionWorker.getCommittable();
//
//      assertThat(toComparable(committables))
//          .containsExactly(
//              new ComparableCommittable(
//                  TOPIC_PARTITION,
//                  new Offset(12L, 101L),
//                  ImmutableList.of(
//                      new ComparableTableCommittable(
//                          TABLE_1_IDENTIFIER,
//                          PartitionSpec.unpartitioned().partitionType(),
//                          2L,
//                          0L))));
//
//      assertThat(toComparable(partitionWorker.getCommittable()))
//          .as("Calling getCommittables again should return an empty list of table committables")
//          .containsExactly(
//              new ComparableCommittable(TOPIC_PARTITION, Offset.NULL_OFFSET, ImmutableList.of()));
//    }
//  }
//
//  @Test
//  public void testPutMultipleRecordsForMultipleStaticTables() {
//    inMemoryCatalog.createTable(TABLE_1_IDENTIFIER, SCHEMA);
//    inMemoryCatalog.createTable(TABLE_2_IDENTIFIER, SCHEMA);
//
//    try (PartitionWorker partitionWorker =
//        new PartitionWorker(
//            new IcebergSinkConfig(
//                ImmutableMap.of(
//                    "iceberg.catalog.catalog-impl",
//                    "org.apache.iceberg.inmemory.InMemoryCatalog",
//                    "iceberg.tables",
//                    String.format("%s,%s", TABLE_1_NAME, TABLE_2_NAME))),
//            TOPIC_PARTITION,
//            writerFactory)) {
//      final SinkRecord sinkRecord1 = makeSinkRecord(10L, 100L);
//      final SinkRecord sinkRecord2 = makeSinkRecord(11L, 101L);
//
//      partitionWorker.put(ImmutableList.of(sinkRecord1));
//      partitionWorker.put(ImmutableList.of(sinkRecord2));
//      final List<Committable> committables = partitionWorker.getCommittable();
//
//      assertThat(toComparable(committables))
//          .containsExactly(
//              new ComparableCommittable(
//                  TOPIC_PARTITION,
//                  new Offset(12L, 101L),
//                  ImmutableList.of(
//                      new ComparableTableCommittable(
//                          TABLE_1_IDENTIFIER,
//                          PartitionSpec.unpartitioned().partitionType(),
//                          2L,
//                          0L),
//                      new ComparableTableCommittable(
//                          TABLE_2_IDENTIFIER,
//                          PartitionSpec.unpartitioned().partitionType(),
//                          2L,
//                          0L))));
//
//      assertThat(toComparable(partitionWorker.getCommittable()))
//          .as("Calling getCommittables again should return an empty list of table committables")
//          .containsExactly(
//              new ComparableCommittable(TOPIC_PARTITION, Offset.NULL_OFFSET, ImmutableList.of()));
//    }
//  }
//
//  @Test
//  public void testClose() {
//    inMemoryCatalog.createTable(TABLE_1_IDENTIFIER, SCHEMA);
//
//    final PartitionWorker partitionWorker =
//        new PartitionWorker(BASIC_CONFIGS, TOPIC_PARTITION, writerFactory);
//    partitionWorker.put(ImmutableList.of(makeSinkRecord(10L, 100L)));
//
//    assertThat(writerFactory.allWritersClosed()).isFalse();
//    partitionWorker.close();
//    assertThat(writerFactory.allWritersClosed()).isTrue();
//  }
// }
