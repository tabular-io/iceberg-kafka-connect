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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.data.IcebergWriter;
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.RecordWriter;
import io.tabular.iceberg.connect.data.WriterResult;
import io.tabular.iceberg.connect.deadletter.DeadLetterUtils;
import io.tabular.iceberg.connect.events.CommitReadyPayload;
import io.tabular.iceberg.connect.events.CommitRequestPayload;
import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventTestUtil;
import io.tabular.iceberg.connect.events.EventType;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class WorkerTest extends ChannelTestBase {

  private static final String TABLE_NAME = "db.tbl";
  private static final String FIELD_NAME = "fld";

  private static class RecordingRecordWriter implements RecordWriter {

    private final boolean shouldThrowOnFirstRecord;
    private int count = 0;

    RecordingRecordWriter(boolean shouldThrowOnFirstRecord) {
      this.shouldThrowOnFirstRecord = shouldThrowOnFirstRecord;
    }

    List<SinkRecord> written = Lists.newArrayList();

    public void write(SinkRecord record) {
      if (shouldThrowOnFirstRecord && count == 0) {
        count += 1;
        throw new IllegalArgumentException("test throw");
      }
      written.add(record);
      count += 1;
    }
  }

  private static final byte[] ORIGINAL_BYTES =
      "{\"field\":\"success\"}".getBytes(StandardCharsets.UTF_8);

  private SinkRecord errorTransformSuccessRecord() {
    Schema schema =
        SchemaBuilder.struct()
            .field("field", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();
    Struct struct = new Struct(schema);
    struct.put("field", "success");
    Map<String, Object> map = Maps.newHashMap();
    SinkRecord success =
        new SinkRecord(
            "topic", 0, null, null, schema, struct, 100L, 1000L, TimestampType.CREATE_TIME);
    map.put(DeadLetterUtils.PAYLOAD_KEY, success);
    Map<String, Object> failed = Maps.newHashMap();
    failed.put(DeadLetterUtils.VALUE_BYTES, ORIGINAL_BYTES);
    map.put(DeadLetterUtils.ORIGINAL_BYTES_KEY, failed);
    return new SinkRecord(
        "topic", 0, null, null, null, map, 100L, 1000L, TimestampType.CREATE_TIME);
  }

  private SinkRecord errorTransformFailedRecord() {
    SinkRecord originalRecord =
        new SinkRecord(
            "topic", 0, null, null, null, ORIGINAL_BYTES, 100L, 1000L, TimestampType.CREATE_TIME);
    return DeadLetterUtils.failedRecord(
        originalRecord, new IllegalArgumentException("test"), "location");
  }

  @Test
  public void testStaticRoute() {
    when(config.tables()).thenReturn(ImmutableList.of(TABLE_NAME));
    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, "val");
    workerTest(value);
  }

  @Test
  public void testDynamicRoute() {
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    workerTest(value);
  }

  private void workerTest(Map<String, Object> value) {
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 0)));

    WriterResult writeResult =
        new WriterResult(
            TableIdentifier.parse(TABLE_NAME),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(ImmutableList.of(writeResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Worker worker = new Worker(config, clientFactory, writerFactory, context);
    worker.start();

    // init consumer after subscribe()
    initConsumer();

    // save a record
    SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
    worker.save(ImmutableList.of(rec));

    UUID commitId = UUID.randomUUID();
    Event commitRequest =
        new Event(
            config.controlGroupId(), EventType.COMMIT_REQUEST, new CommitRequestPayload(commitId));
    byte[] bytes = Event.encode(commitRequest);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    worker.process();

    assertThat(producer.history()).hasSize(2);

    Event event = Event.decode(producer.history().get(0).value());
    assertThat(event.type()).isEqualTo(EventType.COMMIT_RESPONSE);
    CommitResponsePayload responsePayload = (CommitResponsePayload) event.payload();
    assertThat(responsePayload.commitId()).isEqualTo(commitId);

    event = Event.decode(producer.history().get(1).value());
    assertThat(event.type()).isEqualTo(EventType.COMMIT_READY);
    CommitReadyPayload readyPayload = (CommitReadyPayload) event.payload();
    assertThat(readyPayload.commitId()).isEqualTo(commitId);
    assertThat(readyPayload.assignments()).hasSize(1);
    // offset should be one more than the record offset
    assertThat(readyPayload.assignments().get(0).offset()).isEqualTo(1L);
  }

  @Test
  @DisplayName("DeadLetterWriterForTable should ignore null records")
  public void deadLetterWriterForTableNullRecords() {
    RecordingRecordWriter writer = new RecordingRecordWriter(false);

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Map<String, RecordWriter> writerMap = Maps.newHashMap();

    Worker.DeadLetterWriterForTable writerForTable =
        new Worker.DeadLetterWriterForTable(writerFactory, writerMap, config);

    SinkRecord nullRecord = new SinkRecord(SRC_TOPIC_NAME, 0, null, null, null, null, 0);
    writerForTable.write(TABLE_NAME, nullRecord, true);

    assertThat(writer.written).isEmpty();
  }

  @Test
  @DisplayName("DeadLetterWriterForTable should write successful ErrorTransformed records")
  public void deadLetterWriterForTableSuccessErrorTransform() {
    RecordingRecordWriter writer = new RecordingRecordWriter(false);

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Map<String, RecordWriter> writerMap = Maps.newHashMap();

    Worker.DeadLetterWriterForTable writerForTable =
        new Worker.DeadLetterWriterForTable(writerFactory, writerMap, config);

    writerForTable.write(TABLE_NAME, errorTransformSuccessRecord(), true);
    Schema expectedSchema =
        SchemaBuilder.struct()
            .field("field", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();
    assertThat(writer.written.size()).isEqualTo(1);
    SinkRecord result = writer.written.get(0);
    assertThat(result.valueSchema()).isEqualTo(expectedSchema);
    assertThat(result.value()).isInstanceOf(Struct.class);
    Struct resultStruct = (Struct) result.value();
    assertThat(resultStruct.get("field")).isEqualTo("success");
  }

  @Test
  @DisplayName("DeadLetterWriterForTable should write failed ErrorTransformed records")
  public void deadLetterWriterForTableFailureErrorTransform() {
    RecordingRecordWriter writer = new RecordingRecordWriter(false);

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Map<String, RecordWriter> writerMap = Maps.newHashMap();

    Worker.DeadLetterWriterForTable writerForTable =
        new Worker.DeadLetterWriterForTable(writerFactory, writerMap, config);

    writerForTable.write(TABLE_NAME, errorTransformFailedRecord(), true);
    assertThat(writer.written.size()).isEqualTo(1);
    SinkRecord result = writer.written.get(0);
    assertThat(result.valueSchema()).isEqualTo(DeadLetterUtils.FAILED_SCHEMA);
    assertThat(result.value()).isInstanceOf(Struct.class);
    Struct resultValue = (Struct) result.value();
    assertThat(resultValue.get("exception").toString().contains("test")).isTrue();
    String resultBytesAsString =
        new String((byte[]) resultValue.get("value_bytes"), StandardCharsets.UTF_8);
    assertThat(resultBytesAsString).isEqualTo(new String(ORIGINAL_BYTES, StandardCharsets.UTF_8));
  }

  @Test
  @DisplayName(
      "DeadLetterWriterForTable should convert Iceberg Writer failures to failed SinkRecord")
  public void deadLetterWriterForTableConvertWriterFailures() {
    RecordingRecordWriter writer = new RecordingRecordWriter(true);

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Map<String, RecordWriter> writerMap = Maps.newHashMap();

    Worker.DeadLetterWriterForTable writerForTable =
        new Worker.DeadLetterWriterForTable(writerFactory, writerMap, config);

    writerForTable.write(TABLE_NAME, errorTransformSuccessRecord(), true);
    assertThat(writer.written.size()).isEqualTo(1);
    SinkRecord result = writer.written.get(0);
    assertThat(result.valueSchema()).isEqualTo(DeadLetterUtils.FAILED_SCHEMA);
    assertThat(result.value()).isInstanceOf(Struct.class);
    Struct resultValue = (Struct) result.value();
    assertThat(resultValue.get("exception").toString().contains("test throw")).isTrue();
    String resultBytesAsString =
        new String((byte[]) resultValue.get("value_bytes"), StandardCharsets.UTF_8);
    assertThat(resultBytesAsString).isEqualTo(new String(ORIGINAL_BYTES, StandardCharsets.UTF_8));
  }
}
