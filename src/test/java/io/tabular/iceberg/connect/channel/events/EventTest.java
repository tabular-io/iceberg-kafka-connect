// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class EventTest {

  @Test
  public void testCommitRequestSerialization() throws Exception {
    UUID commitId = UUID.randomUUID();
    Event event = new Event(EventType.COMMIT_REQUEST, new CommitRequestPayload(commitId));

    byte[] data = AvroEncoderUtil.encode(event, event.getSchema());
    Event result = AvroEncoderUtil.decode(data);

    assertEquals(event.getType(), result.getType());
    CommitRequestPayload payload = (CommitRequestPayload) result.getPayload();
    assertEquals(commitId, payload.getCommitId());
  }

  @Test
  public void testCommitResponseSerialization() throws Exception {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            EventType.COMMIT_RESPONSE,
            new CommitResponsePayload(
                StructType.of(),
                commitId,
                new TableName(ImmutableList.of("db"), "tbl"),
                ImmutableList.of(createDataFile(), createDataFile()),
                ImmutableList.of(
                    new TopicPartitionOffset("topic", 1, 1L),
                    new TopicPartitionOffset("topic", 2, null))));

    byte[] data = AvroEncoderUtil.encode(event, event.getSchema());
    Event result = AvroEncoderUtil.decode(data);

    assertEquals(event.getType(), result.getType());
    CommitResponsePayload payload = (CommitResponsePayload) result.getPayload();
    assertEquals(commitId, payload.getCommitId());
    assertThat(payload.getDataFiles()).hasSize(2);
    assertThat(payload.getDataFiles()).allMatch(f -> f.specId() == 1);
    assertThat(payload.getAssignments()).hasSize(2);
    assertThat(payload.getAssignments()).allMatch(tp -> tp.getTopic().equals("topic"));
  }

  private DataFile createDataFile() throws Exception {
    Class<?> clazz = Class.forName("org.apache.iceberg.GenericDataFile");
    Constructor<?> ctor =
        clazz.getDeclaredConstructor(
            int.class,
            String.class,
            FileFormat.class,
            PartitionData.class,
            long.class,
            Metrics.class,
            ByteBuffer.class,
            List.class,
            Integer.class);
    ctor.setAccessible(true);

    PartitionData partitionData =
        new PartitionData(StructType.of(required(999, "type", StringType.get())));
    Metrics metrics =
        new Metrics(1L, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());

    return (DataFile)
        ctor.newInstance(1, "hi", FileFormat.PARQUET, partitionData, 1L, metrics, null, null, null);
  }
}
