// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class EventTest {

  @Test
  public void testBeginCommitSerialization() throws Exception {
    Event event = new Event(StructType.of(), UUID.randomUUID(), EventType.BEGIN_COMMIT);

    byte[] data = AvroEncoderUtil.encode(event, event.getSchema());
    Event result = AvroEncoderUtil.decode(data);

    assertEquals(event.getCommitId(), result.getCommitId());
    assertEquals(event.getType(), result.getType());
  }

  @Test
  public void testDataFilesSerialization() throws Exception {
    Event event =
        new Event(
            StructType.of(),
            UUID.randomUUID(),
            EventType.DATA_FILES,
            ImmutableList.of(createDataFile(), createDataFile()),
            ImmutableList.of(new TopicAndPartition("topic", 1), new TopicAndPartition("topic", 2)));

    byte[] data = AvroEncoderUtil.encode(event, event.getSchema());
    Event result = AvroEncoderUtil.decode(data);

    assertEquals(event.getCommitId(), result.getCommitId());
    assertEquals(event.getType(), result.getType());
    assertThat(result.getDataFiles()).hasSize(2);
    assertThat(result.getDataFiles()).allMatch(f -> f.specId() == 1);
    assertThat(result.getAssignments()).hasSize(2);
    assertThat(result.getAssignments()).allMatch(tp -> tp.getTopic().equals("topic"));
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
