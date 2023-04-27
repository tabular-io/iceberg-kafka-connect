// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.commit;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.tabular.iceberg.connect.commit.Message.Type;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class MessageTest {

  @Test
  public void testBeginCommitSerialization() throws Exception {
    Message message = new Message(StructType.of());
    message.setCommitId(UUID.randomUUID());
    message.setType(Type.BEGIN_COMMIT);

    byte[] data = AvroEncoderUtil.encode(message, message.getAvroSchema());
    Message result = AvroEncoderUtil.decode(data);

    assertEquals(message.getCommitId(), result.getCommitId());
    assertEquals(message.getType(), result.getType());
  }

  @Test
  public void testDataFilesSerialization() throws Exception {
    Message message = new Message(StructType.of());
    message.setCommitId(UUID.randomUUID());
    message.setType(Type.DATA_FILES);
    message.setDataFiles(List.of(createDataFile(), createDataFile()));
    message.setAssignments(
        List.of(new TopicPartitionData("topic", 1), new TopicPartitionData("topic", 2)));

    byte[] data = AvroEncoderUtil.encode(message, message.getAvroSchema());
    Message result = AvroEncoderUtil.decode(data);

    assertEquals(message.getCommitId(), result.getCommitId());
    assertEquals(message.getType(), result.getType());
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
    Metrics metrics = new Metrics(1L, Map.of(), Map.of(), Map.of(), Map.of());

    return (DataFile)
        ctor.newInstance(1, "hi", FileFormat.PARQUET, partitionData, 1L, metrics, null, null, null);
  }
}
