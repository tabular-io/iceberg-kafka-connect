// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.commit;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.Serializable;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData.SchemaConstructable;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

@Getter
public class TopicPartitionData
    implements StructLike, IndexedRecord, SchemaConstructable, Serializable {

  private String topic;
  private Integer partition;
  private Schema avroSchema;

  public static final StructType STRUCT_TYPE =
      StructType.of(
          required(50, "topic", StringType.get()), required(51, "partition", IntegerType.get()));
  public static final Schema AVRO_SCHEMA =
      AvroSchemaUtil.convert(STRUCT_TYPE, TopicPartitionData.class.getName());

  public TopicPartitionData(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public TopicPartitionData(String topic, int partition) {
    this.topic = topic;
    this.partition = partition;
    this.avroSchema = AVRO_SCHEMA;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.topic = v == null ? null : v.toString();
        return;
      case 1:
        this.partition = (Integer) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public <T> void set(int pos, T value) {
    put(pos, value);
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return topic;
      case 1:
        return partition;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
  }

  @Override
  public int size() {
    return 2;
  }
}
