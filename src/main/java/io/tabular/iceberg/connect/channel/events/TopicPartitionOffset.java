// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

import static org.apache.iceberg.avro.AvroSchemaUtil.FIELD_ID_PROP;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class TopicPartitionOffset implements Element {

  private String topic;
  private Integer partition;
  private Long offset;
  private Schema avroSchema;

  public static final Schema AVRO_SCHEMA =
      SchemaBuilder.builder()
          .record(TopicPartitionOffset.class.getName())
          .fields()
          .name("topic")
          .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
          .type()
          .stringType()
          .noDefault()
          .name("partition")
          .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
          .type()
          .intType()
          .noDefault()
          .name("offset")
          .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
          .type()
          .nullable()
          .longType()
          .noDefault()
          .endRecord();

  public TopicPartitionOffset(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public TopicPartitionOffset(String topic, int partition, Long offset) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.avroSchema = AVRO_SCHEMA;
  }

  public String getTopic() {
    return topic;
  }

  public Integer getPartition() {
    return partition;
  }

  public Long getOffset() {
    return offset;
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
      case 2:
        this.offset = (Long) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return topic;
      case 1:
        return partition;
      case 2:
        return offset;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
