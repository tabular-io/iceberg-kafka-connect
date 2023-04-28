// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

import static org.apache.iceberg.avro.AvroSchemaUtil.FIELD_ID_PROP;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class TopicAndPartition implements Element {

  private String topic;
  private Integer partition;
  private Schema avroSchema;

  public static final Schema AVRO_SCHEMA =
      SchemaBuilder.builder()
          .record(TopicAndPartition.class.getName())
          .fields()
          .name("topic")
          .prop(FIELD_ID_PROP, "30")
          .type()
          .stringType()
          .noDefault()
          .name("partition")
          .prop(FIELD_ID_PROP, "31")
          .type()
          .intType()
          .noDefault()
          .endRecord();

  public TopicAndPartition(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public TopicAndPartition(String topic, int partition) {
    this.topic = topic;
    this.partition = partition;
    this.avroSchema = AVRO_SCHEMA;
  }

  public String getTopic() {
    return topic;
  }

  public Integer getPartition() {
    return partition;
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
