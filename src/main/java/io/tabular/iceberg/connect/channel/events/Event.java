// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

import static org.apache.iceberg.avro.AvroSchemaUtil.FIELD_ID_PROP;

import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class Event implements Element {

  private UUID id;
  private EventType type;
  private Long timestamp;
  private Payload payload;
  private Schema avroSchema;

  public Event(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public Event(EventType type, Payload payload) {
    this.id = UUID.randomUUID();
    this.type = type;
    this.timestamp = System.currentTimeMillis();
    this.payload = payload;

    this.avroSchema =
        SchemaBuilder.builder()
            .record(getClass().getName())
            .fields()
            .name("id")
            .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
            .type(UUID_SCHEMA)
            .noDefault()
            .name("type")
            .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
            .type()
            .intType()
            .noDefault()
            .name("timestamp")
            .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
            .type()
            .longType()
            .noDefault()
            .name("payload")
            .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
            .type(payload.getSchema())
            .noDefault()
            .endRecord();
  }

  public UUID getId() {
    return id;
  }

  public EventType getType() {
    return type;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public Payload getPayload() {
    return payload;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.id = (UUID) v;
        return;
      case 1:
        this.type = v == null ? null : EventType.values()[(Integer) v];
        return;
      case 2:
        this.timestamp = (Long) v;
        return;
      case 3:
        this.payload = (Payload) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return id;
      case 1:
        return type == null ? null : type.getId();
      case 2:
        return timestamp;
      case 3:
        return payload;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
