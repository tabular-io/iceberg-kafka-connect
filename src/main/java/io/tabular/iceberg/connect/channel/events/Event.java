// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

import static org.apache.iceberg.avro.AvroSchemaUtil.FIELD_ID_PROP;

import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;

public class Event implements Element {

  private UUID id;
  private UUID commitId;
  private EventType type;
  private Payload payload;
  private Schema avroSchema;

  public Event(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public Event(UUID commitId, EventType type) {
    this(commitId, type, null);
  }

  public Event(UUID commitId, EventType type, Payload payload) {
    this.id = UUID.randomUUID();
    this.commitId = commitId;
    this.type = type;
    this.payload = payload;

    Schema payloadSchema =
        payload == null ? SchemaBuilder.builder().nullType() : payload.getSchema();

    this.avroSchema =
        SchemaBuilder.builder()
            .record(getClass().getName())
            .fields()
            .name("id")
            .prop(FIELD_ID_PROP, "1")
            .type()
            .stringType()
            .noDefault()
            .name("commitId")
            .prop(FIELD_ID_PROP, "2")
            .type()
            .stringType()
            .noDefault()
            .name("type")
            .prop(FIELD_ID_PROP, "3")
            .type()
            .intType()
            .noDefault()
            .name("payload")
            .prop(FIELD_ID_PROP, "4")
            .type(payloadSchema)
            .noDefault()
            .endRecord();
  }

  public UUID getId() {
    return id;
  }

  public UUID getCommitId() {
    return commitId;
  }

  public EventType getType() {
    return type;
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
        this.id = v == null ? null : UUID.fromString(((Utf8) v).toString());
        return;
      case 1:
        this.commitId = v == null ? null : UUID.fromString(((Utf8) v).toString());
        return;
      case 2:
        this.type = v == null ? null : EventType.values()[(Integer) v];
        return;
      case 3:
        this.payload = (Payload) v;
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
        return id == null ? null : id.toString();
      case 1:
        return commitId == null ? null : commitId.toString();
      case 2:
        return type == null ? null : type.getId();
      case 3:
        return payload;
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
    return 4;
  }
}
