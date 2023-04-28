// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

import static org.apache.iceberg.avro.AvroSchemaUtil.FIELD_ID_PROP;

import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;

public class CommitRequestPayload implements Payload {

  private UUID commitId;
  private Schema avroSchema;

  public static final Schema AVRO_SCHEMA =
      SchemaBuilder.builder()
          .nullable()
          .record(CommitRequestPayload.class.getName())
          .fields()
          .name("commitId")
          .prop(FIELD_ID_PROP, "80")
          .type()
          .stringType()
          .noDefault()
          .endRecord();

  public CommitRequestPayload(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public CommitRequestPayload(UUID commitId) {
    this.commitId = commitId;
    this.avroSchema = AVRO_SCHEMA;
  }

  public UUID getCommitId() {
    return commitId;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.commitId = v == null ? null : UUID.fromString(((Utf8) v).toString());
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
        return commitId == null ? null : commitId.toString();
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
    return 1;
  }
}
