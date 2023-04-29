// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

import static org.apache.iceberg.avro.AvroSchemaUtil.FIELD_ID_PROP;

import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class CommitRequestPayload implements Payload {

  private UUID commitId;
  private Schema avroSchema;

  public static final Schema AVRO_SCHEMA =
      SchemaBuilder.builder()
          .record(CommitRequestPayload.class.getName())
          .fields()
          .name("commitId")
          .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
          .type(UUID_SCHEMA)
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
        this.commitId = (UUID) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return commitId;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
