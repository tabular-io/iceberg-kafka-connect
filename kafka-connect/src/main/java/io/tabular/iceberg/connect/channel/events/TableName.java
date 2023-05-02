// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

import static java.util.stream.Collectors.toList;
import static org.apache.iceberg.avro.AvroSchemaUtil.FIELD_ID_PROP;

import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public class TableName implements Element {

  private List<String> namespace;
  private String name;
  private Schema avroSchema;

  public static final Schema AVRO_SCHEMA =
      SchemaBuilder.builder()
          .record(TableName.class.getName())
          .fields()
          .name("namespace")
          .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
          .type()
          .array()
          .items()
          .stringType()
          .noDefault()
          .name("name")
          .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
          .type()
          .stringType()
          .noDefault()
          .endRecord();

  public static TableName of(TableIdentifier tableIdentifier) {
    return new TableName(
        Arrays.asList(tableIdentifier.namespace().levels()), tableIdentifier.name());
  }

  public TableName(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public TableName(List<String> namespace, String name) {
    this.namespace = namespace;
    this.name = name;
    this.avroSchema = AVRO_SCHEMA;
  }

  public TableIdentifier toIdentifier() {
    Namespace icebergNamespace = Namespace.of(namespace.toArray(new String[0]));
    return TableIdentifier.of(icebergNamespace, name);
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.namespace =
            v == null ? null : ((List<Utf8>) v).stream().map(Utf8::toString).collect(toList());
        return;
      case 1:
        this.name = v == null ? null : v.toString();
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return namespace;
      case 1:
        return name;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
