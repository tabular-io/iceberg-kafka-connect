// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData.SchemaConstructable;

public interface Element extends IndexedRecord, SchemaConstructable {
  // this is required by Iceberg's Avro deserializer to check for special metadata
  // fields, but we aren't using any
  String DUMMY_FIELD_ID = "-1";

  Schema UUID_SCHEMA =
      LogicalTypes.uuid().addToSchema(SchemaBuilder.builder().fixed("uuid").size(16));
}
