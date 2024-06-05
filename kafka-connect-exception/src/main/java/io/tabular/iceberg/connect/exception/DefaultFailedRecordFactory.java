/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.exception;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class DefaultFailedRecordFactory implements FailedRecordFactory {
  private static final String DEAD_LETTER_TABLE_NAME_PROP = "table_name";

  private static final String DEAD_LETTER_ROUTE_FIELD_PROP = "route_field";
  private static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              DEAD_LETTER_TABLE_NAME_PROP,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.MEDIUM,
              "dead letter table name namespace.table"
          )
          .define(DEAD_LETTER_ROUTE_FIELD_PROP,
              ConfigDef.Type.STRING,
             null,
              ConfigDef.Importance.MEDIUM,
             "route field to inject table name on"
          );

  private static final String HEADERS = "headers";
  private Schema schema;

  private String deadLetterTableName;
  private String deadLetterRouteField;

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public SinkRecord recordFromSmt(SinkRecord original, Throwable error) {
    Struct struct = new Struct(schema);
    addCommon(struct, original, error);

    if (original.key() != null) {
      struct.put("key_bytes", original.key());
    }
    if (original.value() != null) {
      struct.put("value_bytes", original.value());
    }
    if (!original.headers().isEmpty()) {
      struct.put(HEADERS, DeadLetterUtils.serializedHeaders(original));
    }

    return original.newRecord(
        original.topic(),
        original.kafkaPartition(),
        null,
        null,
        schema,
        struct,
        original.timestamp());
  }

  @Override
  public SinkRecord recordFromConnector(SinkRecord record, Throwable error) {

    Struct struct = new Struct(schema);
    addCommon(struct, record, error);

    Headers headers = record.headers();

    Header originalData = headers.lastWithName(DeadLetterUtils.ORIGINAL_DATA);
    Struct originalStruct = (Struct) originalData.value();

    Object keyBytes = originalStruct.get(DeadLetterUtils.KEY);
    Object valueBytes = originalStruct.get(DeadLetterUtils.VALUE);
    Object serializedHeader = originalStruct.get(DeadLetterUtils.HEADERS);

    if (keyBytes != null) {
      struct.put("key_bytes", keyBytes);
    }
    if (valueBytes != null) {
      struct.put("value_bytes", valueBytes);
    }
    if (serializedHeader != null) {
      struct.put(HEADERS, serializedHeader);
    }

    return record.newRecord(
        record.topic(), record.kafkaPartition(), null, null, schema, struct, record.timestamp());
  }

  @Override
  public void configure(Map<String, ?> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    deadLetterTableName = config.getString(DEAD_LETTER_TABLE_NAME_PROP);
    deadLetterRouteField = config.getString(DEAD_LETTER_ROUTE_FIELD_PROP);
    if (deadLetterTableName == null) {
      throw new IllegalArgumentException("Dead letter table name cannot be null");
    }
    schema =
        SchemaBuilder.struct()
            .field("topic", Schema.STRING_SCHEMA)
            .field("partition", Schema.INT32_SCHEMA)
            .field("offset", Schema.INT64_SCHEMA)
            .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
            .field("exception", Schema.OPTIONAL_STRING_SCHEMA)
            .field("stack_trace", Schema.OPTIONAL_STRING_SCHEMA)
            .field("key_bytes", Schema.OPTIONAL_BYTES_SCHEMA)
            .field("value_bytes", Schema.OPTIONAL_BYTES_SCHEMA)
            .field(HEADERS, DeadLetterUtils.HEADER_SCHEMA)
            .field("target_table", Schema.OPTIONAL_STRING_SCHEMA)
            .field(deadLetterRouteField, Schema.STRING_SCHEMA)
            .build();
  }

  private void addCommon(Struct struct, SinkRecord record, Throwable error) {
    struct.put("topic", record.topic());
    struct.put("partition", record.kafkaPartition());
    struct.put("offset", record.kafkaOffset());
    struct.put("timestamp", record.timestamp());
    struct.put("exception", error.toString());
    String stack = DeadLetterUtils.stackTrace(error);
    if (!stack.isEmpty()) {
      struct.put("stack_trace", stack);
    }

    struct.put(deadLetterRouteField, deadLetterTableName);
  }
}
