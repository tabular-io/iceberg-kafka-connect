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
package io.tabular.iceberg.connect.transforms;

import io.tabular.iceberg.connect.transforms.util.KafkaMetadataAppender;
import io.tabular.iceberg.connect.transforms.util.RecordAppender;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

public class KafkaMetadataTransform<R extends ConnectRecord<R>> implements Transformation<R> {
  private RecordAppender<R> kafkaAppender;

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              KafkaMetadataAppender.INCLUDE_KAFKA_METADATA,
              ConfigDef.Type.BOOLEAN,
              false,
              ConfigDef.Importance.LOW,
              "Include appending of Kafka metadata to SinkRecord")
          .define(
              KafkaMetadataAppender.KEY_METADATA_FIELD_NAME,
              ConfigDef.Type.STRING,
              KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME,
              ConfigDef.Importance.LOW,
              "field to append Kafka metadata under")
          .define(
              KafkaMetadataAppender.KEY_METADATA_IS_NESTED,
              ConfigDef.Type.BOOLEAN,
              false,
              ConfigDef.Importance.LOW,
              "(true/false) to make a nested record under name or prefix names on the top level")
          .define(
              KafkaMetadataAppender.EXTERNAL_KAFKA_METADATA,
              ConfigDef.Type.STRING,
              "none",
              ConfigDef.Importance.LOW,
              "key,value representing a String to be injected on Kafka metadata (e.g. Cluster)");

  @Override
  public R apply(R record) {
    if (record.value() == null) {
      return record;
    } else if (record.valueSchema() == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applyWithSchema(R record) {
    Struct value = Requirements.requireStruct(record.value(), "KafkaMetadata transform");
    Schema newSchema = makeUpdatedSchema(record.valueSchema());
    Struct newValue = new Struct(newSchema);
    for (Field field : record.valueSchema().fields()) {
      newValue.put(field.name(), value.get(field));
    }
    kafkaAppender.addToStruct(record, newValue);
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        newSchema,
        newValue,
        record.timestamp(),
        record.headers());
  }

  private R applySchemaless(R record) {
    Map<String, Object> value = Requirements.requireMap(record.value(), "KafkaMetadata transform");
    Map<String, Object> newValue = Maps.newHashMap(value);
    kafkaAppender.addToMap(record, newValue);

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        null,
        newValue,
        record.timestamp(),
        record.headers());
  }

  private Schema makeUpdatedSchema(Schema schema) {
    SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
    for (Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }
    kafkaAppender.addToSchema(builder);
    return builder.build();
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {
    kafkaAppender = KafkaMetadataAppender.from(configs);
  }
}
