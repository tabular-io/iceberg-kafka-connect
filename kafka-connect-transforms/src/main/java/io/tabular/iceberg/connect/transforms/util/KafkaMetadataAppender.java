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
package io.tabular.iceberg.connect.transforms.util;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class KafkaMetadataAppender {

  public static final String INCLUDE_KAFKA_METADATA = "cdc.kafka.include_metadata";
  public static final String EXTERNAL_KAFKA_METADATA = "cdc.kafka.external_field";
  public static final String KEY_METADATA_FIELD_NAME = "cdc.kafka.metadata_field";
  public static final String DEFAULT_METADATA_FIELD_NAME = "_kafka_metadata";

  private final ExternalKafkaData externalData;

  private final Schema schema;

  private final String metadataFieldName;

  public KafkaMetadataAppender(
      ExternalKafkaData externalData, Schema schema, String metadataFieldName) {
    this.externalData = externalData;
    this.schema = schema;
    this.metadataFieldName = metadataFieldName;
  }

  public interface ExternalKafkaData {
    SchemaBuilder addToSchema(SchemaBuilder builder);

    Struct addToStruct(Struct struct);

    Map<String, Object> addToMap(Map<String, Object> map);
  }

  public static class ExternalStringKafkaData implements ExternalKafkaData {
    private final String name;
    private final String value;

    public ExternalStringKafkaData(String name, String value) {
      this.name = name;
      this.value = value;
    }

    public static ExternalKafkaData parse(String field) {
      if (field.equals("none")) {
        return new EmptyExternalData();
      }
      String[] parts = field.split(",");
      if (parts.length != 2) {
        throw new ConfigException(
            String.format(
                "Could not parse %s for %s", field, KafkaMetadataAppender.EXTERNAL_KAFKA_METADATA));
      }
      return new ExternalStringKafkaData(parts[0], parts[1]);
    }

    @Override
    public SchemaBuilder addToSchema(SchemaBuilder builder) {
      return builder.field(this.name, Schema.STRING_SCHEMA);
    }

    @Override
    public Struct addToStruct(Struct struct) {
      return struct.put(this.name, this.value);
    }

    @Override
    public Map<String, Object> addToMap(Map<String, Object> map) {
      map.put(this.name, this.value);
      return map;
    }
  }

  public static class EmptyExternalData implements ExternalKafkaData {
    @Override
    public SchemaBuilder addToSchema(SchemaBuilder builder) {
      return builder;
    }

    @Override
    public Struct addToStruct(Struct struct) {
      return struct;
    }

    @Override
    public Map<String, Object> addToMap(Map<String, Object> map) {
      return map;
    }
  }

  public static KafkaMetadataAppender from(SimpleConfig config) {
    ExternalKafkaData externalAppender =
        ExternalStringKafkaData.parse(config.getString(EXTERNAL_KAFKA_METADATA));
    String metadataFieldName = config.getString(KEY_METADATA_FIELD_NAME);

    SchemaBuilder schema = SchemaBuilder.struct();

    externalAppender
        .addToSchema(schema)
        .field("topic", Schema.STRING_SCHEMA)
        .field("partition", Schema.INT32_SCHEMA)
        .field("offset", Schema.INT64_SCHEMA)
        .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA);
    // TODO headers
    return new KafkaMetadataAppender(externalAppender, schema.build(), metadataFieldName);
  }

  public SchemaBuilder appendSchema(SchemaBuilder builder) {
    return builder.field(this.metadataFieldName, this.schema);
  }

  public Struct appendToStruct(SinkRecord record, Struct struct) {
    Struct metadata = new Struct(this.schema);
    externalData.addToStruct(metadata);
    metadata.put("topic", record.topic());
    metadata.put("partition", record.kafkaPartition());
    metadata.put("offset", record.kafkaOffset());
    if (record.timestamp() == null) {
      metadata.put("timestamp", record.timestamp());
    }
    struct.put(this.metadataFieldName, metadata);
    return struct;
  }

  public Map<String, Object> appendToMap(SinkRecord record, Map<String, Object> map) {
    Map<String, Object> metadata = Maps.newHashMap();
    externalData.addToMap(metadata);
    metadata.put("topic", record.topic());
    metadata.put("partition", record.kafkaPartition());
    metadata.put("offset", record.kafkaOffset());
    if (record.timestamp() == null) {
      metadata.put("timestamp", record.timestamp());
    }
    map.put(this.metadataFieldName, metadata);
    return map;
  }
}
