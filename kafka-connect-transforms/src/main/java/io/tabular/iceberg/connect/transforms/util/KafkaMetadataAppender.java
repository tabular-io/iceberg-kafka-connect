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
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class KafkaMetadataAppender {

  public static final String INCLUDE_KAFKA_METADATA = "cdc.kafka.include_metadata";
  public static final String EXTERNAL_KAFKA_METADATA = "cdc.kafka.external_field";
  public static final String KEY_METADATA_FIELD_NAME = "cdc.kafka.metadata_field";
  public static final String KEY_METADATA_IS_NESTED = "cdc.kafka.metadata_is_nested";
  public static final String DEFAULT_METADATA_FIELD_NAME = "_kafka_metadata";

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

  public static <R extends ConnectRecord<R>> RecordAppender<R> from(Map<String, ?> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    if (config.getBoolean(KafkaMetadataAppender.INCLUDE_KAFKA_METADATA)) {
      return from(config);
    } else {
      return new NoOpRecordAppender<R>();
    }
  }

  private static <R extends ConnectRecord<R>> RecordAppender<R> from(SimpleConfig config) {
    RecordAppender<R> externalFieldAppender =
        getExternalFieldAppender(config.getString(EXTERNAL_KAFKA_METADATA));
    String metadataFieldName = config.getString(KEY_METADATA_FIELD_NAME);
    Boolean nestedMetadata = config.getBoolean(KEY_METADATA_IS_NESTED);

    String topicFieldName;
    String partitionFieldName;
    String offsetFieldName;
    String timestampFieldName;

    if (nestedMetadata) {
      topicFieldName = "topic";
      partitionFieldName = "partition";
      offsetFieldName = "offset";
      timestampFieldName = "record_timestamp";

      SchemaBuilder nestedSchemaBuilder = SchemaBuilder.struct();
      nestedSchemaBuilder
          .field(topicFieldName, Schema.STRING_SCHEMA)
          .field(partitionFieldName, Schema.INT32_SCHEMA)
          .field(offsetFieldName, Schema.OPTIONAL_INT64_SCHEMA)
          .field(timestampFieldName, Schema.OPTIONAL_INT64_SCHEMA);
      externalFieldAppender.addToSchema(nestedSchemaBuilder);

      Schema nestedSchema = nestedSchemaBuilder.build();

      return new RecordAppender<R>() {
        @Override
        public SchemaBuilder addToSchema(SchemaBuilder builder) {
          return builder.field(metadataFieldName, nestedSchema);
        }

        @Override
        public Struct addToStruct(R record, Struct struct) {
          Struct nested = new Struct(nestedSchema);
          nested.put(topicFieldName, record.topic());
          nested.put(partitionFieldName, record.kafkaPartition());
          if (record instanceof SinkRecord) {
            SinkRecord sinkRecord = (SinkRecord) record;
            nested.put(offsetFieldName, sinkRecord.kafkaOffset());
          }
          if (record.timestamp() != null) {
            nested.put(timestampFieldName, record.timestamp());
          }
          externalFieldAppender.addToStruct(record, nested);
          struct.put(metadataFieldName, nested);
          return struct;
        }

        @Override
        public Map<String, Object> addToMap(R record, Map<String, Object> map) {
          Map<String, Object> nested = Maps.newHashMap();
          nested.put("topic", record.topic());
          nested.put("partition", record.kafkaPartition());
          if (record instanceof SinkRecord) {
            SinkRecord sinkRecord = (SinkRecord) record;
            nested.put("offset", sinkRecord.kafkaOffset());
          }
          if (record.timestamp() != null) {
            nested.put("record_timestamp", record.timestamp());
          }
          externalFieldAppender.addToMap(record, nested);
          map.put(metadataFieldName, nested);
          return map;
        }
      };

    } else {
      Function<String, String> namer = name -> String.format("%s_%s", metadataFieldName, name);
      topicFieldName = namer.apply("topic");
      partitionFieldName = namer.apply("partition");
      offsetFieldName = namer.apply("offset");
      timestampFieldName = namer.apply("record_timestamp");

      return new RecordAppender<R>() {
        @Override
        public SchemaBuilder addToSchema(SchemaBuilder builder) {
          builder
              .field(topicFieldName, Schema.STRING_SCHEMA)
              .field(partitionFieldName, Schema.INT32_SCHEMA)
              .field(offsetFieldName, Schema.OPTIONAL_INT64_SCHEMA)
              .field(timestampFieldName, Schema.OPTIONAL_INT64_SCHEMA);
          return externalFieldAppender.addToSchema(builder);
        }

        @Override
        public Struct addToStruct(R record, Struct struct) {
          struct.put(topicFieldName, record.topic());
          struct.put(partitionFieldName, record.kafkaPartition());
          if (record instanceof SinkRecord) {
            SinkRecord sinkRecord = (SinkRecord) record;
            struct.put(offsetFieldName, sinkRecord.kafkaOffset());
          }
          if (record.timestamp() != null) {
            struct.put(timestampFieldName, record.timestamp());
          }
          externalFieldAppender.addToStruct(record, struct);
          return struct;
        }

        @Override
        public Map<String, Object> addToMap(R record, Map<String, Object> map) {
          map.put("topic", record.topic());
          map.put("partition", record.kafkaPartition());
          if (record instanceof SinkRecord) {
            SinkRecord sinkRecord = (SinkRecord) record;
            map.put("offset", sinkRecord.kafkaOffset());
          }
          if (record.timestamp() != null) {
            map.put("record_timestamp", record.timestamp());
          }
          externalFieldAppender.addToMap(record, map);
          return map;
        }
      };
    }
  }

  public static class NoOpRecordAppender<R extends ConnectRecord<R>> implements RecordAppender<R> {

    @Override
    public SchemaBuilder addToSchema(SchemaBuilder builder) {
      return builder;
    }

    @Override
    public Struct addToStruct(R record, Struct struct) {
      return struct;
    }

    @Override
    public Map<String, Object> addToMap(R record, Map<String, Object> map) {
      return map;
    }
  }

  private static <R extends ConnectRecord<R>> RecordAppender<R> getExternalFieldAppender(
      String field) {
    if (field.equals("none")) {
      return new NoOpRecordAppender<>();
    }
    String[] parts = field.split(",");
    if (parts.length != 2) {
      throw new ConfigException(
          String.format("Could not parse %s for %s", field, EXTERNAL_KAFKA_METADATA));
    }
    String fieldName = parts[0];
    String fieldValue = parts[1];
    return new RecordAppender<R>() {

      @Override
      public SchemaBuilder addToSchema(SchemaBuilder builder) {
        return builder.field(fieldName, Schema.STRING_SCHEMA);
      }

      @Override
      public Struct addToStruct(R record, Struct struct) {
        return struct.put(fieldName, fieldValue);
      }

      @Override
      public Map<String, Object> addToMap(R record, Map<String, Object> map) {
        map.put(fieldName, fieldValue);
        return map;
      }
    };
  }
}
