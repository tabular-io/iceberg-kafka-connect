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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class KafkaMetadataAppenderTest {

  private static SchemaBuilder baseBuilder() {
    return SchemaBuilder.struct()
        .field("topic", Schema.STRING_SCHEMA)
        .field("partition", Schema.INT32_SCHEMA)
        .field("offset", Schema.OPTIONAL_INT64_SCHEMA)
        .field("record_timestamp", Schema.OPTIONAL_INT64_SCHEMA);
  }

  private static final long TIMESTAMP = 100L;
  private static final String TOPIC = "topic";
  private static final int PARTITION = 0;
  private static final long OFFSET = 1000;
  private static final Schema SCHEMA = baseBuilder().build();

  private static final Schema SCHEMA_WITH_EXT_FIELD =
      baseBuilder().field("external", Schema.STRING_SCHEMA).build();

  private static final Schema RECORD_SCHEMA =
      SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();

  private static SinkRecord genSinkRecord(Boolean includeTimestamp) {
    Struct value = new Struct(RECORD_SCHEMA);
    value.put("id", "id");
    if (includeTimestamp) {
      return new SinkRecord(
          TOPIC,
          PARTITION,
          null,
          null,
          RECORD_SCHEMA,
          value,
          OFFSET,
          TIMESTAMP,
          TimestampType.CREATE_TIME);
    } else {
      return new SinkRecord(TOPIC, PARTITION, null, null, RECORD_SCHEMA, value, OFFSET);
    }
  }

  private static SinkRecord genSinkRecordAsMap(Boolean includeTimestamp) {
    Map<String, Object> value = ImmutableMap.of("id", "id");
    if (includeTimestamp) {
      return new SinkRecord(
          TOPIC, PARTITION, null, null, null, value, OFFSET, TIMESTAMP, TimestampType.CREATE_TIME);
    } else {
      return new SinkRecord(TOPIC, PARTITION, null, null, null, value, OFFSET);
    }
  }

  @Test
  @DisplayName("appendSchema should append the configured schema")
  public void appendSchema() {
    RecordAppender<SinkRecord> appender =
        KafkaMetadataAppender.from(
            ImmutableMap.of(
                KafkaMetadataAppender.INCLUDE_KAFKA_METADATA, true,
                KafkaMetadataAppender.KEY_METADATA_IS_NESTED, true));
    RecordAppender<SinkRecord> appenderWithExternalField =
        KafkaMetadataAppender.from(
            ImmutableMap.of(
                KafkaMetadataAppender.INCLUDE_KAFKA_METADATA, true,
                KafkaMetadataAppender.KEY_METADATA_IS_NESTED, true,
                KafkaMetadataAppender.EXTERNAL_KAFKA_METADATA, "external,value"));

    Schema appendResult = appender.addToSchema(SchemaBuilder.struct());
    Schema appendExternalResult = appenderWithExternalField.addToSchema(SchemaBuilder.struct());

    assertThat(appendResult.field(KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME).schema())
        .isEqualTo(SCHEMA);
    assertThat(
            appendExternalResult.field(KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME).schema())
        .isEqualTo(SCHEMA_WITH_EXT_FIELD);
  }

  @Test
  @DisplayName("appendToStruct should append record metadata under the configured key")
  public void appendToStruct() {
    RecordAppender<SinkRecord> appenderWithExternalField =
        KafkaMetadataAppender.from(
            ImmutableMap.of(
                KafkaMetadataAppender.INCLUDE_KAFKA_METADATA, true,
                KafkaMetadataAppender.KEY_METADATA_IS_NESTED, true,
                KafkaMetadataAppender.EXTERNAL_KAFKA_METADATA, "external,value"));

    SinkRecord recordWithTimestamp = genSinkRecord(true);
    SinkRecord recordWithoutTimestamp = genSinkRecord(false);

    Schema schema =
        appenderWithExternalField.addToSchema(
            SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA));

    Struct resultParent =
        appenderWithExternalField.addToStruct(recordWithTimestamp, new Struct(schema));
    Struct result = (Struct) resultParent.get(KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME);
    Struct resultWithoutTime =
        (Struct)
            appenderWithExternalField
                .addToStruct(recordWithoutTimestamp, new Struct(schema))
                .get(KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME);

    assertThat(resultParent.schema().field("id").schema().type()).isEqualTo(Schema.Type.STRING);
    assertThat(result.get("external")).isEqualTo("value");
    assertThat(resultWithoutTime.get("external")).isEqualTo("value");
    assertThat(result.get("topic")).isEqualTo(TOPIC);
    assertThat(resultWithoutTime.get("topic")).isEqualTo(TOPIC);
    assertThat(result.get("partition")).isEqualTo(PARTITION);
    assertThat(resultWithoutTime.get("partition")).isEqualTo(PARTITION);
    assertThat(result.get("offset")).isEqualTo(OFFSET);
    assertThat(resultWithoutTime.get("offset")).isEqualTo(OFFSET);
    assertThat(result.get("record_timestamp")).isEqualTo(TIMESTAMP);
    assertThat(resultWithoutTime.get("record_timestamp")).isNull();
  }

  @Test
  @DisplayName("appendToStruct should append flattened record metadata under the configured key")
  public void appendToStructFlat() {
    RecordAppender<SinkRecord> appenderWithExternalField =
        KafkaMetadataAppender.from(
            ImmutableMap.of(
                KafkaMetadataAppender.INCLUDE_KAFKA_METADATA,
                true,
                KafkaMetadataAppender.EXTERNAL_KAFKA_METADATA,
                "external,value"));

    SinkRecord recordWithTimestamp = genSinkRecord(true);
    SinkRecord recordWithoutTimestamp = genSinkRecord(false);

    Schema schema =
        appenderWithExternalField.addToSchema(
            SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA));

    Struct result = appenderWithExternalField.addToStruct(recordWithTimestamp, new Struct(schema));
    Struct resultWithoutTime =
        appenderWithExternalField.addToStruct(recordWithoutTimestamp, new Struct(schema));

    assertThat(result.get("_kafka_metadata_external")).isEqualTo("value");
    assertThat(resultWithoutTime.get("_kafka_metadata_external")).isEqualTo("value");
    assertThat(result.get("_kafka_metadata_topic")).isEqualTo(TOPIC);
    assertThat(resultWithoutTime.get("_kafka_metadata_topic")).isEqualTo(TOPIC);
    assertThat(result.get("_kafka_metadata_partition")).isEqualTo(PARTITION);
    assertThat(resultWithoutTime.get("_kafka_metadata_partition")).isEqualTo(PARTITION);
    assertThat(result.get("_kafka_metadata_offset")).isEqualTo(OFFSET);
    assertThat(resultWithoutTime.get("_kafka_metadata_offset")).isEqualTo(OFFSET);
    assertThat(result.get("_kafka_metadata_record_timestamp")).isEqualTo(TIMESTAMP);
    assertThat(resultWithoutTime.get("_kafka_metadata_record_timestamp")).isNull();
  }

  @Test
  @DisplayName("appendToMap should append record metadata under the configured key")
  public void appendToMap() {
    RecordAppender<SinkRecord> appenderWithExternalField =
        KafkaMetadataAppender.from(
            ImmutableMap.of(
                KafkaMetadataAppender.INCLUDE_KAFKA_METADATA, true,
                KafkaMetadataAppender.KEY_METADATA_IS_NESTED, true,
                KafkaMetadataAppender.EXTERNAL_KAFKA_METADATA, "external,value"));

    SinkRecord recordWithTimestamp = genSinkRecordAsMap(true);
    SinkRecord recordWithoutTimestamp = genSinkRecordAsMap(false);

    Map<String, Object> record1 = Maps.newHashMap();
    record1.put("id", "id");
    Map<String, Object> record2 = Maps.newHashMap();
    record2.put("id", "id");
    Map<String, Object> expected =
        ImmutableMap.of(
            "id",
            "id",
            KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME,
            ImmutableMap.of(
                "topic", TOPIC,
                "partition", PARTITION,
                "offset", OFFSET,
                "record_timestamp", TIMESTAMP,
                "external", "value"));

    Map<String, Object> expectedWithoutTime =
        ImmutableMap.of(
            "id",
            "id",
            KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME,
            ImmutableMap.of(
                "topic", TOPIC,
                "partition", PARTITION,
                "offset", OFFSET,
                "external", "value"));

    Map<String, Object> result = appenderWithExternalField.addToMap(recordWithTimestamp, record1);
    Map<String, Object> resultWithoutTime =
        appenderWithExternalField.addToMap(recordWithoutTimestamp, record2);

    assertThat(result).isEqualTo(expected);
    assertThat(resultWithoutTime).isEqualTo(expectedWithoutTime);
  }

  @Test
  @DisplayName("appendToMap should append flattened record metadata under the configured key")
  public void appendToMapFlat() {
    RecordAppender<SinkRecord> appenderWithExternalField =
        KafkaMetadataAppender.from(
            ImmutableMap.of(
                KafkaMetadataAppender.INCLUDE_KAFKA_METADATA,
                true,
                KafkaMetadataAppender.EXTERNAL_KAFKA_METADATA,
                "external,value"));

    SinkRecord recordWithTimestamp = genSinkRecordAsMap(true);
    SinkRecord recordWithoutTimestamp = genSinkRecordAsMap(false);

    Map<String, Object> record1 = Maps.newHashMap();
    record1.put("id", "id");

    Map<String, Object> record2 = Maps.newHashMap();
    record2.put("id", "id");
    Map<String, Object> expected =
        ImmutableMap.of(
            "id",
            "id",
            "_kafka_metadata_topic",
            TOPIC,
            "_kafka_metadata_partition",
            PARTITION,
            "_kafka_metadata_offset",
            OFFSET,
            "_kafka_metadata_record_timestamp",
            TIMESTAMP,
            "_kafka_metadata_external",
            "value");

    Map<String, Object> expectedWithoutTime =
        ImmutableMap.of(
            "id",
            "id",
            "_kafka_metadata_topic",
            TOPIC,
            "_kafka_metadata_partition",
            PARTITION,
            "_kafka_metadata_offset",
            OFFSET,
            "_kafka_metadata_external",
            "value");

    Map<String, Object> result = appenderWithExternalField.addToMap(recordWithTimestamp, record1);
    Map<String, Object> resultWithoutTime =
        appenderWithExternalField.addToMap(recordWithoutTimestamp, record2);

    assertThat(result).isEqualTo(expected);
    assertThat(resultWithoutTime).isEqualTo(expectedWithoutTime);
  }
}
