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

import io.tabular.iceberg.connect.transforms.util.KafkaMetadataAppender.EmptyExternalData;
import io.tabular.iceberg.connect.transforms.util.KafkaMetadataAppender.ExternalStringKafkaData;
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
        .field("offset", Schema.INT64_SCHEMA)
        .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA);
  }

  private static final long TIMESTAMP = 100L;
  private static final String TOPIC = "topic";
  private static final int PARTITION = 0;
  private static final long OFFSET = 1000;
  private static final Schema SCHEMA = baseBuilder().build();

  private static final Schema SCHEMA_WITH_EXT_FIELD =
      baseBuilder().field("external", Schema.STRING_SCHEMA);

  private static final ExternalStringKafkaData EXT_FIELD =
      new ExternalStringKafkaData("external", "value");

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
    KafkaMetadataAppender appender =
        new KafkaMetadataAppender(
            new EmptyExternalData(), SCHEMA, KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME);
    KafkaMetadataAppender appenderWithExternalField =
        new KafkaMetadataAppender(
            EXT_FIELD, SCHEMA_WITH_EXT_FIELD, KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME);

    Schema appendResult = appender.appendSchema(SchemaBuilder.struct());
    Schema appendExternalResult = appenderWithExternalField.appendSchema(SchemaBuilder.struct());

    assertThat(appendResult.field(KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME).schema())
        .isEqualTo(SCHEMA);
    assertThat(
            appendExternalResult.field(KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME).schema())
        .isEqualTo(SCHEMA_WITH_EXT_FIELD);
  }

  @Test
  @DisplayName("appendToStruct should append record metadata under the configured key")
  public void appendToStruct() {
    KafkaMetadataAppender appenderWithExternalField =
        new KafkaMetadataAppender(
            EXT_FIELD, SCHEMA_WITH_EXT_FIELD, KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME);

    SinkRecord recordWithTimestamp = genSinkRecord(true);
    SinkRecord recordWithoutTimestamp = genSinkRecord(false);

    Schema schema =
        appenderWithExternalField.appendSchema(
            SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA));

    Struct resultParent =
        appenderWithExternalField.appendToStruct(recordWithTimestamp, new Struct(schema));
    Struct result = (Struct) resultParent.get(KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME);
    Struct resultWithoutTime =
        (Struct)
            appenderWithExternalField
                .appendToStruct(recordWithoutTimestamp, new Struct(schema))
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
    assertThat(result.get("timestamp")).isEqualTo(TIMESTAMP);
    assertThat(resultWithoutTime.get("timestamp")).isNull();
  }

  @Test
  @DisplayName("appendToMap should append record metadata under the configured key")
  public void appendToMap() {
    KafkaMetadataAppender appenderWithExternalField =
        new KafkaMetadataAppender(
            EXT_FIELD, SCHEMA_WITH_EXT_FIELD, KafkaMetadataAppender.DEFAULT_METADATA_FIELD_NAME);

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
                "timestamp", TIMESTAMP,
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

    Map<String, Object> result =
        appenderWithExternalField.appendToMap(recordWithTimestamp, record1);
    Map<String, Object> resultWithoutTime =
        appenderWithExternalField.appendToMap(recordWithoutTimestamp, record2);

    assertThat(result).isEqualTo(expected);
    assertThat(resultWithoutTime).isEqualTo(expectedWithoutTime);
  }
}
