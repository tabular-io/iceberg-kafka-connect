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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class JsonToMapTransformTest extends FileLoads {

  private String loadJson() {
    try {
      return getFile("jsonmap.json");
    } catch (Exception e) {
      throw new RuntimeException("failed to load jsonmap.json in test", e);
    }
  }

  private final String topic = "topic";
  private final int partition = 0;
  private final Long offset = 100L;
  private final Long timestamp = 1000L;
  private final String keyValue = "key_value:";
  private final Schema keySchema = SchemaBuilder.STRING_SCHEMA;

  @Test
  @DisplayName("should return null records as-is")
  public void nullRecords() {
    try (JsonToMapTransform<SinkRecord> smt = new JsonToMapTransform<>()) {
      smt.configure(ImmutableMap.of(JsonToMapTransform.JSON_LEVEL, "0"));
      SinkRecord record =
          new SinkRecord(
              topic,
              partition,
              null,
              null,
              null,
              null,
              offset,
              timestamp,
              TimestampType.CREATE_TIME);
      SinkRecord result = smt.apply(record);
      assertThat(result).isSameAs(record);
    }
  }

  @Test
  @DisplayName("should throw exception if the value is not a json object")
  public void shouldThrowExceptionNonJsonObjects() throws Exception {
    try (JsonToMapTransform<SinkRecord> smt = new JsonToMapTransform<>()) {
      SinkRecord record =
          new SinkRecord(
              topic,
              partition,
              keySchema,
              keyValue,
              null,
              "not_a_json_object",
              offset,
              timestamp,
              TimestampType.CREATE_TIME);
      assertThrows(RuntimeException.class, () -> smt.apply(record));
    }
  }

  @Test
  @DisplayName("should throw exception if not valid json")
  public void shouldThrowExceptionInvalidJson() {
    try (JsonToMapTransform<SinkRecord> smt = new JsonToMapTransform<>()) {
      SinkRecord record =
          new SinkRecord(
              topic,
              partition,
              keySchema,
              keyValue,
              null,
              "{\"key\": 1,\"\"\"***",
              offset,
              timestamp,
              TimestampType.CREATE_TIME);
      assertThrows(RuntimeException.class, () -> smt.apply(record));
    }
  }

  @Test
  @DisplayName(
      "should contain a single value of Map<String,String> if configured to convert root node")
  public void singleValueOnRootNode() {
    try (JsonToMapTransform<SinkRecord> smt = new JsonToMapTransform<>()) {
      smt.configure(ImmutableMap.of(JsonToMapTransform.JSON_LEVEL, "0"));
      SinkRecord record =
          new SinkRecord(
              topic,
              partition,
              keySchema,
              keyValue,
              null,
              "{\"key\":1,\"a\":\"a\"}",
              offset,
              timestamp,
              TimestampType.CREATE_TIME);
      SinkRecord result = smt.apply(record);
      Schema expectedSchema = SchemaBuilder.struct().field("payload", Schema.STRING_SCHEMA).build();
      Struct expecedStruct = new Struct(expectedSchema).put("payload", "{\"key\":1,\"a\":\"a\"}");
      assertInstanceOf(Struct.class, result.value());
      Struct resultStruct = (Struct) result.value();
      assertThat(resultStruct.get("payload")).isEqualTo("{\"key\":1,\"a\":\"a\"}");
      assertThat(result.valueSchema()).isEqualTo(expectedSchema);
    }
  }

  @Test
  @DisplayName("should contain a struct on the value if configured to convert after root node")
  public void structOnRootNode() {
    try (JsonToMapTransform<SinkRecord> smt = new JsonToMapTransform<>()) {
      SinkRecord record =
          new SinkRecord(
              topic,
              partition,
              keySchema,
              keyValue,
              null,
              loadJson(),
              offset,
              timestamp,
              TimestampType.CREATE_TIME);
      SinkRecord result = smt.apply(record);
      assertInstanceOf(Struct.class, result.value());
      Struct resultStruct = (Struct) result.value();
      assertThat(resultStruct.schema().fields().size()).isEqualTo(16);
      assertThat(resultStruct.get("string")).isEqualTo("string");
    }
  }
}
