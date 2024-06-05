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
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.tabular.iceberg.connect.deadletter.DeadLetterUtils;
import io.tabular.iceberg.connect.deadletter.FailedRecordFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class ErrorTransformTest {
  private static final String TOPIC = "some-topic";
  private static final int PARTITION = 3;
  private static final long OFFSET = 100;
  private static final long TIMESTAMP = 1000;
  private static final String KEY_STRING = "key";
  private static final String VALUE_STRING = "value";
  private static final String JSON_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
  private static final String STRING_CONVERTER = "org.apache.kafka.connect.storage.StringConverter";

  private static final String DEAD_LETTER_TABLE_NAME = "dead_letter.table";

  private static final String DEAD_LETTER_ROUTE_FIELD_PROP = "route_field";

  private static final String DEAD_LETTER_ROUTE_FIELD = "some_field";

  private final FailedRecordFactory failedRecordFactory = getFailedRecordFactory();

  private Headers stringAsByteHeaders() {
    Headers headers = new ConnectHeaders();
    headers.add(
        "h1k", new SchemaAndValue(Schema.BYTES_SCHEMA, "h1v".getBytes(StandardCharsets.UTF_8)));
    return headers;
  }

  private SinkRecord createRecord(String key, String value, Headers headers) {
    byte[] valueBytes = (value == null) ? null : value.getBytes(StandardCharsets.UTF_8);
    byte[] keyBytes = (key == null) ? null : key.getBytes(StandardCharsets.UTF_8);

    return new SinkRecord(
        TOPIC,
        PARTITION,
        null,
        keyBytes,
        null,
        valueBytes,
        OFFSET,
        TIMESTAMP,
        TimestampType.CREATE_TIME,
        headers);
  }

  private FailedRecordFactory getFailedRecordFactory() {
    FailedRecordFactory factory =
        (FailedRecordFactory)
            DeadLetterUtils.loadClass(
                "io.tabular.iceberg.connect.deadletter.DefaultFailedRecordFactory",
                this.getClass().getClassLoader());
    factory.configure(ImmutableMap.of("table_name", DEAD_LETTER_TABLE_NAME, DEAD_LETTER_ROUTE_FIELD_PROP, DEAD_LETTER_ROUTE_FIELD));
    return factory;
  }

  @Test
  @DisplayName(
      "It should deserialize using the supplied converters into the custom SinkRecord shape with additional headers")
  public void deserialize() {
    try (ErrorTransform smt = new ErrorTransform()) {
      smt.configure(
          ImmutableMap.of(
              "value.converter",
              STRING_CONVERTER,
              "key.converter",
              STRING_CONVERTER,
              "header.converter",
              STRING_CONVERTER,
              "header.converter.converter.type",
              "header",
              "table_name",
              DEAD_LETTER_TABLE_NAME, DEAD_LETTER_ROUTE_FIELD_PROP, DEAD_LETTER_ROUTE_FIELD));

      SinkRecord result = smt.apply(createRecord(KEY_STRING, VALUE_STRING, stringAsByteHeaders()));

      assertThat(result.keySchema()).isEqualTo(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
      assertThat(result.valueSchema()).isEqualTo(SchemaBuilder.OPTIONAL_STRING_SCHEMA);
      assertThat(result.key()).isEqualTo(KEY_STRING);
      assertThat(result.value()).isEqualTo(VALUE_STRING);

      Headers headers = result.headers();
      Header originalData = headers.lastWithName(DeadLetterUtils.ORIGINAL_DATA);

      assertThat(originalData.value()).isInstanceOf(Struct.class);
      Struct originalStruct = (Struct) originalData.value();

      assertThat(originalStruct.get(DeadLetterUtils.KEY)).isEqualTo(KEY_STRING.getBytes(StandardCharsets.UTF_8));
      assertThat(originalStruct.get(DeadLetterUtils.VALUE)).isEqualTo(VALUE_STRING.getBytes(StandardCharsets.UTF_8));
      assertThat(originalStruct.get(DeadLetterUtils.HEADERS)).isInstanceOf(List.class);
      List<Struct> resultHeaders = (List<Struct>) originalStruct.get(DeadLetterUtils.HEADERS);
      assertThat(resultHeaders.size()).isEqualTo(1);
      assertThat(resultHeaders.get(0).get("key")).isEqualTo("h1k");
      assertThat(resultHeaders.get(0).get("value"))
          .isEqualTo("h1v".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  @DisplayName("It should apply the configured nested SMT transforms")
  public void smt() {
    try (ErrorTransform smt = new ErrorTransform()) {

      String transformString = "_transformed";

      smt.configure(
          ImmutableMap.of(
              "value.converter",
              STRING_CONVERTER,
              "key.converter",
              STRING_CONVERTER,
              "smts",
              "io.tabular.iceberg.connect.transforms.TestStringTransform,io.tabular.iceberg.connect.transforms.TestStringTransform",
              "smts.transform_text",
              transformString,
              "table_name",
              DEAD_LETTER_TABLE_NAME, DEAD_LETTER_ROUTE_FIELD_PROP, DEAD_LETTER_ROUTE_FIELD));

      SinkRecord result = smt.apply(createRecord(KEY_STRING, VALUE_STRING, null));

      assertThat(result.value()).isInstanceOf(String.class);
      // each transformer appends _transformed to the original value
      // we are configured with two transform appenders
      assertThat(result.value()).isEqualTo(VALUE_STRING + "_transformed_transformed");

      assertThat(result.key()).isEqualTo(KEY_STRING);
      assertThat(result.keySchema()).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
      assertThat(result.topic()).isEqualTo(TOPIC);
      assertThat(result.kafkaPartition()).isEqualTo(PARTITION);

      assertThat(result.kafkaOffset()).isEqualTo(OFFSET);
      Headers headers = result.headers();
      Header originalData = headers.lastWithName(DeadLetterUtils.ORIGINAL_DATA);

      assertThat(originalData.value()).isInstanceOf(Struct.class);
      Struct originalStruct = (Struct) originalData.value();

      assertThat(originalStruct.get(DeadLetterUtils.KEY)).isEqualTo(KEY_STRING.getBytes(StandardCharsets.UTF_8));
      assertThat(originalStruct.get(DeadLetterUtils.VALUE)).isEqualTo(VALUE_STRING.getBytes(StandardCharsets.UTF_8));
      assertThat(originalStruct.get(DeadLetterUtils.HEADERS)).isNull();

    }
  }

  @Test
  @DisplayName("Tombstone records should be returned as-is")
  public void tombstone() {
    try (ErrorTransform smt = new ErrorTransform()) {
      smt.configure(
          ImmutableMap.of(
              "value.converter",
              STRING_CONVERTER,
              "key.converter",
              STRING_CONVERTER,
              "table_name",
              DEAD_LETTER_TABLE_NAME, DEAD_LETTER_ROUTE_FIELD_PROP, DEAD_LETTER_ROUTE_FIELD));
      SinkRecord record = createRecord(null, null, null);
      assertThat(smt.apply(record)).isSameAs(record);
    }
  }

  @Test
  @DisplayName("Should return null if SMT filters out message")
  public void nullFilteredBySMT() {
    try (ErrorTransform smt = new ErrorTransform()) {

      String transformString = "_transformed";

      smt.configure(
          ImmutableMap.of(
              "value.converter",
              STRING_CONVERTER,
              "key.converter",
              STRING_CONVERTER,
              "smts",
              "io.tabular.iceberg.connect.transforms.TestStringTransform,io.tabular.iceberg.connect.transforms.TestStringTransform",
              "smts.transform_text",
              transformString,
              "smts.null",
              "true",
              "table_name",
              DEAD_LETTER_TABLE_NAME, DEAD_LETTER_ROUTE_FIELD_PROP, DEAD_LETTER_ROUTE_FIELD));

      SinkRecord result = smt.apply(createRecord(KEY_STRING, VALUE_STRING, null));

      assertThat(result).isNull();
    }
  }

  @Test
  @DisplayName("Should return failed SinkRecord if key deserializer fails")
  public void keyFailed() {
    try (ErrorTransform smt = new ErrorTransform()) {
      smt.configure(
          ImmutableMap.of(
              "value.converter",
              STRING_CONVERTER,
              "key.converter",
              JSON_CONVERTER,
              "header.converter",
              STRING_CONVERTER,
              "header.converter.converter.type",
              "header",
              "table_name",
              DEAD_LETTER_TABLE_NAME, DEAD_LETTER_ROUTE_FIELD_PROP, DEAD_LETTER_ROUTE_FIELD));

      String malformedKey = "{\"malformed_json\"\"\"{}{}{}{}**";
      SinkRecord result =
          smt.apply(createRecord(malformedKey, VALUE_STRING, stringAsByteHeaders()));
      assertThat(result.keySchema()).isNull();
      assertThat(result.valueSchema()).isEqualTo(failedRecordFactory.schema());
      assertThat(result.value()).isInstanceOf(Struct.class);
      Struct value = (Struct) result.value();
      assertThat(value.get("topic")).isEqualTo(TOPIC);
      assertThat(value.get("partition")).isEqualTo(PARTITION);
      assertThat(value.get("offset")).isEqualTo(OFFSET);
      assertThat(((String) value.get("stack_trace")).contains("JsonConverter")).isTrue();
      assertThat(((String) value.get("exception")).contains("DataException")).isTrue();
      assertThat((byte[]) value.get("key_bytes"))
          .isEqualTo(malformedKey.getBytes(StandardCharsets.UTF_8));
      assertThat((byte[]) value.get("value_bytes"))
          .isEqualTo(VALUE_STRING.getBytes(StandardCharsets.UTF_8));

      assertThat(value.get("headers")).isInstanceOf(List.class);
      List<?> resultHeaders = (List<?>) (value.get("headers"));
      assertThat(resultHeaders).isNotEmpty();
      Struct headerElement = (Struct) resultHeaders.get(0);
      assertThat(headerElement.get("key")).isEqualTo("h1k");
      assertThat((byte[]) headerElement.get("value"))
          .isEqualTo("h1v".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  @DisplayName("Should return failed SinkRecord if value deserializer fails")
  public void valueFailed() {
    try (ErrorTransform smt = new ErrorTransform()) {
      smt.configure(
          ImmutableMap.of(
              "value.converter",
              JSON_CONVERTER,
              "key.converter",
              STRING_CONVERTER,
              "header.converter",
              STRING_CONVERTER,
              "header.converter.converter.type",
              "header",
              "table_name",
              DEAD_LETTER_TABLE_NAME, DEAD_LETTER_ROUTE_FIELD_PROP, DEAD_LETTER_ROUTE_FIELD));

      String malformedValue = "{\"malformed_json\"\"\"{}{}{}{}**";
      SinkRecord result =
          smt.apply(createRecord(KEY_STRING, malformedValue, stringAsByteHeaders()));
      assertThat(result.keySchema()).isNull();
      assertThat(result.valueSchema()).isEqualTo(failedRecordFactory.schema());
      assertThat(result.value()).isInstanceOf(Struct.class);
      Struct value = (Struct) result.value();
      assertThat(value.get("topic")).isEqualTo(TOPIC);
      assertThat(value.get("partition")).isEqualTo(PARTITION);
      assertThat(value.get("offset")).isEqualTo(OFFSET);
      assertThat(((String) value.get("stack_trace")).contains("JsonConverter")).isTrue();
      assertThat(((String) value.get("exception")).contains("DataException")).isTrue();
      assertThat((byte[]) value.get("key_bytes"))
          .isEqualTo(KEY_STRING.getBytes(StandardCharsets.UTF_8));
      assertThat((byte[]) value.get("value_bytes"))
          .isEqualTo(malformedValue.getBytes(StandardCharsets.UTF_8));

      assertThat(value.get("headers")).isInstanceOf(List.class);
      List<?> resultHeaders = (ArrayList<?>) (value.get("headers"));
      assertThat(resultHeaders).isNotEmpty();
      Struct headerElement = (Struct) resultHeaders.get(0);
      assertThat(headerElement.get("key")).isEqualTo("h1k");
      assertThat((byte[]) headerElement.get("value"))
          .isEqualTo("h1v".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  @DisplayName("Should return failed SinkRecord if header deserializer fails")
  public void headerFailed() {
    String malformedValue = "{\"malformed_json\"\"\"{}{}{}{}**";
    Headers headers = new ConnectHeaders();
    headers.add(
        "h1",
        new SchemaAndValue(Schema.BYTES_SCHEMA, malformedValue.getBytes(StandardCharsets.UTF_8)));

    try (ErrorTransform smt = new ErrorTransform()) {
      smt.configure(
          ImmutableMap.of(
              "value.converter",
              STRING_CONVERTER,
              "key.converter",
              STRING_CONVERTER,
              "header.converter",
              JSON_CONVERTER,
              "header.converter.schemas.enable",
              "false",
              "header.converter.converter.type",
              "header",
              "table_name",
              DEAD_LETTER_TABLE_NAME, DEAD_LETTER_ROUTE_FIELD_PROP, DEAD_LETTER_ROUTE_FIELD));

      SinkRecord record = createRecord(KEY_STRING, VALUE_STRING, headers);
      SinkRecord result = smt.apply(record);
      assertThat(result.keySchema()).isNull();
      assertThat(result.valueSchema()).isEqualTo(failedRecordFactory.schema());
      assertThat(result.value()).isInstanceOf(Struct.class);
      Struct value = (Struct) result.value();
      assertThat(value.get("topic")).isEqualTo(TOPIC);
      assertThat(value.get("partition")).isEqualTo(PARTITION);
      assertThat(value.get("offset")).isEqualTo(OFFSET);
      assertThat(((String) value.get("stack_trace")).contains("JsonConverter")).isTrue();
      assertThat(((String) value.get("exception")).contains("DataException")).isTrue();
      assertThat((byte[]) value.get("key_bytes"))
          .isEqualTo(KEY_STRING.getBytes(StandardCharsets.UTF_8));
      assertThat((byte[]) value.get("value_bytes"))
          .isEqualTo(VALUE_STRING.getBytes(StandardCharsets.UTF_8));

      assertThat(value.get("headers")).isInstanceOf(List.class);
      List<?> resultHeaders = (ArrayList<?>) (value.get("headers"));
      assertThat(resultHeaders).isNotEmpty();
      Struct headerElement = (Struct) resultHeaders.get(0);
      assertThat(headerElement.get("key")).isEqualTo("h1");
      assertThat((byte[]) headerElement.get("value"))
          .isEqualTo(malformedValue.getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  @DisplayName("Should return failed SinkRecord if SMT fails")
  public void smtFailed() {
    try (ErrorTransform smt = new ErrorTransform()) {
      smt.configure(
          ImmutableMap.of(
              "value.converter",
              STRING_CONVERTER,
              "key.converter",
              STRING_CONVERTER,
              "header.converter",
              STRING_CONVERTER,
              "header.converter.converter.type",
              "header",
              "smts",
              "io.tabular.iceberg.connect.transforms.TestStringTransform,io.tabular.iceberg.connect.transforms.TestStringTransform",
              "smts.throw",
              "true",
              "table_name",
              DEAD_LETTER_TABLE_NAME, DEAD_LETTER_ROUTE_FIELD_PROP, DEAD_LETTER_ROUTE_FIELD));

      SinkRecord record = createRecord(KEY_STRING, VALUE_STRING, stringAsByteHeaders());
      SinkRecord result = smt.apply(record);
      assertThat(result.keySchema()).isNull();
      assertThat(result.valueSchema()).isEqualTo(failedRecordFactory.schema());
      assertThat(result.value()).isInstanceOf(Struct.class);
      Struct value = (Struct) result.value();
      assertThat(value.get("topic")).isEqualTo(TOPIC);
      assertThat(value.get("partition")).isEqualTo(PARTITION);
      assertThat(value.get("offset")).isEqualTo(OFFSET);
      assertThat(((String) value.get("stack_trace")).contains("smt failure")).isTrue();
      assertThat(((String) value.get("exception")).contains("smt failure")).isTrue();
      assertThat((byte[]) value.get("key_bytes"))
          .isEqualTo(KEY_STRING.getBytes(StandardCharsets.UTF_8));
      assertThat((byte[]) value.get("value_bytes"))
          .isEqualTo(VALUE_STRING.getBytes(StandardCharsets.UTF_8));

      assertThat(value.get("headers")).isInstanceOf(List.class);
      List<?> resultHeaders = (ArrayList<?>) (value.get("headers"));
      assertThat(resultHeaders).isNotEmpty();
      Struct headerElement = (Struct) resultHeaders.get(0);
      assertThat(headerElement.get("key")).isEqualTo("h1k");
      assertThat((byte[]) headerElement.get("value"))
          .isEqualTo("h1v".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  @DisplayName("Should throw if runtime classes cannot be dynamically loaded or configured")
  public void shouldThrowClassLoader() {
    try (ErrorTransform smt = new ErrorTransform()) {
      assertThrows(
          ErrorTransform.TransformInitializationException.class,
          () -> smt.configure(ImmutableMap.of("value.converter", "")));
    }

    try (ErrorTransform smt = new ErrorTransform()) {
      assertThrows(
          ErrorTransform.TransformInitializationException.class,
          () -> smt.configure(ImmutableMap.of("value.converter", "some_bogus_class")));
    }

    try (ErrorTransform smt = new ErrorTransform()) {
      assertThrows(
          ErrorTransform.TransformInitializationException.class,
          () ->
              smt.configure(
                  ImmutableMap.of(
                      "value.converter",
                      STRING_CONVERTER,
                      "key.converter",
                      STRING_CONVERTER,
                      "header.converter",
                      "some_bogus_class",
                      "header.converter.converter.type",
                      "header",
                      "smts",
                      "io.tabular.iceberg.connect.transforms.TestStringTransform,io.tabular.iceberg.connect.transforms.TestStringTransform",
                      "smts.throw",
                      "true")));
    }

    try (ErrorTransform smt = new ErrorTransform()) {
      assertThrows(
          ErrorTransform.TransformInitializationException.class,
          () ->
              smt.configure(
                  ImmutableMap.of(
                      "value.converter",
                      STRING_CONVERTER,
                      "key.converter",
                      STRING_CONVERTER,
                      "header.converter",
                      STRING_CONVERTER,
                      "header.converter.converter.type",
                      "header",
                      "smts",
                      "some_bogus_smt",
                      "smts.throw",
                      "true")));
    }
    try (ErrorTransform smt = new ErrorTransform()) {
      // throws because the header converter fails when .configure is called
      assertThrows(
          ErrorTransform.TransformInitializationException.class,
          () ->
              smt.configure(
                  ImmutableMap.of(
                      "value.converter",
                      STRING_CONVERTER,
                      "key.converter",
                      STRING_CONVERTER,
                      "header.converter",
                      STRING_CONVERTER)));
    }
  }

  @Test
  @DisplayName("PropsParser should return an empty map for keys that do not match target")
  public void propsParserEmptyMap() {
    Map<String, String> input = ImmutableMap.of("some.key", "some.value");
    assertThat(ErrorTransform.PropsParser.apply(input, "missing")).isEmpty();
  }

  @Test
  @DisplayName(
      "PropsParser should return Map with keys matching target stripped of the target prefix")
  public void keysMatching() {
    Map<String, String> input =
        ImmutableMap.of("value.converter", "some.class.here", "value.converter.prop", "some.prop");
    assertThat(ErrorTransform.PropsParser.apply(input, "value.converter"))
        .isEqualTo(ImmutableMap.of("prop", "some.prop"));
  }
}
