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
package io.tabular.iceberg.connect.deadletter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

public class DeadLetterUtils {

  private DeadLetterUtils() {
    throw new IllegalStateException("Should not be initialialized");
  }

  public static final String KEY_BYTES = "key";
  public static final String VALUE_BYTES = "value";
  public static final String PAYLOAD_KEY = "transformed";
  public static final String ORIGINAL_BYTES_KEY = "original";
  private static final String HEADERS = "headers";
  public static final Schema HEADER_ELEMENT_SCHEMA =
      SchemaBuilder.struct()
          .field("key", Schema.STRING_SCHEMA)
          .field("value", Schema.OPTIONAL_BYTES_SCHEMA)
          .optional()
          .build();
  public static final Schema HEADER_SCHEMA =
      SchemaBuilder.array(HEADER_ELEMENT_SCHEMA).optional().build();
  public static final Schema FAILED_SCHEMA =
      SchemaBuilder.struct()
          .name("failed_message")
          .parameter("isFailed", "true")
          .field("topic", Schema.STRING_SCHEMA)
          .field("partition", Schema.INT32_SCHEMA)
          .field("offset", Schema.INT64_SCHEMA)
          .field("location", Schema.STRING_SCHEMA)
          .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
          .field("exception", Schema.OPTIONAL_STRING_SCHEMA)
          .field("stack_trace", Schema.OPTIONAL_STRING_SCHEMA)
          .field("key_bytes", Schema.OPTIONAL_BYTES_SCHEMA)
          .field("value_bytes", Schema.OPTIONAL_BYTES_SCHEMA)
          .field(HEADERS, HEADER_SCHEMA)
          .field("target_table", Schema.OPTIONAL_STRING_SCHEMA)
          .schema();

  public static String stackTrace(Throwable error) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    error.printStackTrace(pw);
    return sw.toString();
  }

  public static class Values {
    // expect byte[]
    private final Object keyBytes;
    // expect byte[]
    private final Object valueBytes;
    // expect List<Struct>
    private final Object headers;

    public Values(Object keyBytes, Object valueBytes, Object headers) {
      this.keyBytes = keyBytes;
      this.valueBytes = valueBytes;
      this.headers = headers;
    }

    public Object getKeyBytes() {
      return keyBytes;
    }

    public Object getValueBytes() {
      return valueBytes;
    }

    public Object getHeaders() {
      return headers;
    }
  }

  public static SinkRecord failedRecord(SinkRecord original, Throwable error, String location) {
    List<Struct> headers = null;
    if (!original.headers().isEmpty()) {
      headers = DeadLetterUtils.serializedHeaders(original);
    }
    Values values = new Values(original.key(), original.value(), headers);
    return failedRecord(original, values, error, location, null);
  }

  private static SinkRecord failedRecord(
      SinkRecord original, Values values, Throwable error, String location, String targetTable) {

    Struct struct = new Struct(FAILED_SCHEMA);
    struct.put("topic", original.topic());
    struct.put("partition", original.kafkaPartition());
    struct.put("offset", original.kafkaOffset());
    struct.put("timestamp", original.timestamp());
    struct.put("location", location);
    struct.put("exception", error.toString());
    String stack = stackTrace(error);
    if (!stack.isEmpty()) {
      struct.put("stack_trace", stackTrace(error));
    }
    if (values.getKeyBytes() != null) {
      struct.put("key_bytes", values.getKeyBytes());
    }
    if (values.getValueBytes() != null) {
      struct.put("value_bytes", values.getValueBytes());
    }
    if (values.getHeaders() != null) {
      struct.put(HEADERS, values.getHeaders());
    }
    if (targetTable != null) {
      struct.put("target_table", targetTable);
    }

    return original.newRecord(
        original.topic(),
        original.kafkaPartition(),
        null,
        null,
        FAILED_SCHEMA,
        struct,
        original.timestamp());
  }

  /**
   * No way to get back the original Kafka header bytes. We instead have an array with elements of
   * {"key": String, "value": bytes} for each header. This can be converted back into a Kafka
   * Connect header by the user later, and further converted into Kafka RecordHeaders to be put back
   * into a ProducerRecord to create the original headers on the Kafka record.
   *
   * @param original record where headers are still byte array values
   * @return Struct for an Array that can be put into Iceberg
   */
  public static List<Struct> serializedHeaders(SinkRecord original) {
    List<Struct> headers = Lists.newArrayList();
    for (Header header : original.headers()) {
      Struct headerStruct = new Struct(HEADER_ELEMENT_SCHEMA);
      headerStruct.put("key", header.key());
      headerStruct.put("value", header.value());
      headers.add(headerStruct);
    }
    return headers;
  }

  @SuppressWarnings("unchecked")
  public static SinkRecord mapToFailedRecord(
      String targetTable, SinkRecord record, String location, Throwable error) {
    Map<String, Object> payload = (Map<String, Object>) record.value();
    Map<String, Object> bytes = (Map<String, Object>) payload.get(ORIGINAL_BYTES_KEY);
    Object keyBytes = bytes.get(KEY_BYTES);
    Object valueBytes = bytes.get(VALUE_BYTES);
    Object headers = bytes.get(HEADERS);
    Values values = new Values(keyBytes, valueBytes, headers);
    return failedRecord(record, values, error, location, targetTable);
  }
}
