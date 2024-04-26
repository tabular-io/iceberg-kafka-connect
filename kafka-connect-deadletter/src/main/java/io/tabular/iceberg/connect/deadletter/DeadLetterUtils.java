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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

public class DeadLetterUtils {

  public static class DeadLetterException extends RuntimeException {
    private final String location;
    private final Throwable error;

    public DeadLetterException(String location, Throwable error) {
      super(error);
      this.location = location;
      this.error = error;
    }

    public String getLocation() {
      return location;
    }

    public Throwable getError() {
      return error;
    }
  }

  private DeadLetterUtils() {
    throw new IllegalStateException("Should not be initialialized");
  }

  public static final String KEY_HEADER = "t_original_key";
  public static final String VALUE_HEADER = "t_original_value";

  public static final String HEADERS_HEADER = "t_original_headers";
  public static final Schema HEADER_ELEMENT_SCHEMA =
      SchemaBuilder.struct()
          .field("key", Schema.STRING_SCHEMA)
          .field("value", Schema.OPTIONAL_BYTES_SCHEMA)
          .optional()
          .build();

  public static final Schema HEADER_SCHEMA =
      SchemaBuilder.array(HEADER_ELEMENT_SCHEMA).optional().build();

  public static String stackTrace(Throwable error) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    error.printStackTrace(pw);
    return sw.toString();
  }

  //  public static class Values {
  //    // expect byte[]
  //    private final Object keyBytes;
  //    // expect byte[]
  //    private final Object valueBytes;
  //    // expect List<Struct>
  //    private final Object headers;
  //
  //    public Values(Object keyBytes, Object valueBytes, Object headers) {
  //      this.keyBytes = keyBytes;
  //      this.valueBytes = valueBytes;
  //      this.headers = headers;
  //    }
  //
  //    public Object getKeyBytes() {
  //      return keyBytes;
  //    }
  //
  //    public Object getValueBytes() {
  //      return valueBytes;
  //    }
  //
  //    public Object getHeaders() {
  //      return headers;
  //    }
  //  }

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

  //  @SuppressWarnings("unchecked")
  //  public static SinkRecord mapToFailedRecord(
  //      String targetTable, SinkRecord record, String location, Throwable error, String
  // identifier) {
  //    Map<String, Object> payload = (Map<String, Object>) record.value();
  //    Map<String, Object> bytes = (Map<String, Object>) payload.get(ORIGINAL_BYTES_KEY);
  //    Object keyBytes = bytes.get(KEY_BYTES);
  //    Object valueBytes = bytes.get(VALUE_BYTES);
  //    Object headers = bytes.get(HEADERS);
  //    Values values = new Values(keyBytes, valueBytes, headers);
  //    return failedRecord(record, values, error, location, targetTable, identifier);
  //  }

  public static Object loadClass(String name, ClassLoader loader) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("cannot initialize empty class");
    }
    Object obj;
    try {
      Class<?> clazz = Class.forName(name, true, loader);
      obj = clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(String.format("could not initialize class %s", name), e);
    }
    return obj;
  }
}
