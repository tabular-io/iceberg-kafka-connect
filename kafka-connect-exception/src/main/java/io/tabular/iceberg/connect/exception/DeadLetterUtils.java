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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
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

  private DeadLetterUtils() {}

  public static final String ORIGINAL_DATA = "iceberg.error.transform.original";
  public static final String KEY = "KEY";
  public static final String VALUE = "VALUE";
  public static final String HEADERS = "HEADERS";
  public static final Schema HEADER_ELEMENT_SCHEMA =
      SchemaBuilder.struct()
          .field("key", Schema.STRING_SCHEMA)
          .field("value", Schema.OPTIONAL_BYTES_SCHEMA)
          .optional()
          .build();
  public static final Schema HEADER_SCHEMA =
          SchemaBuilder.array(HEADER_ELEMENT_SCHEMA).optional().build();
  public static final Schema HEADER_STRUCT_SCHEMA =
          SchemaBuilder.struct()
                  .field(KEY, Schema.OPTIONAL_BYTES_SCHEMA)
                  .field(VALUE, Schema.OPTIONAL_BYTES_SCHEMA)
                  .field(HEADERS, HEADER_SCHEMA)
                  .optional()
                  .build();

  public static String stackTrace(Throwable error) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    error.printStackTrace(pw);
    return sw.toString();
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
    @SuppressWarnings("RegexpSingleline")
    List<Struct> headers = new ArrayList<>();
    for (Header header : original.headers()) {
      Struct headerStruct = new Struct(HEADER_ELEMENT_SCHEMA);
      headerStruct.put("key", header.key());
      headerStruct.put("value", header.value());
      headers.add(headerStruct);
    }
    return headers;
  }

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
