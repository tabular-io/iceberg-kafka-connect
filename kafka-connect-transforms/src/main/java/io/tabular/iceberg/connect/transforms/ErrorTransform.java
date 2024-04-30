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

import io.tabular.iceberg.connect.deadletter.DeadLetterUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * Wraps key, value, header converters and SMTs in order to catch exceptions. Failed records are
 * converted into a standard struct and sent to the connector to be put into Iceberg
 *
 * <p>MUST ONLY BE USED with `value.converter`, `key.converter`, and `header.converter` set to
 * "org.apache.kafka.connect.converters.ByteArrayConverter" which can not be validated from within
 * this SMT
 *
 * <p>Actual value converter, key converter, and header converter are configured on the
 * `transforms.xxx` key where xxx is the name of this transform. See example for how properties are
 * passed and namespaced
 *
 * <p>"transforms": "tab", "transforms.tab.type":
 * <ul>
 *  <li>"io.tabular.iceberg.connect.transform.managed.ManagedTransform" </li>
 *  <li>"transforms.tab.value.converter": "org.apache.kafka.connect.storage.StringConverter"</li>
 *  <li>"transforms.tab.value.converter.some_property: "...", "transforms.tab.key.converter":</li>
 *  <li>"org.apache.kafka.connect.storage.StringConverter", "transforms.tab.key.converter.some_property": "..."</li>
 *  <li>"transforms.tab.smts" : "some.java.class,some.other.java.class"</li>
 *  <li>"transforms.tab.smts.prop1" : "some_property_for_the_smts"</li>
 * </ul>
 * <p>This should not be used with any other SMT. All SMTs should be added to "transforms.tab.smts".
 *
 * <p>It returns a special Map of String -> Object "original" : Map of String -> Object containing
 * the key,value, and header bytes of the original message "transformed" : [null, Struct, Map, etc.]
 * of whatever the deserialized record is (after transformation if SMTs are configured)
 *
 * <p>The original payload can be used in the Iceberg Connector if the record cannot be transformed
 * to an Iceberg record so that the original kafka message can be stored in Iceberg at that point.
 *
 * <p>If any of the key, value, header deserializers or SMTs throw an exception a failed record is
 * constructed that contains kafka metadata, exception/location information, and the original
 * key/value/header bytes.
 */
public class ErrorTransform implements Transformation<SinkRecord> {

  public static class TransformInitializationException extends RuntimeException {
    TransformInitializationException(String errorMessage) {
      super(errorMessage);
    }

    TransformInitializationException(String errorMessage, Throwable err) {
      super(errorMessage, err);
    }
  }

  public static class PropsParser {
    static Map<String, ?> apply(Map<String, ?> props, String target) {
      return props.entrySet().stream()
          .filter(
              entry ->
                  (!Objects.equals(entry.getKey(), target)) && (entry.getKey().startsWith(target)))
          .collect(
              Collectors.toMap(
                  entry -> entry.getKey().replaceFirst("^" + target + ".", ""),
                  Map.Entry::getValue));
    }
  }

  private static class DeserializedRecord {
    private final SinkRecord record;
    private final boolean failed;

    DeserializedRecord(SinkRecord record, boolean failed) {
      this.record = record;
      this.failed = failed;
    }

    public SinkRecord getRecord() {
      return record;
    }

    public boolean isFailed() {
      return failed;
    }
  }

  private static final String KEY_CONVERTER = "key.converter";
  private static final String VALUE_CONVERTER = "value.converter";
  private static final String HEADER_CONVERTER = "header.converter";
  private static final String CONVERTER_ERROR_HANDLER = "error.converter";
  private static final String SMT_ERROR_HANDLER = "error.smt";
  private static final String TRANSFORMATIONS = "smts";
  private static final String KEY_FAILURE = "KEY_CONVERTER";
  private static final String VALUE_FAILURE = "VALUE_CONVERTER";
  private static final String HEADER_FAILURE = "HEADER_CONVERTER";
  private static final String SMT_FAILURE = "SMT_FAILURE";
  private static final Schema OPTIONAL_BYTES_SCHEMA = SchemaBuilder.OPTIONAL_BYTES_SCHEMA;

  private TransformExceptionHandler converterErrorHandler;
  private TransformExceptionHandler smtErrorHandler;
  private List<Transformation<SinkRecord>> smts;
  private Function<SinkRecord, SchemaAndValue> keyConverter;
  private Function<SinkRecord, SchemaAndValue> valueConverter;
  private Function<SinkRecord, Headers> headerConverterFn;

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              KEY_CONVERTER,
              ConfigDef.Type.STRING,
              "org.apache.kafka.connect.converters.ByteArrayConverter",
              ConfigDef.Importance.MEDIUM,
              "key.converter")
          .define(
              VALUE_CONVERTER,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.MEDIUM,
              "value.converter")
          .define(
              HEADER_CONVERTER,
              ConfigDef.Type.STRING,
              "org.apache.kafka.connect.converters.ByteArrayConverter",
              ConfigDef.Importance.MEDIUM,
              "header.converter")
          .define(TRANSFORMATIONS, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "smts")
          .define(
              CONVERTER_ERROR_HANDLER,
              ConfigDef.Type.STRING,
              "io.tabular.iceberg.connect.transforms.DefaultExceptionHandler",
              ConfigDef.Importance.MEDIUM,
              "Error handling class for converter errors")
          .define(
              SMT_ERROR_HANDLER,
              ConfigDef.Type.STRING,
              "io.tabular.iceberg.connect.transforms.DefaultExceptionHandler",
              ConfigDef.Importance.MEDIUM,
              "Error handling class for SMT errors");

  @Override
  public SinkRecord apply(SinkRecord record) {
    // tombstones returned as-is
    if (record == null || record.value() == null) {
      return record;
    }

    DeserializedRecord deserialized = deserialize(record);
    if (deserialized.isFailed()) {
      return deserialized.getRecord();
    }

    SinkRecord transformedRecord = deserialized.getRecord();

    for (Transformation<SinkRecord> smt : smts) {
      try {
        transformedRecord = smt.apply(transformedRecord);
        if (transformedRecord == null) {
          break;
        }
      } catch (Exception e) {
        return smtErrorHandler.handle(record, e, SMT_FAILURE);
      }
    }
    // SMT could filter out messages
    if (transformedRecord == null) {
      return null;
    }

    return newRecord(record, transformedRecord);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}

  /*
  Kafka Connect filters the properties it passes to the SMT to
  only the keys under the `transform.xxx` name.
  */
  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    ClassLoader loader = this.getClass().getClassLoader();

    if (Objects.equals(
        config.getString(KEY_CONVERTER),
        "org.apache.kafka.connect.converters.ByteArrayConverter")) {
      keyConverter = record -> new SchemaAndValue(record.keySchema(), record.value());
    } else {
      Converter converter = (Converter) loadClass(config.getString(KEY_CONVERTER), loader);
      converter.configure(PropsParser.apply(props, KEY_CONVERTER), true);
      keyConverter = record -> converter.toConnectData(record.topic(), (byte[]) record.key());
    }

    if (config.getString(VALUE_CONVERTER) == null) {
      throw new TransformInitializationException(
          "ManagedTransformWrapper cannot be used without a defined value converter");
    } else {
      Converter converter = (Converter) loadClass(config.getString(VALUE_CONVERTER), loader);
      converter.configure(PropsParser.apply(props, VALUE_CONVERTER), false);
      valueConverter = record -> converter.toConnectData(record.topic(), (byte[]) record.value());
    }

    HeaderConverter headerConverter;

    if (Objects.equals(
        config.getString(HEADER_CONVERTER),
        "org.apache.kafka.connect.converters.ByteArrayConverter")) {
      try (HeaderConverter converter =
          (HeaderConverter)
              loadClass("org.apache.kafka.connect.converters.ByteArrayConverter", loader)) {
        converter.configure(PropsParser.apply(props, HEADER_CONVERTER));
      } catch (Exception e) {
        throw new TransformInitializationException(
            String.format(
                "Error loading header converter class %s", config.getString(HEADER_CONVERTER)),
            e);
      }
      headerConverterFn = ConnectRecord::headers;
    } else {
      try (HeaderConverter converter =
          (HeaderConverter) loadClass(config.getString(HEADER_CONVERTER), loader)) {
        converter.configure(PropsParser.apply(props, HEADER_CONVERTER));
        headerConverter = converter;
      } catch (Exception e) {
        throw new TransformInitializationException(
            String.format(
                "Error loading header converter class %s", config.getString(HEADER_CONVERTER)),
            e);
      }

      headerConverterFn =
          record -> {
            Headers newHeaders = new ConnectHeaders();
            Headers recordHeaders = record.headers();
            if (recordHeaders != null) {
              String topic = record.topic();
              for (Header recordHeader : recordHeaders) {
                SchemaAndValue schemaAndValue =
                    headerConverter.toConnectHeader(
                        topic, recordHeader.key(), (byte[]) recordHeader.value());
                newHeaders.add(recordHeader.key(), schemaAndValue);
              }
            }
            return newHeaders;
          };
    }

    if (config.getString(TRANSFORMATIONS) == null) {
      smts = Lists.newArrayList();
    } else {

      smts =
          Arrays.stream(config.getString(TRANSFORMATIONS).split(","))
              .map(className -> loadClass(className, loader))
              .map(obj -> (Transformation<SinkRecord>) obj)
              .peek(smt -> smt.configure(PropsParser.apply(props, TRANSFORMATIONS)))
              .collect(Collectors.toList());
    }

    converterErrorHandler =
        (TransformExceptionHandler) loadClass(config.getString(CONVERTER_ERROR_HANDLER), loader);
    converterErrorHandler.configure(props);
    smtErrorHandler =
        (TransformExceptionHandler) loadClass(config.getString(SMT_ERROR_HANDLER), loader);
    smtErrorHandler.configure(props);
  }

  private Object loadClass(String name, ClassLoader loader) {
    if (name == null || name.isEmpty()) {
      throw new TransformInitializationException("cannot initialize empty class");
    }
    Object obj;
    try {
      Class<?> clazz = Class.forName(name, true, loader);
      obj = clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new TransformInitializationException(
          String.format("could not initialize class %s", name), e);
    }
    return obj;
  }

  private DeserializedRecord deserialize(SinkRecord record) {
    SchemaAndValue keyData;
    SchemaAndValue valueData;
    Headers newHeaders;

    try {
      keyData = keyConverter.apply(record);
    } catch (Exception e) {
      return new DeserializedRecord(converterErrorHandler.handle(record, e, KEY_FAILURE), true);
    }

    try {
      valueData = valueConverter.apply(record);
    } catch (Exception e) {
      return new DeserializedRecord(converterErrorHandler.handle(record, e, VALUE_FAILURE), true);
    }
    try {
      newHeaders = headerConverterFn.apply(record);
    } catch (Exception e) {
      return new DeserializedRecord(converterErrorHandler.handle(record, e, HEADER_FAILURE), true);
    }

    return new DeserializedRecord(
        record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            keyData.schema(),
            keyData.value(),
            valueData.schema(),
            valueData.value(),
            record.timestamp(),
            newHeaders),
        false);
  }

  private SinkRecord newRecord(SinkRecord original, SinkRecord transformed) {
    if (!original.headers().isEmpty()) {
      List<Struct> serializedHeaders = DeadLetterUtils.serializedHeaders(original);
      transformed
          .headers()
          .add(
              DeadLetterUtils.HEADERS_HEADER,
              new SchemaAndValue(DeadLetterUtils.HEADER_SCHEMA, serializedHeaders));
    }
    if (original.key() != null) {
      transformed
          .headers()
          .add(
              DeadLetterUtils.KEY_HEADER,
              new SchemaAndValue(OPTIONAL_BYTES_SCHEMA, original.key()));
    }
    if (original.value() != null) {
      transformed
          .headers()
          .add(
              DeadLetterUtils.VALUE_HEADER,
              new SchemaAndValue(OPTIONAL_BYTES_SCHEMA, original.value()));
    }

    return transformed;
  }
}
