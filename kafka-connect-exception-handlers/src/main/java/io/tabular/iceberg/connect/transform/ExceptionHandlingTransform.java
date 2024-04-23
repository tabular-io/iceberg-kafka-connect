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
package io.tabular.iceberg.connect.transform;

import io.tabular.iceberg.connect.DeadLetterTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * This transform only works under the following conditions:
 * <ol>
 *  <li>Connector is configured with iceberg.tables.dynamic-enabled=true</li>
 *  <li>Connector is configured with with a iceberg.tables.route-field=target_table</li>
 *  <li>Connector is configured with key.converter=ByteArrayConverter</li>
 *  <li>Connector is configured with value.converter=ByteArrayConverter</li>
 *  <li>Connector is configured with header.converter=ByteArrayConverter</li>
 * </ol>
 */
// TODO: bring in Lists and Maps classes library to get rid of SuppressWarning annotation
@SuppressWarnings("RegexpSingleline")
public class ExceptionHandlingTransform implements Transformation<SinkRecord> {

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

  private static final String KEY_CONVERTER = "key.converter";
  private static final String VALUE_CONVERTER = "value.converter";
  private static final String HEADER_CONVERTER = "header.converter";
  private static final String TRANSFORMATIONS = "SMTs";

  private List<Transformation<SinkRecord>> smts;
  private Converter keyConverter;
  private Converter valueConverter;
  private HeaderConverter headerConverter;

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              KEY_CONVERTER,
              ConfigDef.Type.STRING,
              "org.apache.kafka.connect.converters.ByteArrayConverter",
              ConfigDef.Importance.MEDIUM,
              "The key.converter to use")
          .define(
              VALUE_CONVERTER,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.MEDIUM,
              "The value.converter to use")
          .define(
              HEADER_CONVERTER,
              ConfigDef.Type.STRING,
              "org.apache.kafka.connect.converters.ByteArrayConverter",
              ConfigDef.Importance.MEDIUM,
              "The header.converter to use")
          .define(
              TRANSFORMATIONS,
              ConfigDef.Type.STRING,
              new ArrayList<>(),
              ConfigDef.Importance.MEDIUM,
              "The SMTs to apply");

  @Override
  public SinkRecord apply(SinkRecord originalRecord) {
    try {
      SinkRecord deserializedRecord = deserialize(originalRecord);
      SinkRecord transformedRecord = transform(deserializedRecord);
      return transformedRecord;
    } catch (Exception exception) {
      // Users could customize here and make a dead-letter table per topic or whatever they want
      return DeadLetterTable.sinkRecord(originalRecord, exception, DeadLetterTable.NAME);
    }
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    // TODO: humm close AutoCloseable converters and SMTs
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    ClassLoader loader = this.getClass().getClassLoader();

    keyConverter = (Converter) loadClass(config.getString(KEY_CONVERTER), loader);
    keyConverter.configure(PropsParser.apply(props, KEY_CONVERTER), true);

    valueConverter = (Converter) loadClass(config.getString(VALUE_CONVERTER), loader);
    valueConverter.configure(PropsParser.apply(props, VALUE_CONVERTER), false);

    headerConverter = (HeaderConverter) loadClass(config.getString(HEADER_CONVERTER), loader);
    headerConverter.configure(PropsParser.apply(props, HEADER_CONVERTER));

    smts =
        Arrays.stream(config.getString(TRANSFORMATIONS).split(","))
            .map(className -> loadClass(className, loader))
            .map(obj -> (Transformation<SinkRecord>) obj)
            .peek(smt -> smt.configure(PropsParser.apply(props, TRANSFORMATIONS)))
            .collect(Collectors.toList());
  }

  private Object loadClass(String name, ClassLoader loader) {
    if (name == null || name.isEmpty()) {
      throw new RuntimeException("cannot initialize empty class");
    }
    Object obj;
    try {
      Class<?> clazz = Class.forName(name, true, loader);
      obj = clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(String.format("Could not initialize class %s", name), e);
    }
    return obj;
  }

  private SinkRecord deserialize(SinkRecord originalRecord) {
    String topic = originalRecord.topic();

    RecordHeaders recordHeaders = new RecordHeaders();
    for (Header header : originalRecord.headers()) {
      recordHeaders.add(
          header.key(),
          headerConverter.fromConnectHeader(topic, header.key(), header.schema(), header.value()));
    }

    SchemaAndValue key =
        keyConverter.toConnectData(topic, recordHeaders, (byte[]) originalRecord.key());

    SchemaAndValue value =
        valueConverter.toConnectData(topic, recordHeaders, (byte[]) originalRecord.value());

    return originalRecord.newRecord(
        topic,
        originalRecord.kafkaPartition(),
        key.schema(),
        key.value(),
        value.schema(),
        value.value(),
        originalRecord.timestamp(),
        originalRecord.headers());
  }

  private SinkRecord transform(SinkRecord deserializedRecord) {
    SinkRecord transformedRecord = deserializedRecord;
    for (Transformation<SinkRecord> smt : smts) {
      transformedRecord = smt.apply(deserializedRecord);
    }
    return transformedRecord;
  }
}
