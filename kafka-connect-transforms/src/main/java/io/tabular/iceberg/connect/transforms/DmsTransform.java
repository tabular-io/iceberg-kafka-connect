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

import io.tabular.iceberg.connect.transforms.util.KafkaMetadataAppender;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DmsTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger LOG = LoggerFactory.getLogger(DmsTransform.class.getName());
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
              KafkaMetadataAppender.EXTERNAL_KAFKA_METADATA,
              ConfigDef.Type.STRING,
              "none",
              ConfigDef.Importance.LOW,
              "key,value representing a String to be injected on Kafka metadata (e.g. Cluster)");

  private KafkaMetadataAppender kafkaAppender = null;

  @Override
  public R apply(R record) {
    if (record.value() == null) {
      return record;
    } else if (record.valueSchema() == null) {
      return applySchemaless(record);
    } else {
      throw new UnsupportedOperationException("Schema not support for DMS records");
    }
  }

  @SuppressWarnings("unchecked")
  private R applySchemaless(R record) {
    Map<String, Object> value = Requirements.requireMap(record.value(), "DMS transform");

    // promote fields under "data"
    Object dataObj = value.get("data");
    Object metadataObj = value.get("metadata");
    if (!(dataObj instanceof Map) || !(metadataObj instanceof Map)) {
      LOG.debug("Unable to transform DMS record, skipping...");
      return null;
    }

    Map<String, Object> metadata = (Map<String, Object>) metadataObj;

    String dmsOp = metadata.get("operation").toString();
    String op;
    switch (dmsOp) {
      case "update":
        op = CdcConstants.OP_UPDATE;
        break;
      case "delete":
        op = CdcConstants.OP_DELETE;
        break;
      default:
        op = CdcConstants.OP_INSERT;
    }

    // create the CDC metadata
    Map<String, Object> cdcMetadata = Maps.newHashMap();
    cdcMetadata.put(CdcConstants.COL_OP, op);
    cdcMetadata.put(CdcConstants.COL_TS, metadata.get("timestamp"));
    cdcMetadata.put(
        CdcConstants.COL_SOURCE,
        String.format("%s.%s", metadata.get("schema-name"), metadata.get("table-name")));

    // create the new value
    Map<String, Object> newValue = Maps.newHashMap((Map<String, Object>) dataObj);
    newValue.put(CdcConstants.COL_CDC, cdcMetadata);

    if (kafkaAppender != null) {
      if (record instanceof SinkRecord) {
        kafkaAppender.appendToMap((SinkRecord) record, newValue);
      }
    }

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        null,
        newValue,
        record.timestamp());
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    if (config.getBoolean(KafkaMetadataAppender.INCLUDE_KAFKA_METADATA)) {
      kafkaAppender = KafkaMetadataAppender.from(config);
    }
  }
}
