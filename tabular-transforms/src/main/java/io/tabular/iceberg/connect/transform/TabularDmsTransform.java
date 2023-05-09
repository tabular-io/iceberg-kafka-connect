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

import static java.lang.String.format;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TabularDmsTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger LOG = LoggerFactory.getLogger(TabularDmsTransform.class.getName());
  private static final ConfigDef EMPTY_CONFIG = new ConfigDef();

  @Override
  @SuppressWarnings("unchecked")
  public R apply(R record) {
    if (!(record.value() instanceof Map)) {
      throw new IllegalArgumentException("Record must be a map");
    }

    Map<?, ?> original = (Map<?, ?>) record.value();

    // promote fields under "data"
    Object dataObj = original.get("data");
    Object metadataObj = original.get("metadata");
    if (!(dataObj instanceof Map) || !(metadataObj instanceof Map)) {
      LOG.warn("Unable to transform DMS record, leaving as-is...");
      return record;
    }

    final Map<String, Object> result = new HashMap<>((Map<String, Object>) dataObj);

    Map<String, Object> metadata = (Map<String, Object>) metadataObj;

    String dmsOp = metadata.get("operation").toString();
    String op;
    switch (dmsOp) {
      case "update":
        op = "U";
        break;
      case "delete":
        op = "D";
        break;
      default:
        op = "I";
    }

    result.put("_cdc_op", op);
    result.put(
        "_cdc_table", format("%s.%s", metadata.get("schema-name"), metadata.get("table-name")));
    result.put("_cdc_ts", metadata.get("timestamp"));

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        result,
        record.timestamp());
  }

  @Override
  public ConfigDef config() {
    return EMPTY_CONFIG;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {}
}
