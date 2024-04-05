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

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

/* Appends values to record.values that are strings, useful for testing SMT control flow */
public class TestStringTransform implements Transformation<SinkRecord> {

  private String text;
  private boolean returnNull;

  private boolean shouldThrow;

  @Override
  public synchronized SinkRecord apply(SinkRecord record) {
    if (shouldThrow) {
      throw new RuntimeException("smt failure");
    }
    if (record.value() == null) {
      return record;
    }
    if (returnNull) {
      return null;
    }
    String newValue;
    if (record.value() instanceof String) {
      newValue = (record.value()) + text;
    } else {
      throw new IllegalArgumentException("record.value is not a string");
    }
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        newValue,
        record.timestamp());
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> map) {
    if (map.get("transform_text") != null) {
      text = (String) map.get("transform_text");
    } else {
      text = "default";
    }

    if (map.get("null") != null) {
      returnNull = Boolean.parseBoolean((String) map.get("null"));
    } else {
      returnNull = false;
    }

    if (map.get("throw") != null) {
      shouldThrow = Boolean.parseBoolean((String) map.get("throw"));
    } else {
      shouldThrow = false;
    }
  }
}
