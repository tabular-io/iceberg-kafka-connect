// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.transform;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

public class TabularEventTransform<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final ConfigDef EMPTY_CONFIG = new ConfigDef();

  @Override
  public R apply(R record) {
    Preconditions.checkArgument(record.value() instanceof Map);
    Map<?, ?> original = (Map<?, ?>) record.value();

    final Map<String, Object> updated = new HashMap<>();
    updated.put("id", original.get("id"));
    updated.put("type", original.get("type"));
    updated.put("ts", original.get("event_ts_ms"));
    updated.put("payload", record.value());

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        updated,
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
