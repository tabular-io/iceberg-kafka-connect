// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc.custom;

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

    final Map<String, Object> event = new HashMap<>();
    event.put("id", original.get("id"));
    event.put("type", original.get("type"));
    event.put("event_ts", original.get("event_ts_ms"));
    event.put("payload", record.value());

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        event,
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
