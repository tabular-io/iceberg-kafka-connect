// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.transform;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

public class TabularEventTransform<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final ConfigDef EMPTY_CONFIG = new ConfigDef();

  @Override
  public R apply(R record) {
    if (!(record.value() instanceof Map)) {
      throw new IllegalArgumentException("Record must be a map");
    }

    Map<?, ?> original = (Map<?, ?>) record.value();

    final Map<String, Object> result = new HashMap<>();
    result.put("id", original.get("id"));
    result.put("type", original.get("type"));
    result.put("ts", original.get("event_ts_ms"));
    result.put("payload", record.value());

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
