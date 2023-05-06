// Copyright 2023 Tabular Technologies Inc.
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
