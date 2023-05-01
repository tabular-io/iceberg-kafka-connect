// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect;

import static java.util.stream.Collectors.toCollection;

import java.util.Arrays;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.sink.SinkConnector;

public class IcebergSinkConfig {

  public static final String INTERNAL_TRANSACTIONAL_SUFFIX_PROP =
      "iceberg.coordinator.transactional.suffix";

  private static final String CATALOG_PROP_PREFIX = "iceberg.catalog.";
  private static final String KAFKA_PROP_PREFIX = "iceberg.kafka.";

  private static final String CATALOG_IMPL_PROP = "iceberg.catalog";
  private static final String TABLE_PROP = "iceberg.table";
  private static final String CONTROL_TOPIC_PROP = "iceberg.control.topic";
  private static final String CONTROL_GROUP_ID_PROP = "iceberg.control.group.id";
  private static final String COMMIT_INTERVAL_MS_PROP = "iceberg.table.commitIntervalMs";
  private static final int COMMIT_INTERVAL_MS_DEFAULT = 60_000;
  private static final String COMMIT_TIMEOUT_MS_PROP = "iceberg.table.commitTimeoutMs";
  private static final int COMMIT_TIMEOUT_MS_DEFAULT = 30_000;

  public static ConfigDef newConfigDef() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(CATALOG_IMPL_PROP, Type.STRING, Importance.HIGH, "Iceberg catalog class name");
    configDef.define(
        CONTROL_TOPIC_PROP, Type.STRING, Importance.HIGH, "Name of the Kafka control topic");
    configDef.define(
        CONTROL_GROUP_ID_PROP,
        Type.STRING,
        Importance.HIGH,
        "Name of the consumer group to store offsets");
    configDef.define(
        COMMIT_INTERVAL_MS_PROP,
        Type.INT,
        COMMIT_INTERVAL_MS_DEFAULT,
        Importance.MEDIUM,
        "Coordinator interval for performing Iceberg table commits, in millis");
    configDef.define(
        COMMIT_TIMEOUT_MS_PROP,
        Type.INT,
        COMMIT_TIMEOUT_MS_DEFAULT,
        Importance.MEDIUM,
        "Coordinator time to wait for worker responses before committing, in millis");
    return configDef;
  }

  // TODO: cache values?

  private final Map<String, String> props;

  public IcebergSinkConfig(Map<String, String> props) {
    this.props = props;
  }

  public String getTransactionalSuffix() {
    return props.get(INTERNAL_TRANSACTIONAL_SUFFIX_PROP);
  }

  public SortedSet<String> getTopics() {
    return Arrays.stream(props.get(SinkConnector.TOPICS_CONFIG).split(","))
        .map(String::trim)
        .collect(toCollection(TreeSet::new));
  }

  public Map<String, String> getCatalogProps() {
    return PropertyUtil.propertiesWithPrefix(props, CATALOG_PROP_PREFIX);
  }

  public Map<String, String> getKafkaProps() {
    return PropertyUtil.propertiesWithPrefix(props, KAFKA_PROP_PREFIX);
  }

  public String getCatalogImpl() {
    return props.get(CATALOG_IMPL_PROP);
  }

  public TableIdentifier getTable() {
    return TableIdentifier.parse(props.get(TABLE_PROP));
  }

  public String getControlTopic() {
    return props.get(CONTROL_TOPIC_PROP);
  }

  public String getControlGroupId() {
    return props.get(CONTROL_GROUP_ID_PROP);
  }

  public int getCommitIntervalMs() {
    return PropertyUtil.propertyAsInt(props, COMMIT_INTERVAL_MS_PROP, COMMIT_INTERVAL_MS_DEFAULT);
  }

  public int getCommitTimeoutMs() {
    return PropertyUtil.propertyAsInt(props, COMMIT_TIMEOUT_MS_PROP, COMMIT_TIMEOUT_MS_DEFAULT);
  }
}
