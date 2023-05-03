// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.sink.SinkConnector;

public class IcebergSinkConfig extends AbstractConfig {

  public static final String INTERNAL_TRANSACTIONAL_SUFFIX_PROP =
      "iceberg.coordinator.transactional.suffix";

  private static final String CATALOG_PROP_PREFIX = "iceberg.catalog.";
  private static final String KAFKA_PROP_PREFIX = "iceberg.kafka.";

  private static final String CATALOG_IMPL_PROP = "iceberg.catalog";
  private static final String TABLE_PROP = "iceberg.table";
  private static final String CONTROL_TOPIC_PROP = "iceberg.control.topic";
  private static final String CONTROL_TOPIC_PARTITIONS_PROP = "iceberg.control.topic.partitions";
  private static final String CONTROL_TOPIC_REPLICATION_PROP = "iceberg.control.topic.replication";
  private static final String CONTROL_GROUP_ID_PROP = "iceberg.control.group.id";
  private static final String COMMIT_INTERVAL_MS_PROP = "iceberg.table.commitIntervalMs";
  private static final int COMMIT_INTERVAL_MS_DEFAULT = 60_000;
  private static final String COMMIT_TIMEOUT_MS_PROP = "iceberg.table.commitTimeoutMs";
  private static final int COMMIT_TIMEOUT_MS_DEFAULT = 30_000;
  private static final String TOPIC_AUTO_CREATE_PROP = "topic.auto.create";

  public static ConfigDef CONFIG_DEF = newConfigDef();

  private static ConfigDef newConfigDef() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(
        SinkConnector.TOPICS_CONFIG,
        Type.LIST,
        Importance.HIGH,
        "Comma-delimited list of source topics");
    configDef.define(TABLE_PROP, Type.STRING, Importance.HIGH, "Iceberg destination table");
    configDef.define(CATALOG_IMPL_PROP, Type.STRING, Importance.HIGH, "Iceberg catalog class name");
    configDef.define(CONTROL_TOPIC_PROP, Type.STRING, Importance.HIGH, "Name of the control topic");
    configDef.define(
        CONTROL_TOPIC_PARTITIONS_PROP,
        Type.INT,
        1,
        Importance.MEDIUM,
        "Number of partitions to use when automatically creating the control topic");
    configDef.define(
        CONTROL_TOPIC_REPLICATION_PROP,
        Type.SHORT,
        (short) 1,
        Importance.MEDIUM,
        "Replication factor to use when automatically creating the control topic");
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
    configDef.define(
        TOPIC_AUTO_CREATE_PROP,
        Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Whether to automatically create the control topic or not");
    return configDef;
  }

  private final Map<String, String> props;

  public IcebergSinkConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
    this.props = props;
  }

  public String getTransactionalSuffix() {
    // this is for internal use and is not part of the config definition...
    return props.get(INTERNAL_TRANSACTIONAL_SUFFIX_PROP);
  }

  public SortedSet<String> getTopics() {
    return new TreeSet<>(getList(SinkConnector.TOPICS_CONFIG));
  }

  public Map<String, String> getCatalogProps() {
    return PropertyUtil.propertiesWithPrefix(props, CATALOG_PROP_PREFIX);
  }

  public Map<String, String> getKafkaProps() {
    return PropertyUtil.propertiesWithPrefix(props, KAFKA_PROP_PREFIX);
  }

  public String getCatalogImpl() {
    return getString(CATALOG_IMPL_PROP);
  }

  public TableIdentifier getTable() {
    return TableIdentifier.parse(getString(TABLE_PROP));
  }

  public String getControlTopic() {
    return getString(CONTROL_TOPIC_PROP);
  }

  public int getControlTopicPartitions() {
    return getInt(CONTROL_TOPIC_PARTITIONS_PROP);
  }

  public short getControlTopicReplication() {
    return getShort(CONTROL_TOPIC_REPLICATION_PROP);
  }

  public String getControlGroupId() {
    return getString(CONTROL_GROUP_ID_PROP);
  }

  public int getCommitIntervalMs() {
    return getInt(COMMIT_INTERVAL_MS_PROP);
  }

  public int getCommitTimeoutMs() {
    return getInt(COMMIT_TIMEOUT_MS_PROP);
  }

  public boolean getTopicAutoCreate() {
    return getBoolean(TOPIC_AUTO_CREATE_PROP);
  }
}
