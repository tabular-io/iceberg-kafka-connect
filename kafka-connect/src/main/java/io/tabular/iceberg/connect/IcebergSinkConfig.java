// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.sink.SinkConnector;

public class IcebergSinkConfig extends AbstractConfig {

  public static final String INTERNAL_TRANSACTIONAL_SUFFIX_PROP =
      "iceberg.coordinator.transactional.suffix";
  private static final String ROUTE_VALUES = "routeValues";

  private static final String CATALOG_PROP_PREFIX = "iceberg.catalog.";
  private static final String KAFKA_PROP_PREFIX = "iceberg.kafka.";
  private static final String TABLE_PROP_PREFIX = "iceberg.table.";

  private static final String CATALOG_IMPL_PROP = "iceberg.catalog";
  private static final String TABLES_PROP = "iceberg.tables";
  private static final String TABLES_ROUTE_FIELD_PROP = "iceberg.tables.routeField";
  private static final String TABLES_CDC_FIELD_PROP = "iceberg.tables.cdcField";
  private static final String TABLES_UPSERT_MODE_ENABLED_PROP = "iceberg.tables.upsertModeEnabled";
  private static final String CONTROL_TOPIC_PROP = "iceberg.control.topic";
  private static final String CONTROL_GROUP_ID_PROP = "iceberg.control.group.id";
  private static final String COMMIT_INTERVAL_MS_PROP = "iceberg.control.commitIntervalMs";
  private static final int COMMIT_INTERVAL_MS_DEFAULT = 60_000;
  private static final String COMMIT_TIMEOUT_MS_PROP = "iceberg.control.commitTimeoutMs";
  private static final int COMMIT_TIMEOUT_MS_DEFAULT = 30_000;

  public static ConfigDef CONFIG_DEF = newConfigDef();

  private final Map<String, Pattern> tableRouteRegexMap = new HashMap<>();

  public static String getVersion() {
    String kcVersion = IcebergSinkConfig.class.getPackage().getImplementationVersion();
    if (kcVersion == null) {
      kcVersion = "unknown";
    }
    return IcebergBuild.version() + "-kc-" + kcVersion;
  }

  private static ConfigDef newConfigDef() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(
        SinkConnector.TOPICS_CONFIG,
        Type.LIST,
        Importance.HIGH,
        "Comma-delimited list of source topics");
    configDef.define(
        TABLES_PROP, Type.LIST, Importance.HIGH, "Comma-delimited list of destination tables");
    configDef.define(
        TABLES_ROUTE_FIELD_PROP,
        Type.STRING,
        null,
        Importance.MEDIUM,
        "Source record field for routing records to tables");
    configDef.define(
        TABLES_CDC_FIELD_PROP,
        Type.STRING,
        null,
        Importance.MEDIUM,
        "Source record field that identifies the type of operation (insert, update, or delete)");
    configDef.define(
        TABLES_UPSERT_MODE_ENABLED_PROP,
        Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to treat all appends as upserts, false otherwise");
    configDef.define(CATALOG_IMPL_PROP, Type.STRING, Importance.HIGH, "Iceberg catalog class name");
    configDef.define(CONTROL_TOPIC_PROP, Type.STRING, Importance.HIGH, "Name of the control topic");
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

  public List<String> getTables() {
    return getList(TABLES_PROP);
  }

  public String getTablesRouteField() {
    return getString(TABLES_ROUTE_FIELD_PROP);
  }

  public Pattern getTableRouteValues(String tableName) {
    return tableRouteRegexMap.computeIfAbsent(
        tableName,
        notUsed -> {
          String value = props.get(TABLE_PROP_PREFIX + tableName + "." + ROUTE_VALUES);
          if (value == null) {
            return null;
          }
          return Pattern.compile(value);
        });
  }

  public String getTablesCdcField() {
    return getString(TABLES_CDC_FIELD_PROP);
  }

  public String getControlTopic() {
    return getString(CONTROL_TOPIC_PROP);
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

  public boolean isUpsertMode() {
    return getBoolean(TABLES_UPSERT_MODE_ENABLED_PROP);
  }
}
