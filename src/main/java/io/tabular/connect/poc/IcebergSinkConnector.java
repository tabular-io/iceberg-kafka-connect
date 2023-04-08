// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class IcebergSinkConnector extends SinkConnector {

  private Map<String, String> props;
  private Catalog catalog;

  private static final String CATALOG_PROP = "iceberg.catalog";
  private static final String CATALOG_PROP_PREFIX = "iceberg.catalog.";
  private static final int CATALOG_PROP_PREFIX_LEN = CATALOG_PROP_PREFIX.length();

  @Override
  public String version() {
    return "0.0.1";
  }

  @Override
  public void start(Map<String, String> props) {
    this.props = props;

    String catalogImpl = props.get(CATALOG_PROP);
    Map<String, String> catalogProps = new HashMap<>();
    for (Entry<String, String> entry : props.entrySet()) {
      if (entry.getKey().startsWith(CATALOG_PROP_PREFIX)) {
        catalogProps.put(entry.getKey().substring(CATALOG_PROP_PREFIX_LEN), entry.getValue());
      }
    }

    catalog = CatalogUtil.loadCatalog(catalogImpl, "iceberg", catalogProps, null);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return IcebergSinkConnectorTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return List.of(props);
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }
}
