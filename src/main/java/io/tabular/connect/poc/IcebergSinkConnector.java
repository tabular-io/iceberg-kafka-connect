// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class IcebergSinkConnector extends SinkConnector {

  private Map<String, String> props;
  private Catalog catalog;

  @Override
  public String version() {
    return "0.0.1";
  }

  @Override
  public void start(Map<String, String> props) {
    this.props = props;

    Map<String, String> catalogProps = Map.of("uri", "http://iceberg:8181/");

    catalog = new RESTCatalog();
    catalog.initialize("local", catalogProps);
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
