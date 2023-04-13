// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class IcebergSinkConnector extends SinkConnector {

  private Map<String, String> props;

  @Override
  public String version() {
    return "0.0.1";
  }

  @Override
  @SneakyThrows
  public void start(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return IcebergSinkConnectorTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return IntStream.range(0, maxTasks).mapToObj(i -> props).collect(toList());
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }
}
