// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect;

import static io.tabular.iceberg.connect.IcebergSinkConfig.INTERNAL_TRANSACTIONAL_SUFFIX_PROP;
import static java.util.stream.Collectors.toList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
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
  public void start(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return IcebergSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return IntStream.range(0, maxTasks)
        .mapToObj(
            i -> {
              Map<String, String> map = new HashMap<>(props);
              map.put(INTERNAL_TRANSACTIONAL_SUFFIX_PROP, "-txn-" + i);
              return map;
            })
        .collect(toList());
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return IcebergSinkConfig.CONFIG_DEF;
  }
}
