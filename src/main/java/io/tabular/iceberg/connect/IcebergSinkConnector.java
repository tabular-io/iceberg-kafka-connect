// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect;

import static io.tabular.iceberg.connect.IcebergSinkConfig.INTERNAL_TRANSACTIONAL_SUFFIX_PROP;
import static java.util.stream.Collectors.toList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkConnector extends SinkConnector {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkConnector.class.getName());

  private Map<String, String> props;

  @Override
  public String version() {
    return "0.0.1";
  }

  @Override
  public void start(Map<String, String> props) {
    this.props = props;
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    if (config.getTopicAutoCreate()) {
      createControlTopicIfNeeded(config);
    }
  }

  private void createControlTopicIfNeeded(IcebergSinkConfig config) {
    // TODO: move admin functions to new class
    Map<String, Object> adminProps = new HashMap<>(config.getKafkaProps());
    try (Admin admin = Admin.create(adminProps)) {
      // TODO: partitions/replication setting
      NewTopic newTopic = new NewTopic(config.getControlTopic(), 1, (short) 1);
      admin.createTopics(ImmutableList.of(newTopic)).all().get();
      LOG.info("Created control topic: {}", config.getControlTopic());
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        LOG.info("Using existing control topic: {}", config.getControlTopic());
      } else {
        throw new ConnectException(e);
      }
    } catch (InterruptedException e) {
      throw new ConnectException(e);
    }
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
    return IcebergSinkConfig.newConfigDef();
  }
}
