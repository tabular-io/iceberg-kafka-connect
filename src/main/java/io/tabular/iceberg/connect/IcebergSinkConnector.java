// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect;

import static java.util.stream.Collectors.toList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.log4j.Logger;

public class IcebergSinkConnector extends SinkConnector {

  private static final Logger LOG = Logger.getLogger(IcebergSinkConnector.class);

  private Map<String, String> props;

  public static final String COORDINATOR_PROP = "iceberg.coordinator";

  // TODO: the following are all defined in Channel also...
  private static final String TRANSACTIONAL_SUFFIX_PROP =
      "iceberg.coordinator.transactional.suffix";
  private static final String KAFKA_PROP_PREFIX = "iceberg.kafka.";
  private static final String COORDINATOR_TOPIC_PROP = "iceberg.coordinator.topic";
  private static final String COORDINATOR_TOPIC_CREATION_PROP =
      "iceberg.coordinator.topic.creation.enable";

  @Override
  public String version() {
    return "0.0.1";
  }

  @Override
  public void start(Map<String, String> props) {
    this.props = props;
    createCoordinatorTopicIfNotExists();
  }

  private void createCoordinatorTopicIfNotExists() {
    boolean creationEnable =
        PropertyUtil.propertyAsBoolean(props, COORDINATOR_TOPIC_CREATION_PROP, true);
    if (!creationEnable) {
      return;
    }

    String coordinatorTopic = props.get(COORDINATOR_TOPIC_PROP);
    Map<String, Object> adminProps =
        new HashMap<>(PropertyUtil.propertiesWithPrefix(props, KAFKA_PROP_PREFIX));
    try (Admin admin = Admin.create(adminProps)) {
      admin
          .createTopics(ImmutableList.of(new NewTopic(coordinatorTopic, 1, (short) 1)))
          .all()
          .get();
      LOG.info("Created coordinator topic: " + coordinatorTopic);
    } catch (InterruptedException | ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        LOG.info("Coordinator topic exists: " + coordinatorTopic);
      } else {
        throw new RuntimeException(e);
      }
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
              map.put(TRANSACTIONAL_SUFFIX_PROP, "-txn-" + i);
              if (i == 0) {
                // make one task the coordinator
                map.put(COORDINATOR_PROP, "true");
              }
              return map;
            })
        .collect(toList());
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }
}
