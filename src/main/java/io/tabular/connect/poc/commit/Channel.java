// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc.commit;

import java.util.Map;

public class Channel {

  private final String bootstrapServers;
  private final String coordinatorTopic;

  public Channel(Map<String, String> props) {
    this.bootstrapServers = props.get("bootstrap.servers");
    this.coordinatorTopic = props.get("iceberg.coordinator.topic");
  }
}
