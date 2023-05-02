// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

public enum EventType {
  COMMIT_REQUEST(0),
  COMMIT_RESPONSE(1);

  private final int id;

  EventType(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }
}
