// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

public enum EventType {
  BEGIN_COMMIT(0),
  WRITE_RESULT(1);

  private final int id;

  EventType(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }
}
