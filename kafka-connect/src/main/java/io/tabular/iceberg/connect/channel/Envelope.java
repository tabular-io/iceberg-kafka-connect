// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel;

import io.tabular.iceberg.connect.channel.events.Event;

public class Envelope {
  private final Event event;
  private final int partition;
  private final long offset;

  public Envelope(Event event, int partition, long offset) {
    this.event = event;
    this.partition = partition;
    this.offset = offset;
  }

  public Event getEvent() {
    return event;
  }

  public int getPartition() {
    return partition;
  }

  public long getOffset() {
    return offset;
  }
}
