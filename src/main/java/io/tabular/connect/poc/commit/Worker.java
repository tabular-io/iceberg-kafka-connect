// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc.commit;

import java.util.Map;

public class Worker extends Channel {
  public Worker(Map<String, String> props) {
    super(props);
  }

  @Override
  public void notify(byte[] message) {
    // TODO
  }
}
