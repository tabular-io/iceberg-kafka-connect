// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc.commit;

import java.util.Map;

public class Coordinator extends Channel {

  public Coordinator(Map<String, String> props) {
    super(props);
  }

  @Override
  public void notify(byte[] message) {
    // TODO
  }
}
