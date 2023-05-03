// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

public class IcebergSinkConfigTest {

  @Test
  public void testMissingRequired() {
    Map<String, String> props = ImmutableMap.of();
    assertThatExceptionOfType(ConfigException.class).isThrownBy(() -> new IcebergSinkConfig(props));
  }

  @Test
  public void testGetDefault() {
    Map<String, String> props =
        ImmutableMap.of(
            "topics", "source-topic",
            "iceberg.table", "db.landing",
            "iceberg.catalog", RESTCatalog.class.getName(),
            "iceberg.control.topic", "control-topic",
            "iceberg.control.group.id", "control-group");
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    assertEquals(60_000, config.getCommitIntervalMs());
  }
}
