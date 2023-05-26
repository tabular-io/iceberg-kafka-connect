/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

public class IcebergSinkConfigTest {

  @Test
  public void testGetVersion() {
    String version = IcebergSinkConfig.getVersion();
    assertNotNull(version);
  }

  @Test
  public void testMissingRequired() {
    Map<String, String> props = ImmutableMap.of();
    assertThatExceptionOfType(ConfigException.class).isThrownBy(() -> new IcebergSinkConfig(props));
  }

  @Test
  public void testInvalid() {
    Map<String, String> props =
        ImmutableMap.of(
            "topics", "source-topic",
            "iceberg.catalog", RESTCatalog.class.getName(),
            "iceberg.tables", "db.landing",
            "iceberg.tables.dynamic.enabled", "true");
    assertThatExceptionOfType(ConfigException.class).isThrownBy(() -> new IcebergSinkConfig(props));
  }

  @Test
  public void testGetDefault() {
    Map<String, String> props =
        ImmutableMap.of(
            "iceberg.catalog", RESTCatalog.class.getName(),
            "topics", "source-topic",
            "iceberg.tables", "db.landing");
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    assertEquals(300_000, config.getCommitIntervalMs());
  }
}
