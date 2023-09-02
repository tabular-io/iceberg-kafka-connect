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
package io.tabular.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class UtilitiesTest {

  public static class TestCatalog extends InMemoryCatalog implements Configurable<Configuration> {
    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }
  }

  @Test
  public void testLoadCatalog() {
    Map<String, String> props =
        ImmutableMap.of(
            "topics",
            "mytopic",
            "iceberg.tables",
            "mytable",
            "iceberg.hadoop.prop",
            "value",
            "iceberg.catalog.catalog-impl",
            TestCatalog.class.getName());
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    Catalog result = Utilities.loadCatalog(config);

    assertThat(result).isInstanceOf(TestCatalog.class);

    Configuration conf = ((TestCatalog) result).conf;
    assertThat(conf).isNotNull();
    assertThat(conf.get("prop")).isEqualTo("value");

    // check that core-site.xml was loaded
    assertThat(conf.get("foo")).isEqualTo("bar");
  }
}
