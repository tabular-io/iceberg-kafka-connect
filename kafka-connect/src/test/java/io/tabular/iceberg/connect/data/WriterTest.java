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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.api.Committable;
import io.tabular.iceberg.connect.api.Writer;
import io.tabular.iceberg.connect.events.EventTestUtil;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class WriterTest {
  private static final String SRC_TOPIC_NAME = "src-topic";
  private static final String TABLE_NAME = "db.tbl";
  private static final String FIELD_NAME = "fld";
  private static final String CONTROL_TOPIC = "control-topic-name";

  @Test
  public void testStaticRoute() {
    IcebergSinkConfig config =
        new IcebergSinkConfig(
            ImmutableMap.of(
                "iceberg.catalog.catalog-impl",
                "org.apache.iceberg.inmemory.InMemoryCatalog",
                "iceberg.tables",
                TABLE_NAME,
                "iceberg.control.topic",
                CONTROL_TOPIC,
                "iceberg.control.commit.threads",
                "1"));
    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, "val");
    writerTest(config, value);
  }

  @Test
  public void testDynamicRoute() {
    IcebergSinkConfig config =
        new IcebergSinkConfig(
            ImmutableMap.of(
                "iceberg.catalog.catalog-impl",
                "org.apache.iceberg.inmemory.InMemoryCatalog",
                "iceberg.tables.dynamic-enabled",
                "true",
                "iceberg.tables.route-field",
                FIELD_NAME,
                "iceberg.control.topic",
                CONTROL_TOPIC,
                "iceberg.control.commit.threads",
                "1"));

    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    writerTest(config, value);
  }

  private void writerTest(IcebergSinkConfig config, Map<String, Object> value) {
    WriterResult writeResult =
        new WriterResult(
            TableIdentifier.parse(TABLE_NAME),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter icebergWriter = mock(IcebergWriter.class);
    when(icebergWriter.complete()).thenReturn(ImmutableList.of(writeResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(icebergWriter);

    Writer writer = new WriterImpl(config, writerFactory);

    // save a record
    SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
    writer.write(ImmutableList.of(rec));

    Committable committable = writer.committable();

    assertThat(committable.offsetsByTopicPartition()).hasSize(1);
    // offset should be one more than the record offset
    assertThat(
            committable
                .offsetsByTopicPartition()
                .get(committable.offsetsByTopicPartition().keySet().iterator().next())
                .offset())
        .isEqualTo(1L);
  }
}
