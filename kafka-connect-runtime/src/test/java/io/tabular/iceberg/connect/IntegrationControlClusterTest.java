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

import static io.tabular.iceberg.connect.TestEvent.TEST_SCHEMA;
import static io.tabular.iceberg.connect.TestEvent.TEST_SPEC;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;

public class IntegrationControlClusterTest extends IntegrationTestBase {

  private static final String TEST_DB = "test";
  private static final String TEST_TABLE = "foobar";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);

  @BeforeEach
  public void before() {
    createTopic(testTopic, TEST_TOPIC_PARTITIONS);
    ((SupportsNamespaces) catalog).createNamespace(Namespace.of(TEST_DB));
  }

  @AfterEach
  public void after() {
    context.stopConnector(connectorName);
    deleteTopic(testTopic);
    catalog.dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE));
    ((SupportsNamespaces) catalog).dropNamespace(Namespace.of(TEST_DB));
  }

  @ParameterizedTest
  @NullSource
  public void testIcebergSinkPartitionedTable(String branch)
      throws InterruptedException, ExecutionException {
    catalog.createTable(
        TABLE_IDENTIFIER, TEST_SCHEMA, TEST_SPEC, ImmutableMap.of(FORMAT_VERSION, "2"));

    boolean useSchema = branch == null; // use a schema for one of the tests
    runTest(branch, useSchema);

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    // partition may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(2, 3);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(4);

    List<DeleteFile> deleteFiles = deleteFiles(TABLE_IDENTIFIER, branch);
    // partition may involve 1 or 2 workers
    assertThat(files).hasSizeBetween(2, 3);
    assertThat(deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum()).isEqualTo(2);

    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  private void runTest(String branch, boolean useSchema)
      throws InterruptedException, ExecutionException {
    // set offset reset to earliest so we don't miss any test messages
    KafkaConnectContainer.Config connectorConfig =
        new KafkaConnectContainer.Config(connectorName)
            .config("topics", testTopic)
            .config("connector.class", IcebergSinkConnector.class.getName())
            .config("tasks.max", 2)
            .config("consumer.override.auto.offset.reset", "earliest")
            .config("key.converter", "org.apache.kafka.connect.json.JsonConverter")
            .config("key.converter.schemas.enable", false)
            .config("value.converter", "org.apache.kafka.connect.json.JsonConverter")
            .config("value.converter.schemas.enable", useSchema)
            .config("iceberg.tables", String.format("%s.%s", TEST_DB, TEST_TABLE))
            .config("iceberg.tables.cdc-field", "op")
            .config("iceberg.control.commit.interval-ms", 1000)
            .config("iceberg.control.commit.timeout-ms", 10000)
            .config("iceberg.kafka.auto.offset.reset", "earliest")
            .config(
                IcebergSinkConfig.COMMITTER_FACTORY_CLASS_PROP,
                IcebergSinkConfig.COMMITTER_FACTORY_V2)
            .config(
                String.format(
                    "iceberg.control.kafka.%s", CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
                context.getRemoteControlBootstrapServers());

    context.connectorCatalogProperties().forEach(connectorConfig::config);

    if (branch != null) {
      connectorConfig.config("iceberg.tables.default-commit-branch", branch);
    }

    if (!useSchema) {
      connectorConfig.config("value.converter.schemas.enable", false);
    }

    context.startConnector(connectorConfig);

    // start with 3 records, update 1, delete 1. Should be a total of 4 adds and 2 deletes
    // (the update will be 1 add and 1 delete)

    TestEvent event1 = new TestEvent(1, "type1", new Date(), "hello world!", "I");
    TestEvent event2 = new TestEvent(2, "type2", new Date(), "having fun?", "I");

    Date threeDaysAgo = Date.from(Instant.now().minus(Duration.ofDays(3)));
    TestEvent event3 = new TestEvent(3, "type3", threeDaysAgo, "hello from the past!", "I");

    TestEvent event4 = new TestEvent(1, "type1", new Date(), "hello world!", "D");
    TestEvent event5 = new TestEvent(3, "type3", threeDaysAgo, "updated!", "U");

    send(testTopic, 0, event1, useSchema);
    send(testTopic, 1, event2, useSchema);
    send(testTopic, 0, event3, useSchema);
    send(testTopic, 1, event4, useSchema);
    send(testTopic, 0, event5, useSchema);
    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(this::assertSnapshotAdded);

    // TODO: assert visible behaviours:
    //  - control topic exists only on control cluster
    //  - consumer offsets exists only on control cluster

    Map<TopicPartition, Long> controlConsumerOffsets =
        controlAdmin
            .listConsumerGroupOffsets(
                IcebergSinkConfig.DEFAULT_CONTROL_GROUP_PREFIX + connectorName,
                new ListConsumerGroupOffsetsOptions().requireStable(true))
            .partitionsToOffsetAndMetadata().get().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, (entry) -> entry.getValue().offset()));

    assertThat(controlConsumerOffsets)
        .isEqualTo(
            ImmutableMap.of(
                new TopicPartition(testTopic, 0), 3L,
                new TopicPartition(testTopic, 1), 2L));
  }

  private void assertSnapshotAdded() {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    assertThat(table.snapshots()).hasSize(1);
  }
}
