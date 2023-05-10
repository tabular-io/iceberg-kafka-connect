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

import static java.lang.String.format;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IntegrationCdcTest extends IntegrationTestBase {

  private static final String CONNECTOR_NAME = "test_connector";
  private static final String TEST_TOPIC = "test-topic";
  private static final String TEST_DB = "default";
  private static final String TEST_TABLE = "foobar";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);
  private static final Schema TEST_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "type", Types.StringType.get()),
              Types.NestedField.required(3, "ts", Types.TimestampType.withoutZone()),
              Types.NestedField.required(4, "payload", Types.StringType.get())),
          ImmutableSet.of(1));

  private static final PartitionSpec TEST_SPEC =
      PartitionSpec.builderFor(TEST_SCHEMA).day("ts").build();

  private static final String RECORD_FORMAT =
      "{\"id\":%d,\"type\":\"%s\",\"ts\":%d,\"payload\":\"%s\",\"op\":\"%s\"}";

  @BeforeEach
  public void setup() {
    createTopic(TEST_TOPIC, 2);
    catalog.createNamespace(Namespace.of(TEST_DB));
  }

  @AfterEach
  public void teardown() {
    deleteTopic(TEST_TOPIC);
    catalog.dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE));
    catalog.dropNamespace(Namespace.of(TEST_DB));
  }

  @Test
  public void testIcebergSink() throws Exception {
    // set offset reset to earliest so we don't miss any test messages
    // TODO: get bootstrap.servers from worker properties?
    KafkaConnectContainer.Config connectorConfig =
        new KafkaConnectContainer.Config(CONNECTOR_NAME)
            .config("topics", TEST_TOPIC)
            .config("connector.class", IcebergSinkConnector.class.getName())
            .config("tasks.max", 2)
            .config("consumer.override.auto.offset.reset", "earliest")
            .config("key.converter", "org.apache.kafka.connect.json.JsonConverter")
            .config("key.converter.schemas.enable", false)
            .config("value.converter", "org.apache.kafka.connect.json.JsonConverter")
            .config("value.converter.schemas.enable", false)
            .config("iceberg.tables", format("%s.%s", TEST_DB, TEST_TABLE))
            .config("iceberg.tables.cdcField", "op")
            .config("iceberg.control.commitIntervalMs", 1000)
            .config("iceberg.control.commitTimeoutMs", 1000)
            .config("iceberg.catalog", RESTCatalog.class.getName())
            .config("iceberg.catalog." + CatalogProperties.URI, "http://iceberg:8181")
            .config("iceberg.catalog." + AwsProperties.S3FILEIO_ENDPOINT, "http://minio:9000")
            .config("iceberg.catalog." + AwsProperties.S3FILEIO_ACCESS_KEY_ID, AWS_ACCESS_KEY)
            .config("iceberg.catalog." + AwsProperties.S3FILEIO_SECRET_ACCESS_KEY, AWS_SECRET_KEY)
            .config("iceberg.catalog." + AwsProperties.S3FILEIO_PATH_STYLE_ACCESS, true)
            .config("iceberg.catalog." + AwsProperties.CLIENT_REGION, AWS_REGION)
            .config(
                "iceberg.catalog." + AwsProperties.HTTP_CLIENT_TYPE,
                AwsProperties.HTTP_CLIENT_TYPE_APACHE);

    // partitioned table

    catalog.createTable(
        TABLE_IDENTIFIER, TEST_SCHEMA, TEST_SPEC, ImmutableMap.of(FORMAT_VERSION, "2"));

    kafkaConnect.registerConnector(connectorConfig);
    kafkaConnect.ensureConnectorRunning(CONNECTOR_NAME);

    runTest();

    List<DataFile> files = getDataFiles();
    assertThat(files).hasSize(2);
    assertEquals(2, files.get(0).recordCount());
    assertEquals(2, files.get(1).recordCount());

    List<DeleteFile> deleteFiles = getDeleteFiles();
    assertThat(deleteFiles).hasSize(2);
    assertEquals(1, deleteFiles.get(0).recordCount());
    assertEquals(1, deleteFiles.get(1).recordCount());

    // unpartitioned table

    catalog.dropTable(TABLE_IDENTIFIER);
    catalog.createTable(TABLE_IDENTIFIER, TEST_SCHEMA, null, ImmutableMap.of(FORMAT_VERSION, "2"));

    // wait for the flush so the writer will refresh the table...
    Thread.sleep(2000);

    runTest();

    files = getDataFiles();
    assertThat(files).hasSize(1);
    assertEquals(4, files.get(0).recordCount());

    deleteFiles = getDeleteFiles();
    assertThat(deleteFiles).hasSize(1);
    assertEquals(2, deleteFiles.get(0).recordCount());
  }

  private void runTest() {
    // start with 3 records, update 1, delete 1. Should be a total of 4 adds and 2 deletes
    // (the update will be 1 add and 1 delete)

    String event1 =
        format(RECORD_FORMAT, 1, "type1", System.currentTimeMillis(), "hello world!", "I");
    String event2 =
        format(RECORD_FORMAT, 2, "type2", System.currentTimeMillis(), "having fun?", "I");

    long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();
    String event3 = format(RECORD_FORMAT, 3, "type3", threeDaysAgo, "hello from the past!", "I");

    String event4 =
        format(RECORD_FORMAT, 1, "type1", System.currentTimeMillis(), "hello world!", "D");
    String event5 = format(RECORD_FORMAT, 3, "type3", threeDaysAgo, "updated!", "U");

    producer.send(new ProducerRecord<>(TEST_TOPIC, event1));
    producer.send(new ProducerRecord<>(TEST_TOPIC, event2));
    producer.send(new ProducerRecord<>(TEST_TOPIC, event3));
    producer.send(new ProducerRecord<>(TEST_TOPIC, event4));
    producer.send(new ProducerRecord<>(TEST_TOPIC, event5));
    producer.flush();

    Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(this::assertSnapshotAdded);
  }

  private void assertSnapshotAdded() {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    assertThat(table.snapshots()).hasSize(1);
  }

  private List<DataFile> getDataFiles() {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    return Lists.newArrayList(table.currentSnapshot().addedDataFiles(table.io()));
  }

  private List<DeleteFile> getDeleteFiles() {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    return Lists.newArrayList(table.currentSnapshot().addedDeleteFiles(table.io()));
  }
}
