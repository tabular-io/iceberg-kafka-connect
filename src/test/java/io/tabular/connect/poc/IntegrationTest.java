// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IntegrationTest extends IntegrationTestBase {

  private static final String CONNECTOR_NAME = "test_connector";
  private static final String TEST_TOPIC = "test_topic";
  private static final String TEST_DB = "default";
  private static final String TEST_TABLE = "foobar";
  private static final Schema TEST_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()),
          Types.NestedField.required(3, "ts", Types.TimestampType.withoutZone()));

  private static final String RECORD_FORMAT = "{\"id\":%d,\"data\":\"%s\",\"ts\":%d}";

  @BeforeEach
  public void setup() {
    createTopic(TEST_TOPIC);
    TableIdentifier tableIdentifier = TableIdentifier.of(TEST_DB, TEST_TABLE);
    restCatalog.createNamespace(Namespace.of(TEST_DB));
    restCatalog.createTable(tableIdentifier, TEST_SCHEMA);
  }

  @AfterEach
  public void teardown() {
    deleteTopic(TEST_TOPIC);
    restCatalog.dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE));
    restCatalog.dropNamespace(Namespace.of(TEST_DB));
  }

  @Test
  public void testIcebergSink() throws Exception {
    ConnectorConfiguration connectorConfig =
        ConnectorConfiguration.create()
            .with("topics", TEST_TOPIC)
            .with("connector.class", "io.tabular.connect.poc.IcebergSinkConnector")
            .with("tasks.max", 1)
            .with("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter")
            .with("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter")
            .with("iceberg.table", format("%s.%s", TEST_DB, TEST_TABLE))
            .with("iceberg.table.commitIntervalMs", 0) // commit immediately
            .with("iceberg.catalog", RESTCatalog.class.getName())
            .with("iceberg.catalog." + CatalogProperties.URI, "http://iceberg:8181")
            .with(
                "iceberg.catalog." + AwsProperties.HTTP_CLIENT_TYPE,
                AwsProperties.HTTP_CLIENT_TYPE_APACHE)
            .with("iceberg.catalog." + AwsProperties.S3FILEIO_ENDPOINT, "http://minio:9000")
            .with("iceberg.catalog." + AwsProperties.S3FILEIO_ACCESS_KEY_ID, AWS_ACCESS_KEY)
            .with("iceberg.catalog." + AwsProperties.S3FILEIO_SECRET_ACCESS_KEY, AWS_SECRET_KEY)
            .with("iceberg.catalog." + AwsProperties.S3FILEIO_PATH_STYLE_ACCESS, true)
            .with("iceberg.catalog." + AwsProperties.CLIENT_REGION, AWS_REGION);

    kafkaConnect.registerConnector(CONNECTOR_NAME, connectorConfig);

    String event1 = format(RECORD_FORMAT, 1, "hello world!", System.currentTimeMillis());
    String event2 = format(RECORD_FORMAT, 2, "foo bar", System.currentTimeMillis());
    Future<RecordMetadata> f1 = producer.send(new ProducerRecord<>(TEST_TOPIC, event1));
    Future<RecordMetadata> f2 = producer.send(new ProducerRecord<>(TEST_TOPIC, event2));
    f1.get();
    f2.get();

    TableIdentifier tableIdentifier = TableIdentifier.of(TEST_DB, TEST_TABLE);
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Table table = restCatalog.loadTable(tableIdentifier);
              assertThat(table.snapshots()).hasSize(1);
            });

    Table table = restCatalog.loadTable(tableIdentifier);
    List<DataFile> files = Lists.newArrayList(table.currentSnapshot().addedDataFiles(table.io()));
    assertThat(files).hasSize(1);
    assertEquals(2, files.get(0).recordCount());
  }
}
