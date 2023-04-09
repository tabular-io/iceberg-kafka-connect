// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@SuppressWarnings("rawtypes")
public class IntegrationTest {

  private static Network network;
  private static KafkaContainer kafka;
  private static DebeziumContainer kafkaConnect;
  private static GenericContainer catalog;
  private static LocalStackContainer aws;
  private static S3Client s3;

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

  private static final String LOCAL_JARS_DIR = "build/out";
  private static final String BUCKET = "bucket";

  @BeforeAll
  public static void setupAll() throws Exception {
    network = Network.newNetwork();
    aws =
        new LocalStackContainer(DockerImageName.parse("localstack/localstack"))
            .withNetwork(network)
            .withNetworkAliases("aws")
            .withServices(Service.S3);
    catalog =
        new GenericContainer(DockerImageName.parse("tabulario/iceberg-rest"))
            .withNetwork(network)
            .withNetworkAliases("iceberg")
            .dependsOn(aws)
            .withExposedPorts(8181)
            .withEnv("CATALOG_WAREHOUSE", "s3://" + BUCKET + "/warehouse")
            .withEnv("CATALOG_IO__IMPL", S3FileIO.class.getName())
            .withEnv("CATALOG_S3_ENDPOINT", "http://aws:4566")
            .withEnv("CATALOG_S3_ACCESS__KEY__ID", aws.getAccessKey())
            .withEnv("CATALOG_S3_SECRET__ACCESS__KEY", aws.getSecretKey())
            .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
            .withEnv("AWS_REGION", aws.getRegion());

    kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka")).withNetwork(network);
    kafkaConnect =
        new DebeziumContainer(DockerImageName.parse("debezium/connect-base"))
            .withNetwork(network)
            .withKafka(kafka)
            .dependsOn(catalog, kafka)
            .withFileSystemBind(LOCAL_JARS_DIR, "/kafka/connect/" + CONNECTOR_NAME);

    Startables.deepStart(Stream.of(aws, catalog, kafka, kafkaConnect)).join();

    try (AdminClient adminClient =
        AdminClient.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      adminClient
          .createTopics(List.of(new NewTopic(TEST_TOPIC, 1, (short) 1)))
          .all()
          .get(30, TimeUnit.SECONDS);
    }

    s3 =
        S3Client.builder()
            .endpointOverride(aws.getEndpointOverride(Service.S3))
            .region(Region.of(aws.getRegion()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(aws.getAccessKey(), aws.getSecretKey())))
            .build();
    s3.createBucket(req -> req.bucket(BUCKET));
  }

  @AfterAll
  public static void teardownAll() {
    kafkaConnect.close();
    kafka.close();
    catalog.close();
    s3.close();
    aws.close();
    network.close();
  }

  @Test
  public void testIcebergSink() throws Exception {
    ConnectorConfiguration connector =
        ConnectorConfiguration.create()
            .with("topics", TEST_TOPIC)
            .with("connector.class", "io.tabular.connect.poc.IcebergSinkConnector")
            .with("tasks.max", "1")
            .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
            .with("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .with("iceberg.table", format("%s.%s", TEST_DB, TEST_TABLE)) // TODO
            .with("iceberg.catalog", RESTCatalog.class.getName())
            .with("iceberg.catalog." + CatalogProperties.URI, "http://iceberg:8181")
            .with(
                "iceberg.catalog." + AwsProperties.HTTP_CLIENT_TYPE,
                AwsProperties.HTTP_CLIENT_TYPE_APACHE)
            .with("iceberg.catalog." + AwsProperties.S3FILEIO_ENDPOINT, "http://aws:4566")
            .with("iceberg.catalog." + AwsProperties.S3FILEIO_ACCESS_KEY_ID, aws.getAccessKey())
            .with("iceberg.catalog." + AwsProperties.S3FILEIO_SECRET_ACCESS_KEY, aws.getSecretKey())
            .with("iceberg.catalog." + AwsProperties.S3FILEIO_PATH_STYLE_ACCESS, true)
            .with("iceberg.catalog." + AwsProperties.CLIENT_REGION, aws.getRegion());

    kafkaConnect.registerConnector(CONNECTOR_NAME, connector);

    try (RESTCatalog restCatalog = new RESTCatalog();
        KafkaProducer<String, String> producer =
            new KafkaProducer<>(
                Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    kafka.getBootstrapServers(),
                    ProducerConfig.CLIENT_ID_CONFIG,
                    UUID.randomUUID().toString()),
                new StringSerializer(),
                new StringSerializer())) {

      String localCatalogUri = "http://localhost:" + catalog.getMappedPort(8181);
      TableIdentifier tableIdentifier = TableIdentifier.of(TEST_DB, TEST_TABLE);
      restCatalog.initialize(
          "local",
          Map.of(
              CatalogProperties.URI, localCatalogUri,
              CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName(),
              AwsProperties.HTTP_CLIENT_TYPE, AwsProperties.HTTP_CLIENT_TYPE_APACHE,
              AwsProperties.S3FILEIO_ENDPOINT, aws.getEndpointOverride(Service.S3).toString(),
              AwsProperties.S3FILEIO_ACCESS_KEY_ID, aws.getAccessKey(),
              AwsProperties.S3FILEIO_SECRET_ACCESS_KEY, aws.getSecretKey(),
              AwsProperties.S3FILEIO_PATH_STYLE_ACCESS, "true",
              AwsProperties.CLIENT_REGION, aws.getRegion()));
      restCatalog.createNamespace(Namespace.of(TEST_DB));
      restCatalog.createTable(tableIdentifier, TEST_SCHEMA);

      String event = format(RECORD_FORMAT, 1, "hello world!", System.currentTimeMillis());
      producer.send(new ProducerRecord<>(TEST_TOPIC, "key1", event)).get();

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
      assertEquals(1, files.get(0).recordCount());
    }
  }
}
