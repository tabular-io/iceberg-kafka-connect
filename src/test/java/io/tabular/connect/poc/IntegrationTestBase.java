// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import io.debezium.testing.testcontainers.DebeziumContainer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
public class IntegrationTestBase {

  protected static Network network;
  protected static KafkaContainer kafka;
  protected static DebeziumContainer kafkaConnect;
  protected static GenericContainer catalog;
  protected static LocalStackContainer aws;
  protected static S3Client s3;

  private static final String LOCAL_JARS_DIR = "build/out";
  private static final String BUCKET = "bucket";

  @BeforeAll
  public static void setupAll() {
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
            .withFileSystemBind(LOCAL_JARS_DIR, "/kafka/connect/test");

    Startables.deepStart(Stream.of(aws, catalog, kafka, kafkaConnect)).join();

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

  @SneakyThrows
  protected void createTopic(String topicName) {
    try (AdminClient adminClient =
        AdminClient.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      adminClient
          .createTopics(List.of(new NewTopic(topicName, 1, (short) 1)))
          .all()
          .get(10, TimeUnit.SECONDS);
    }
  }

  protected RESTCatalog initLocalCatalog() {
    String localCatalogUri = "http://localhost:" + catalog.getMappedPort(8181);
    RESTCatalog result = new RESTCatalog();
    result.initialize(
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
    return result;
  }

  protected KafkaProducer<String, String> initLocalProducer() {
    return new KafkaProducer<>(
        Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            kafka.getBootstrapServers(),
            ProducerConfig.CLIENT_ID_CONFIG,
            UUID.randomUUID().toString()),
        new StringSerializer(),
        new StringSerializer());
  }
}
