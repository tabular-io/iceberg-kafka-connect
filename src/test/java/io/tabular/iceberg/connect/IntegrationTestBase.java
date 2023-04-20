// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect;

import io.debezium.testing.testcontainers.DebeziumContainer;
import java.net.URI;
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
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
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
  protected static GenericContainer minio;

  protected static S3Client s3;
  protected static RESTCatalog restCatalog;
  protected static KafkaProducer<String, String> producer;

  private static final String LOCAL_JARS_DIR = "build/out";
  private static final String BUCKET = "bucket";

  protected static final String AWS_ACCESS_KEY = "minioadmin";
  protected static final String AWS_SECRET_KEY = "minioadmin";
  protected static final String AWS_REGION = "us-east-1";

  @BeforeAll
  public static void setupAll() {
    network = Network.newNetwork();

    minio =
        new GenericContainer(DockerImageName.parse("minio/minio"))
            .withNetwork(network)
            .withNetworkAliases("minio")
            .withExposedPorts(9000)
            .withCommand("server /data")
            .waitingFor(new HttpWaitStrategy().forPort(9000).forPath("/minio/health/ready"));

    catalog =
        new GenericContainer(DockerImageName.parse("tabulario/iceberg-rest"))
            .withNetwork(network)
            .withNetworkAliases("iceberg")
            .dependsOn(minio)
            .withExposedPorts(8181)
            .withEnv("CATALOG_WAREHOUSE", "s3://" + BUCKET + "/warehouse")
            .withEnv("CATALOG_IO__IMPL", S3FileIO.class.getName())
            .withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000")
            .withEnv("CATALOG_S3_ACCESS__KEY__ID", AWS_ACCESS_KEY)
            .withEnv("CATALOG_S3_SECRET__ACCESS__KEY", AWS_SECRET_KEY)
            .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
            .withEnv("AWS_REGION", AWS_REGION);

    kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka")).withNetwork(network);

    kafkaConnect =
        new DebeziumContainer(DockerImageName.parse("debezium/connect-base"))
            .withNetwork(network)
            .withKafka(kafka)
            .dependsOn(catalog, kafka)
            .withFileSystemBind(LOCAL_JARS_DIR, "/kafka/connect/test")
            .withEnv("OFFSET_FLUSH_INTERVAL_MS", "500");

    Startables.deepStart(Stream.of(minio, catalog, kafka, kafkaConnect)).join();

    s3 = initLocalS3Client();
    s3.createBucket(req -> req.bucket(BUCKET));
    restCatalog = initLocalCatalog();
    producer = initLocalProducer();
  }

  @AfterAll
  @SneakyThrows
  public static void teardownAll() {
    kafkaConnect.close();
    restCatalog.close();
    producer.close();
    kafka.close();
    catalog.close();
    s3.close();
    minio.close();
    network.close();
  }

  @SneakyThrows
  protected void createTopic(String topicName, int partitions) {
    try (AdminClient adminClient =
        AdminClient.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      adminClient
          .createTopics(List.of(new NewTopic(topicName, partitions, (short) 1)))
          .all()
          .get(10, TimeUnit.SECONDS);
    }
  }

  @SneakyThrows
  protected void deleteTopic(String topicName) {
    try (AdminClient adminClient =
        AdminClient.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      adminClient.deleteTopics(List.of(topicName)).all().get(10, TimeUnit.SECONDS);
    }
  }

  @SneakyThrows
  private static S3Client initLocalS3Client() {
    return S3Client.builder()
        .endpointOverride(new URI("http://localhost:" + minio.getMappedPort(9000)))
        .region(Region.of(AWS_REGION))
        .forcePathStyle(true)
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(AWS_ACCESS_KEY, AWS_SECRET_KEY)))
        .build();
  }

  private static RESTCatalog initLocalCatalog() {
    String localCatalogUri = "http://localhost:" + catalog.getMappedPort(8181);
    RESTCatalog result = new RESTCatalog();
    result.initialize(
        "local",
        Map.of(
            CatalogProperties.URI,
            localCatalogUri,
            CatalogProperties.FILE_IO_IMPL,
            S3FileIO.class.getName(),
            AwsProperties.HTTP_CLIENT_TYPE,
            AwsProperties.HTTP_CLIENT_TYPE_APACHE,
            AwsProperties.S3FILEIO_ENDPOINT,
            "http://localhost:" + minio.getMappedPort(9000),
            AwsProperties.S3FILEIO_ACCESS_KEY_ID,
            AWS_ACCESS_KEY,
            AwsProperties.S3FILEIO_SECRET_ACCESS_KEY,
            AWS_SECRET_KEY,
            AwsProperties.S3FILEIO_PATH_STYLE_ACCESS,
            "true"));
    return result;
  }

  private static KafkaProducer<String, String> initLocalProducer() {
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
