// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
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
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

@SuppressWarnings("rawtypes")
public class IntegrationTest {

  private static Network network;
  private static KafkaContainer kafka;
  private static DebeziumContainer kafkaConnect;
  private static GenericContainer catalog;

  private static final String CONNECTOR_NAME = "test_connector";
  private static final String TEST_TOPIC = "test_topic";
  private static final String TEST_FILE = "test.txt";
  private static final String LOCAL_JARS_DIR = "build/out";
  private static final String LOCAL_OUTPUT_DIR = "build/output";
  private static final String REMOTE_OUTPUT_DIR = "/output";

  @BeforeAll
  public static void setupAll() throws Exception {
    network = Network.newNetwork();

    catalog =
        new GenericContainer(DockerImageName.parse("tabulario/iceberg-rest"))
            .withNetwork(network)
            .withNetworkAliases("iceberg");

    catalog.start();

    kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka")).withNetwork(network);
    kafkaConnect =
        new DebeziumContainer(DockerImageName.parse("debezium/connect-base"))
            .withNetwork(network)
            .withKafka(kafka)
            .dependsOn(kafka)
            .withFileSystemBind(LOCAL_JARS_DIR, "/kafka/connect/" + CONNECTOR_NAME)
            .withFileSystemBind(LOCAL_OUTPUT_DIR, REMOTE_OUTPUT_DIR);
    Startables.deepStart(Stream.of(kafka, kafkaConnect)).join();

    try (AdminClient adminClient =
        AdminClient.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      adminClient
          .createTopics(List.of(new NewTopic(TEST_TOPIC, 1, (short) 1)))
          .all()
          .get(30, TimeUnit.SECONDS);
    }
  }

  @AfterAll
  public static void teardownAll() {
    kafkaConnect.close();
    kafka.close();
    network.close();
    catalog.close();
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
            .with("output", REMOTE_OUTPUT_DIR + "/" + TEST_FILE);

    kafkaConnect.registerConnector(CONNECTOR_NAME, connector);
    // kafkaConnect.ensureConnectorTaskState(CONNECTOR_NAME, 0, State.RUNNING);

    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                ProducerConfig.CLIENT_ID_CONFIG,
                UUID.randomUUID().toString()),
            new StringSerializer(),
            new StringSerializer())) {
      producer.send(new ProducerRecord<>(TEST_TOPIC, "foo", "bar")).get();

      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(() -> assertTrue(Files.exists(Path.of(LOCAL_OUTPUT_DIR, TEST_FILE))));
    }
  }
}
