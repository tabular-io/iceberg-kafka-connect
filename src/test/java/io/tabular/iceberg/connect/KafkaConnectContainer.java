// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect;

import static java.lang.String.format;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.SneakyThrows;
import lombok.extern.jackson.Jacksonized;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

public class KafkaConnectContainer extends GenericContainer<KafkaConnectContainer> {

  private static final HttpClient HTTP = HttpClient.newHttpClient();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int PORT = 8083;

  @Jacksonized
  @Builder
  @Getter
  public static class Config {

    @NonNull private String name;

    @Singular("config")
    private Map<String, Object> config;
  }

  public KafkaConnectContainer(DockerImageName dockerImageName) {
    super(dockerImageName);
    this.withExposedPorts(PORT);
    this.withEnv("CONNECT_GROUP_ID", "kc");
    this.withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "kc_config");
    this.withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
    this.withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "kc_offsets");
    this.withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
    this.withEnv("CONNECT_STATUS_STORAGE_TOPIC", "kc_status");
    this.withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
    this.withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
    this.withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
    this.withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
    this.withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");
    this.withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost");
    this.setWaitStrategy(
        new HttpWaitStrategy()
            .forPath("/connectors")
            .forPort(PORT)
            .withStartupTimeout(Duration.ofSeconds(30)));
  }

  @SneakyThrows
  public void registerConnector(Config config) {
    URI uri = new URI(format("http://localhost:%d/connectors", getMappedPort(PORT)));
    String body = MAPPER.writeValueAsString(config);
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(uri)
            .header("Content-Type", "application/json")
            .POST(BodyPublishers.ofString(body))
            .build();
    HTTP.send(request, BodyHandlers.discarding());
  }

  @SneakyThrows
  public void ensureConnectorRunning(String name) {
    URI uri =
        new URI(format("http://localhost:%d/connectors/%s/status", getMappedPort(PORT), name));
    HttpRequest request = HttpRequest.newBuilder().uri(uri).build();
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .until(
            () -> {
              HttpResponse<String> response = HTTP.send(request, BodyHandlers.ofString());
              if (response.statusCode() == 200) {
                JsonNode root = MAPPER.readTree(response.body());
                return "RUNNING".equals(root.get("connector").get("state").asText());
              }
              return false;
            });
  }
}
