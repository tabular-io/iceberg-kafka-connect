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

import static io.tabular.iceberg.connect.TestConstants.AWS_ACCESS_KEY;
import static io.tabular.iceberg.connect.TestConstants.AWS_REGION;
import static io.tabular.iceberg.connect.TestConstants.AWS_SECRET_KEY;
import static io.tabular.iceberg.connect.TestConstants.BUCKET;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
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
public class TestContext {

  public static TestContext INSTANCE = new TestContext();

  private final Network network;
  private final KafkaContainer kafka;
  private final KafkaConnectContainer kafkaConnect;
  private final GenericContainer restCatalog;
  private final GenericContainer minio;

  private static final String LOCAL_INSTALL_DIR = "build/install";
  private static final String KC_PLUGIN_DIR = "/test/kafka-connect";

  private static final String MINIO_IMAGE = "minio/minio";
  private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.4.1";
  private static final String CONNECT_IMAGE = "confluentinc/cp-kafka-connect:7.4.1";
  private static final String REST_CATALOG_IMAGE = "tabulario/iceberg-rest:0.6.0";

  private TestContext() {
    network = Network.newNetwork();

    minio =
        new GenericContainer<>(DockerImageName.parse(MINIO_IMAGE))
            .withNetwork(network)
            .withNetworkAliases("minio")
            .withExposedPorts(9000)
            .withCommand("server /data")
            .waitingFor(new HttpWaitStrategy().forPort(9000).forPath("/minio/health/ready"));

    restCatalog =
        new GenericContainer<>(DockerImageName.parse(REST_CATALOG_IMAGE))
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

    kafka = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE)).withNetwork(network);

    kafkaConnect =
        new KafkaConnectContainer(DockerImageName.parse(CONNECT_IMAGE))
            .withNetwork(network)
            .dependsOn(restCatalog, kafka)
            .withFileSystemBind(LOCAL_INSTALL_DIR, KC_PLUGIN_DIR)
            .withEnv("CONNECT_PLUGIN_PATH", KC_PLUGIN_DIR)
            .withEnv("CONNECT_BOOTSTRAP_SERVERS", kafka.getNetworkAliases().get(0) + ":9092")
            .withEnv("CONNECT_OFFSET_FLUSH_INTERVAL_MS", "500");

    Startables.deepStart(Stream.of(minio, restCatalog, kafka, kafkaConnect)).join();

    try (S3Client s3 = initLocalS3Client()) {
      s3.createBucket(req -> req.bucket(BUCKET));
    }

    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
  }

  private void shutdown() {
    kafkaConnect.close();
    kafka.close();
    restCatalog.close();
    minio.close();
    network.close();
  }

  private int getLocalMinioPort() {
    return minio.getMappedPort(9000);
  }

  private int getLocalCatalogPort() {
    return restCatalog.getMappedPort(8181);
  }

  private String getLocalBootstrapServers() {
    return kafka.getBootstrapServers();
  }

  public void startConnector(KafkaConnectContainer.Config config) {
    kafkaConnect.startConnector(config);
    kafkaConnect.ensureConnectorRunning(config.getName());
  }

  public void stopConnector(String name) {
    kafkaConnect.stopConnector(name);
  }

  public S3Client initLocalS3Client() {
    try {
      return S3Client.builder()
          .endpointOverride(new URI("http://localhost:" + getLocalMinioPort()))
          .region(Region.of(AWS_REGION))
          .forcePathStyle(true)
          .credentialsProvider(
              StaticCredentialsProvider.create(
                  AwsBasicCredentials.create(AWS_ACCESS_KEY, AWS_SECRET_KEY)))
          .build();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public RESTCatalog initLocalCatalog() {
    String localCatalogUri = "http://localhost:" + getLocalCatalogPort();
    RESTCatalog result = new RESTCatalog();
    result.initialize(
        "local",
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, localCatalogUri)
            .put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName())
            .put(S3FileIOProperties.ENDPOINT, "http://localhost:" + getLocalMinioPort())
            .put(S3FileIOProperties.ACCESS_KEY_ID, AWS_ACCESS_KEY)
            .put(S3FileIOProperties.SECRET_ACCESS_KEY, AWS_SECRET_KEY)
            .put(S3FileIOProperties.PATH_STYLE_ACCESS, "true")
            .build());
    return result;
  }

  public KafkaProducer<String, String> initLocalProducer() {
    return new KafkaProducer<>(
        ImmutableMap.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            getLocalBootstrapServers(),
            ProducerConfig.CLIENT_ID_CONFIG,
            UUID.randomUUID().toString()),
        new StringSerializer(),
        new StringSerializer());
  }

  public Admin initLocalAdmin() {
    return Admin.create(
        ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getLocalBootstrapServers()));
  }
}
