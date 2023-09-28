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
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@SuppressWarnings("rawtypes")
public final class TestContextUtil {
  private static final String LOCAL_INSTALL_DIR = "build/install";
  private static final String KC_PLUGIN_DIR = "/test/kafka-connect";
  private static final String MINIO_IMAGE = "minio/minio";
  private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.4.1";
  private static final String CONNECT_IMAGE = "confluentinc/cp-kafka-connect:7.4.1";
  private static final String REST_CATALOG_IMAGE = "tabulario/iceberg-rest:0.6.0";
  private static final String NESSIE_CATALOG_IMAGE = "projectnessie/nessie:0.59.0";

  public static final int MINIO_PORT = 9000;
  public static final int REST_CATALOG_PORT = 8181;
  public static final int NESSIE_CATALOG_PORT = 19121;

  private TestContextUtil() {}

  static GenericContainer minioContainer(Network network) {
    return new GenericContainer<>(DockerImageName.parse(MINIO_IMAGE))
        .withNetwork(network)
        .withNetworkAliases("minio")
        .withExposedPorts(MINIO_PORT)
        .withCommand("server /data")
        .waitingFor(new HttpWaitStrategy().forPort(MINIO_PORT).forPath("/minio/health/ready"));
  }

  static KafkaContainer kafkaContainer(Network network) {
    return new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE)).withNetwork(network);
  }

  static KafkaConnectContainer kafkaConnectContainer(
      Network network,
      GenericContainer restCatalog,
      GenericContainer nessieCatalog,
      KafkaContainer kafka) {
    return new KafkaConnectContainer(DockerImageName.parse(CONNECT_IMAGE))
        .withNetwork(network)
        .dependsOn(restCatalog, nessieCatalog, kafka)
        .withFileSystemBind(LOCAL_INSTALL_DIR, KC_PLUGIN_DIR)
        .withEnv("CONNECT_PLUGIN_PATH", KC_PLUGIN_DIR)
        .withEnv("CONNECT_BOOTSTRAP_SERVERS", kafka.getNetworkAliases().get(0) + ":9092")
        .withEnv("CONNECT_OFFSET_FLUSH_INTERVAL_MS", "500");
  }

  static S3Client initLocalS3Client(int mappedPort) {
    try {
      return S3Client.builder()
          .endpointOverride(new URI("http://localhost:" + mappedPort))
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

  static KafkaProducer<String, String> initLocalProducer(String bootstrapServers) {
    return new KafkaProducer<>(
        ImmutableMap.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers,
            ProducerConfig.CLIENT_ID_CONFIG,
            UUID.randomUUID().toString()),
        new StringSerializer(),
        new StringSerializer());
  }

  static Admin initLocalAdmin(String bootstrapServers) {
    return Admin.create(
        ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
  }

  static GenericContainer restCatalogContainer(Network network, GenericContainer minio) {
    return new GenericContainer<>(DockerImageName.parse(REST_CATALOG_IMAGE))
        .withNetwork(network)
        .withNetworkAliases("rest")
        .dependsOn(minio)
        .withExposedPorts(REST_CATALOG_PORT)
        .withEnv("CATALOG_WAREHOUSE", "s3://" + BUCKET + "/warehouse")
        .withEnv("CATALOG_IO__IMPL", S3FileIO.class.getName())
        .withEnv("CATALOG_S3_ENDPOINT", "http://minio:" + MINIO_PORT)
        .withEnv("CATALOG_S3_ACCESS__KEY__ID", AWS_ACCESS_KEY)
        .withEnv("CATALOG_S3_SECRET__ACCESS__KEY", AWS_SECRET_KEY)
        .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
        .withEnv("AWS_REGION", AWS_REGION);
  }

  static GenericContainer nessieCatalogContainer(Network network, GenericContainer minio) {
    return new GenericContainer<>(DockerImageName.parse(NESSIE_CATALOG_IMAGE))
        .withNetwork(network)
        .withNetworkAliases("nessie")
        .dependsOn(minio)
        .withExposedPorts(19121)
        .withEnv("QUARKUS_HTTP_PORT", String.valueOf(NESSIE_CATALOG_PORT))
        .withEnv("NESSIE_VERSION_STORE_TYPE", "INMEMORY")
        .withEnv("CATALOG_WAREHOUSE", "s3://" + BUCKET + "/warehouse")
        .withEnv("CATALOG_IO__IMPL", S3FileIO.class.getName())
        .withEnv("CATALOG_S3_ENDPOINT", "http://minio:" + MINIO_PORT)
        .withEnv("CATALOG_S3_ACCESS__KEY__ID", AWS_ACCESS_KEY)
        .withEnv("CATALOG_S3_SECRET__ACCESS__KEY", AWS_SECRET_KEY)
        .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
        .withEnv("AWS_REGION", AWS_REGION);
  }

  static Map<String, Object> connectorRestCatalogProperties() {
    return ImmutableMap.<String, Object>builder()
        .put(
            "iceberg.catalog." + CatalogUtil.ICEBERG_CATALOG_TYPE,
            CatalogUtil.ICEBERG_CATALOG_TYPE_REST)
        .put("iceberg.catalog." + CatalogProperties.URI, "http://rest:" + REST_CATALOG_PORT)
        .put("iceberg.catalog." + S3FileIOProperties.ENDPOINT, "http://minio:" + MINIO_PORT)
        .put("iceberg.catalog." + S3FileIOProperties.ACCESS_KEY_ID, AWS_ACCESS_KEY)
        .put("iceberg.catalog." + S3FileIOProperties.SECRET_ACCESS_KEY, AWS_SECRET_KEY)
        .put("iceberg.catalog." + S3FileIOProperties.PATH_STYLE_ACCESS, true)
        .put("iceberg.catalog." + AwsClientProperties.CLIENT_REGION, AWS_REGION)
        .build();
  }

  static Map<String, Object> connectorNessieCatalogProperties() {
    return ImmutableMap.<String, Object>builder()
        .put("iceberg.catalog.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .put(
            "iceberg.catalog." + CatalogProperties.URI,
            "http://nessie:" + NESSIE_CATALOG_PORT + "/api/v1")
        .put(
            "iceberg.catalog." + CatalogProperties.WAREHOUSE_LOCATION,
            "s3://" + BUCKET + "/warehouse")
        .put("iceberg.catalog." + CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName())
        .put("iceberg.catalog." + S3FileIOProperties.ENDPOINT, "http://minio:" + MINIO_PORT)
        .put("iceberg.catalog." + S3FileIOProperties.ACCESS_KEY_ID, AWS_ACCESS_KEY)
        .put("iceberg.catalog." + S3FileIOProperties.SECRET_ACCESS_KEY, AWS_SECRET_KEY)
        .put("iceberg.catalog." + S3FileIOProperties.PATH_STYLE_ACCESS, true)
        .put("iceberg.catalog." + AwsClientProperties.CLIENT_REGION, AWS_REGION)
        .build();
  }
}
