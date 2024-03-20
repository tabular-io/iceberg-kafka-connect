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
package io.tabular.iceberg.connect.internal.worker;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.events.EventType;
import io.tabular.iceberg.connect.internal.coordinator.Envelope;
import io.tabular.iceberg.connect.internal.data.IcebergWriterFactoryImpl;
import io.tabular.iceberg.connect.internal.kafka.AdminFactory;
import io.tabular.iceberg.connect.internal.kafka.ConsumerFactory;
import io.tabular.iceberg.connect.internal.kafka.KafkaUtils;
import io.tabular.iceberg.connect.internal.kafka.TransactionalProducerFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

class PartitionWorkerZombieFencingTest {

  private static final String SOURCE_TOPIC = "source-topic-name";
  private static final String CONTROL_TOPIC = "control-topic-name";
  private InMemoryCatalog inMemoryCatalog;
  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final String TABLE_1_NAME = "db.tbl1";
  private static final TableIdentifier TABLE_1_IDENTIFIER = TableIdentifier.parse(TABLE_1_NAME);
  private static final String ID_FIELD_NAME = "id";
  private static final Schema SCHEMA =
      new Schema(required(1, ID_FIELD_NAME, Types.StringType.get()));

  private static final String CONNECTOR_NAME = "connector-name";

  private static final int PARTITION = 0;
  private static final TopicPartition SOURCE_TOPIC_PARTITION =
      new TopicPartition(SOURCE_TOPIC, PARTITION);

  static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.5.1";
  private KafkaContainer kafka;

  @BeforeEach
  public void before() {
    inMemoryCatalog = new InMemoryCatalog();
    inMemoryCatalog.initialize(null, ImmutableMap.of());
    inMemoryCatalog.createNamespace(NAMESPACE);

    kafka = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE));
    kafka.start();
  }

  @AfterEach
  public void after() throws IOException {
    inMemoryCatalog.close();
    kafka.stop();
  }

  private SinkRecord makeSinkRecord(long offset, Long timestamp) {
    return new SinkRecord(
        SOURCE_TOPIC_PARTITION.topic(),
        SOURCE_TOPIC_PARTITION.partition(),
        null,
        null,
        null,
        ImmutableMap.of(ID_FIELD_NAME, "field_value"),
        offset,
        timestamp,
        TimestampType.LOG_APPEND_TIME);
  }

  @Test
  public void testOlderWorkerHandlingSameTopicPartitionShouldBeFencedOut()
      throws ExecutionException, InterruptedException {
    inMemoryCatalog.createTable(TABLE_1_IDENTIFIER, SCHEMA);

    final SinkRecord sinkRecord = makeSinkRecord(0L, 1L);

    IcebergSinkConfig configs =
        new IcebergSinkConfig(
            ImmutableMap.of(
                "name",
                CONNECTOR_NAME,
                // TODO: add everywhere
                "topics",
                SOURCE_TOPIC,
                "iceberg.catalog.catalog-impl",
                "org.apache.iceberg.inmemory.InMemoryCatalog",
                "iceberg.tables",
                TABLE_1_NAME,
                "iceberg.control.topic",
                CONTROL_TOPIC,
                String.format("iceberg.kafka.%s", CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
                kafka.getBootstrapServers(),
                String.format("iceberg.kafka.%s", ProducerConfig.TRANSACTION_TIMEOUT_CONFIG),
                "5000",
                String.format("iceberg.kafka.%s", ProducerConfig.MAX_BLOCK_MS_CONFIG),
                "1000"));

    try (final Admin admin = new AdminFactory().create(configs.sourceClusterKafkaProps())) {
      admin
          .createTopics(
              ImmutableList.of(
                  new NewTopic(SOURCE_TOPIC, 1, (short) 1),
                  new NewTopic(CONTROL_TOPIC, 1, (short) 1)))
          .all()
          .get();
    }

    final Worker worker1 =
        new PartitionWorker(
            configs,
            SOURCE_TOPIC_PARTITION,
            new IcebergWriterFactoryImpl(inMemoryCatalog, configs),
            new TransactionalProducerFactory());
    worker1.save(ImmutableList.of(sinkRecord));

    final Worker worker2 =
        new PartitionWorker(
            configs,
            SOURCE_TOPIC_PARTITION,
            new IcebergWriterFactoryImpl(inMemoryCatalog, configs),
            new TransactionalProducerFactory());
    // at this point, worker 1 is fenced
    worker2.save(ImmutableList.of(sinkRecord));

    final UUID commitId = UUID.randomUUID();

    AtomicBoolean worker1WasFenced = new AtomicBoolean(false);
    try {
      worker1.commit(commitId);
    } catch (ProducerFencedException e) {
      worker1WasFenced.set(true);
    }
    worker2.commit(commitId);

    assertThat(worker1WasFenced.get()).isTrue();

    try (final Consumer<String, byte[]> consumer =
        new ConsumerFactory()
            .create(
                ImmutableMap.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                    ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
      consumer.subscribe(ImmutableList.of(CONTROL_TOPIC));
      List<Envelope> messages = Lists.newArrayList();
      KafkaUtils.consumeAvailable(consumer, Duration.ofSeconds(1), messages::add);

      assertThat(messages).hasSize(2);
      assertThat(messages.get(0).event().type()).isEqualTo(EventType.COMMIT_RESPONSE);
      assertThat(messages.get(1).event().type()).isEqualTo(EventType.COMMIT_READY);
    }
  }

  // TODO: do not fence workers handling different topic partitions
}
