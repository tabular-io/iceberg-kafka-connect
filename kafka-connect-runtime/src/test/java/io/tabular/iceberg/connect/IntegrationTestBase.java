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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.services.s3.S3Client;

public class IntegrationTestBase {

  protected final TestContext context = TestContext.INSTANCE;
  protected final AtomicInteger cnt = new AtomicInteger(0);

  protected S3Client s3;
  protected HiveCatalog catalog;
  protected Admin admin;

  private KafkaProducer<String, String> producer;

  @BeforeEach
  public void baseSetup() {
    s3 = context.initLocalS3Client();
    catalog = context.initLocalCatalog();
    producer = context.initLocalProducer();
    admin = context.initLocalAdmin();
  }

  @AfterEach
  public void baseTeardown() {
    producer.close();
    admin.close();
    s3.close();
  }

  protected void assertSnapshotProps(TableIdentifier tableIdentifier) {
    Map<String, String> props = catalog.loadTable(tableIdentifier).currentSnapshot().summary();
    assertThat(props)
        .hasKeySatisfying(
            new Condition<String>() {
              @Override
              public boolean matches(String str) {
                return str.startsWith("kafka.connect.offsets.");
              }
            });
    assertThat(props).containsKey("kafka.connect.commitId");
    assertThat(props).containsKey("kafka.connect.vtts");
  }

  protected void createTopic(String topicName, int partitions) {
    try {
      admin
          .createTopics(ImmutableList.of(new NewTopic(topicName, partitions, (short) 1)))
          .all()
          .get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  protected void deleteTopic(String topicName) {
    try {
      admin.deleteTopics(ImmutableList.of(topicName)).all().get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  protected void send(String topicName, int totalPartitions, String event) {
    producer.send(
        new ProducerRecord<>(topicName, cnt.getAndIncrement() % totalPartitions, null, event));
  }

  protected void flush() {
    producer.flush();
  }
}
