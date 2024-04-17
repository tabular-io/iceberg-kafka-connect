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
package io.tabular.iceberg.connect.channel;

import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;

class MockKafkaClientFactory extends KafkaClientFactory {

  private final MockConsumer<String, byte[]> consumer;
  private final Pair<UUID, MockProducer<String, byte[]>> producerPair;
  private final Admin admin;

  MockKafkaClientFactory(
      MockConsumer<String, byte[]> consumer,
      Pair<UUID, MockProducer<String, byte[]>> producerPair,
      Admin admin) {
    super(ImmutableMap.of());
    this.consumer = consumer;
    this.producerPair = producerPair;
    this.admin = admin;
  }

  @Override
  public Pair<UUID, Producer<String, byte[]>> createProducer(String transactionalId) {
    return Pair.of(producerPair.first(), producerPair.second());
  }

  @Override
  public Consumer<String, byte[]> createConsumer(String consumerGroupId) {
    return consumer;
  }

  @Override
  public Admin createAdmin() {
    return admin;
  }
}
