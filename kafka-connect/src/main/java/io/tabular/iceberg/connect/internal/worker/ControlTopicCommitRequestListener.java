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

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.events.CommitRequestPayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventType;
import io.tabular.iceberg.connect.internal.kafka.Factory;
import io.tabular.iceberg.connect.internal.kafka.KafkaUtils;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Listens for commit requests on the Control topic. */
class ControlTopicCommitRequestListener implements CommitRequestListener {

  private static final Logger LOG =
      LoggerFactory.getLogger(ControlTopicCommitRequestListener.class);
  private final IcebergSinkConfig config;
  private final Consumer<String, byte[]> consumer;

  ControlTopicCommitRequestListener(
      IcebergSinkConfig config, Factory<Consumer<String, byte[]>> consumerFactory) {
    this.config = config;

    // pass transient consumer group ID to which we never commit offsets
    Map<String, String> consumerProps = Maps.newHashMap(config.controlClusterKafkaProps());
    consumerProps.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        IcebergSinkConfig.DEFAULT_CONTROL_GROUP_PREFIX + UUID.randomUUID());
    this.consumer = consumerFactory.create(consumerProps);
    consumer.subscribe(ImmutableList.of(config.controlTopic()));
    // TODO: previously there was a long poll here to "initialize subscription." I don't think this
    // is necessary anymore (i believe it was in the past).
    // processControlEvents(Duration.ofMillis(1000));
  }

  // logical change here is we respond to the latest commit request rather than every commit request
  // this does mean we block now until we've caught up on control topic
  // this should not be an issue unless control topic is extremely busy (which in itself is a
  // problem)
  // i could write logic to exit early if we see a commitId as well
  // i could even write logic to exit as oon as we see a commitId, and store buffer, before moving
  // on to next commitId
  // but i feel all of that is unnecessary
  @Override
  public Optional<UUID> getCommitId() {
    final AtomicReference<UUID> latestCommitId = new AtomicReference<>();

    KafkaUtils.consumeAvailable(
        consumer,
        Duration.ZERO,
        envelope -> {
          Event event = envelope.event();
          if (event.groupId().equals(config.controlGroupId())) {
            // TODO: improve logging
            LOG.debug("Received event of type: {}", event.type().name());
            if (event.type() == EventType.COMMIT_REQUEST) {
              latestCommitId.set(((CommitRequestPayload) event.payload()).commitId());
              LOG.info("Handled event of type: {}", event.type().name());
            }
          }
        });

    return Optional.ofNullable(latestCommitId.get());
  }

  @Override
  public void close() throws IOException {
    consumer.close();
  }
}
