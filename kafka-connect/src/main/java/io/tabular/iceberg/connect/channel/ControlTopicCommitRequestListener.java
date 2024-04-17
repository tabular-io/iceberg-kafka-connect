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

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.events.CommitRequestPayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventType;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Listens for commit requests from the {@link Coordinator} on the control topic. */
class ControlTopicCommitRequestListener implements CommitRequestListener, AutoCloseable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ControlTopicCommitRequestListener.class);
  private final IcebergSinkConfig config;
  private final Consumer<String, byte[]> consumer;
  private boolean isFirstPoll = true;

  ControlTopicCommitRequestListener(
      IcebergSinkConfig config, KafkaClientFactory kafkaClientFactory) {
    this.config = config;
    // use a transient consumer group ID to which we never commit offsets
    this.consumer =
        kafkaClientFactory.createConsumer(
            IcebergSinkConfig.DEFAULT_CONTROL_GROUP_PREFIX + UUID.randomUUID());
    consumer.subscribe(ImmutableList.of(config.controlTopic()));
  }

  // initial poll with longer duration so the consumer will initialize...
  private Duration pollDuration() {
    final Duration duration;
    if (isFirstPoll) {
      isFirstPoll = false;
      duration = Duration.ofMillis(1000);
    } else {
      duration = Duration.ZERO;
    }
    return duration;
  }

  @Override
  public Optional<UUID> getCommitId() {
    AtomicReference<UUID> latestCommitId = new AtomicReference<>();

    KafkaUtils.consumeAvailable(
        consumer,
        pollDuration(),
        record -> {
          Event event = Event.decode(record.value());
          if (event.groupId().equals(config.controlGroupId())) {
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
