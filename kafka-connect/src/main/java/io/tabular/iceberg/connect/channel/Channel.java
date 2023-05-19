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

import static java.util.stream.Collectors.toList;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.channel.events.Event;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Channel.class);

  protected final Map<String, String> kafkaProps;
  private final String controlTopic;
  private final String controlGroupId;
  private final String transactionalId;
  private final KafkaProducer<String, byte[]> producer;
  private final KafkaConsumer<String, byte[]> consumer;
  private final Admin admin;
  private final Map<Integer, Long> controlTopicOffsets = new HashMap<>();
  private final String producerId;

  public Channel(String name, String consumerGroupId, IcebergSinkConfig config) {
    this.kafkaProps = config.getKafkaProps();
    this.controlTopic = config.getControlTopic();
    this.controlGroupId = config.getControlGroupId();
    this.transactionalId = name + config.getTransactionalSuffix();
    this.producer = createProducer();
    this.consumer = createConsumer(consumerGroupId);
    this.admin = createAdmin();
    this.producerId = UUID.randomUUID().toString();
  }

  protected void send(Event event) {
    send(ImmutableList.of(event), ImmutableMap.of());
  }

  protected void send(List<Event> events, Map<TopicPartition, Long> sourceOffsets) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    sourceOffsets.forEach((k, v) -> offsetsToCommit.put(k, new OffsetAndMetadata(v)));

    List<ProducerRecord<String, byte[]>> recordList =
        events.stream()
            .map(
                event -> {
                  LOG.info("Sending event of type: {}", event.getType().name());
                  try {
                    byte[] data = AvroEncoderUtil.encode(event, event.getSchema());
                    // key by producer ID to keep event order
                    return new ProducerRecord<>(controlTopic, producerId, data);
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
            .collect(toList());

    producer.beginTransaction();
    try {
      recordList.forEach(producer::send);
      if (!sourceOffsets.isEmpty()) {
        // TODO: this doesn't fence zombies
        producer.sendOffsetsToTransaction(
            offsetsToCommit, new ConsumerGroupMetadata(controlGroupId));
      }
      producer.commitTransaction();
    } catch (Exception e) {
      producer.abortTransaction();
      throw e;
    }
  }

  protected abstract boolean receive(Envelope envelope);

  public void process() {
    consumeAvailable(this::receive, Duration.ZERO);
  }

  public void process(Duration pollDuration) {
    consumeAvailable(this::receive, pollDuration);
  }

  protected void consumeAvailable(Function<Envelope, Boolean> eventHandler, Duration pollDuration) {
    ConsumerRecords<String, byte[]> records = consumer.poll(pollDuration);
    while (!records.isEmpty()) {
      records.forEach(
          record -> {
            // the consumer stores the offsets that corresponds to the next record to consume,
            // so increment the record offset by one
            controlTopicOffsets.put(record.partition(), record.offset() + 1);

            Event event;
            try {
              event = AvroEncoderUtil.decode(record.value());
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }

            LOG.debug("Received event of type: {}", event.getType().name());
            if (eventHandler.apply(new Envelope(event, record.partition(), record.offset()))) {
              LOG.info("Handled event of type: {}", event.getType().name());
            }
          });
      records = consumer.poll(pollDuration);
    }
  }

  protected Map<Integer, Long> getControlTopicOffsets() {
    return controlTopicOffsets;
  }

  private KafkaProducer<String, byte[]> createProducer() {
    Map<String, Object> producerProps = new HashMap<>(kafkaProps);
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    KafkaProducer<String, byte[]> result =
        new KafkaProducer<>(producerProps, new StringSerializer(), new ByteArraySerializer());
    result.initTransactions();
    return result;
  }

  private KafkaConsumer<String, byte[]> createConsumer(String consumerGroupId) {
    Map<String, Object> consumerProps = new HashMap<>(kafkaProps);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    return new KafkaConsumer<>(
        consumerProps, new StringDeserializer(), new ByteArrayDeserializer());
  }

  protected void commitConsumerOffsets() {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    getControlTopicOffsets()
        .forEach(
            (k, v) ->
                offsetsToCommit.put(new TopicPartition(controlTopic, k), new OffsetAndMetadata(v)));
    consumer.commitSync(offsetsToCommit);
  }

  private Admin createAdmin() {
    Map<String, Object> adminProps = new HashMap<>(kafkaProps);
    return Admin.create(adminProps);
  }

  protected Admin admin() {
    return admin;
  }

  public void start() {
    consumer.subscribe(ImmutableList.of(controlTopic));

    // initial poll with longer duration so the consumer will initialize...
    process(Duration.ofMillis(1000));
  }

  public void stop() {
    LOG.info("Channel stopping");
    producer.close();
    consumer.close();
    admin.close();
  }
}
