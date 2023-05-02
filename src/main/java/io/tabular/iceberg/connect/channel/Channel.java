// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel;

import static java.util.stream.Collectors.toList;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.channel.events.Event;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
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
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Channel.class);

  protected final Map<String, String> kafkaProps;
  private final String controlTopic;
  private final String controlGroupId;
  private final String transactionalId;
  private final KafkaProducer<byte[], byte[]> producer;
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final Admin admin;
  private final Map<Integer, Long> controlTopicOffsets = new HashMap<>();

  public Channel(String name, IcebergSinkConfig config) {
    this.kafkaProps = config.getKafkaProps();
    this.controlTopic = config.getControlTopic();
    this.controlGroupId = config.getControlGroupId();
    this.transactionalId = name + config.getTransactionalSuffix();
    this.producer = createProducer();
    this.consumer = createConsumer();
    this.admin = createAdmin();
  }

  protected void send(Event event) {
    send(event, ImmutableMap.of());
  }

  protected void send(Event event, Map<TopicPartition, OffsetAndMetadata> sourceOffsets) {
    LOG.info("Sending event of type: {}", event.getType().name());

    byte[] data;
    try {
      data = AvroEncoderUtil.encode(event, event.getSchema());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    producer.beginTransaction();
    try {
      producer.send(new ProducerRecord<>(controlTopic, data));
      if (!sourceOffsets.isEmpty()) {
        // TODO: this doesn't fence zombies
        producer.sendOffsetsToTransaction(sourceOffsets, new ConsumerGroupMetadata(controlGroupId));
      }
      producer.commitTransaction();
    } catch (Exception e) {
      producer.abortTransaction();
      throw e;
    }
  }

  protected abstract void receive(Event event);

  public void process() {
    consumeAvailable(this::receive);
  }

  @SuppressWarnings("deprecation")
  protected void consumeAvailable(Consumer<Event> eventHandler) {
    // TODO: we're using the deprecated poll(long) API as it waits for metadata, better options?
    ConsumerRecords<byte[], byte[]> records = consumer.poll(0L);
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

            LOG.info("Received event of type: {}", event.getType().name());
            eventHandler.accept(event);
          });
      records = consumer.poll(0L);
    }
  }

  protected Map<Integer, Long> controlTopicOffsets() {
    return controlTopicOffsets;
  }

  private KafkaProducer<byte[], byte[]> createProducer() {
    Map<String, Object> producerProps = new HashMap<>(kafkaProps);
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    KafkaProducer<byte[], byte[]> result =
        new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());
    result.initTransactions();
    return result;
  }

  private KafkaConsumer<byte[], byte[]> createConsumer() {
    Map<String, Object> consumerProps = new HashMap<>(kafkaProps);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "cg-iceberg-" + UUID.randomUUID());
    return new KafkaConsumer<>(
        consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  private Admin createAdmin() {
    Map<String, Object> adminProps = new HashMap<>(kafkaProps);
    return Admin.create(adminProps);
  }

  protected void setControlTopicOffsets(Map<Integer, Long> offsets) {
    offsets.forEach(
        (k, v) -> consumer.seek(new TopicPartition(controlTopic, k), new OffsetAndMetadata(v)));
  }

  protected Admin admin() {
    return admin;
  }

  public void start() {
    Map<String, Object> adminProps = new HashMap<>(kafkaProps);
    try (Admin admin = Admin.create(adminProps)) {
      List<TopicPartition> partitions =
          admin.describeTopics(ImmutableList.of(controlTopic)).topicNameValues().get(controlTopic)
              .get().partitions().stream()
              .map(info -> new TopicPartition(controlTopic, info.partition()))
              .collect(toList());
      consumer.assign(partitions);
    } catch (InterruptedException | ExecutionException e) {
      throw new ConnectException(e);
    }
  }

  public void stop() {
    LOG.info("Channel stopping");
    producer.close();
    consumer.close();
    admin.close();
  }
}
