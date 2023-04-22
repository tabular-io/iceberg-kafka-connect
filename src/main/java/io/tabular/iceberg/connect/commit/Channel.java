// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.commit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

@Log4j
public abstract class Channel {

  protected final Map<String, String> kafkaProps;
  private final String coordinatorTopic;
  private final KafkaProducer<byte[], byte[]> producer;
  private final KafkaConsumer<byte[], byte[]> consumer;

  private static final String COORDINATOR_TOPIC_PROP = "iceberg.coordinator.topic";
  private static final String KAFKA_PROP_PREFIX = "iceberg.kafka.";

  public Channel(Map<String, String> props) {
    this.kafkaProps = PropertyUtil.propertiesWithPrefix(props, KAFKA_PROP_PREFIX);
    this.coordinatorTopic = props.get(COORDINATOR_TOPIC_PROP);
    this.producer = createProducer();
    this.consumer = createConsumer();
  }

  protected void send(Message message) {
    log.info("Sending message of type: " + message.getType().name());
    byte[] data = SerializationUtil.serializeToBytes(message);
    producer.send(new ProducerRecord<>(coordinatorTopic, data));
    producer.flush();
  }

  protected abstract void receive(Message message);

  @SuppressWarnings("deprecation")
  public void process() {
    // TODO: we're using the deprecated poll(long) API as it waits for metadata, better options?
    ConsumerRecords<byte[], byte[]> records = consumer.poll(0);
    while (!records.isEmpty()) {
      records.forEach(
          record -> {
            Message message = SerializationUtil.deserializeFromBytes(record.value());
            log.info("Received message of type: " + message.getType().name());
            receive(message);
          });
      records = consumer.poll(0);
    }
  }

  private KafkaProducer<byte[], byte[]> createProducer() {
    Map<String, Object> producerProps = new HashMap<>(kafkaProps);
    return new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());
  }

  private KafkaConsumer<byte[], byte[]> createConsumer() {
    Map<String, Object> consumerProps = new HashMap<>(kafkaProps);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "cg-iceberg-" + UUID.randomUUID());
    return new KafkaConsumer<>(
        consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  public void start() {
    consumer.subscribe(List.of(coordinatorTopic));
  }

  @SneakyThrows
  public void stop() {
    log.info("Channel stopping");
    producer.close();
    consumer.close();
  }
}
