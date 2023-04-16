// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc.commit;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

@Log4j
public abstract class Channel {

  private final String coordinatorTopic;
  private final ExecutorService executor;
  private final ConcurrentLinkedQueue<byte[]> queue;
  private final KafkaProducer<byte[], byte[]> producer;
  private final KafkaConsumer<byte[], byte[]> consumer;

  private static final int CONSUMER_POLL_TIMEOUT_MS = 500;
  private static final int EXEC_SHUTDOWN_WAIT_MS = 5000;

  public Channel(Map<String, String> props) {
    this.coordinatorTopic = props.get("iceberg.coordinator.topic");
    this.executor = Executors.newSingleThreadExecutor();
    this.queue = new ConcurrentLinkedQueue<>();

    String bootstrapServers = props.get("bootstrap.servers");
    producer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    consumer =
        new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "latest",
                ConsumerConfig.GROUP_ID_CONFIG,
                "cg-iceberg-" + UUID.randomUUID()));
  }

  protected void send(Message message) {
    log.info("Sending message of type: " + message.getType().name());
    byte[] data = SerializationUtil.serializeToBytes(message);
    producer.send(new ProducerRecord<>(coordinatorTopic, data));
    producer.flush();
  }

  protected abstract void receive(Message message);

  public void process() {
    while (queue.peek() != null) {
      Message message = SerializationUtil.deserializeFromBytes(queue.remove());
      log.info("Received message of type: " + message.getType().name());
      receive(message);
    }
  }

  public void start() {
    log.info("Channel starting");
    consumer.subscribe(List.of(coordinatorTopic));
    executor.submit(
        () -> {
          while (true) {
            ConsumerRecords<byte[], byte[]> records =
                consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT_MS));
            records.forEach(record -> queue.add(record.value()));
          }
        });
  }

  @SneakyThrows
  public void stop() {
    log.info("Channel stopping");
    producer.close();
    consumer.close();
    executor.shutdown();
    executor.awaitTermination(EXEC_SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS);
  }
}
