// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc.commit;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
  private final ExecutorService executor;
  private volatile boolean executorTerminated;
  private final ConcurrentLinkedQueue<byte[]> queue;
  private final KafkaProducer<byte[], byte[]> producer;

  private static final int CONSUMER_POLL_TIMEOUT_MS = 500;
  private static final int EXEC_SHUTDOWN_WAIT_MS = 5000;

  public Channel(Map<String, String> props) {
    this.kafkaProps = PropertyUtil.propertiesWithPrefix(props, "iceberg.kafka.");
    this.coordinatorTopic = props.get("iceberg.coordinator.topic");
    this.executor = Executors.newSingleThreadExecutor();
    this.queue = new ConcurrentLinkedQueue<>();

    Map<String, Object> producerProps = new HashMap<>(kafkaProps);
    this.producer =
        new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());
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
    executorTerminated = false;
    executor.submit(
        () -> {
          // TODO: consumer cleanup/close
          KafkaConsumer<byte[], byte[]> consumer = createConsumer();
          consumer.subscribe(List.of(coordinatorTopic));
          while (!executorTerminated) {
            ConsumerRecords<byte[], byte[]> records =
                consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT_MS));
            records.forEach(record -> queue.add(record.value()));
            consumer.commitSync();
          }
        });
  }

  private KafkaConsumer<byte[], byte[]> createConsumer() {
    Map<String, Object> consumerProps = new HashMap<>(kafkaProps);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "cg-iceberg-" + UUID.randomUUID());
    return new KafkaConsumer<>(
        consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  @SneakyThrows
  public void stop() {
    log.info("Channel stopping");
    producer.close();
    executor.shutdown();
    executorTerminated = true;
    executor.awaitTermination(EXEC_SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS);
  }
}
