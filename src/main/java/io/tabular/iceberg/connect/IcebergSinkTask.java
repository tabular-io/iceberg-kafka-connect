// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect;

import static java.util.stream.Collectors.toCollection;

import io.tabular.iceberg.connect.channel.Coordinator;
import io.tabular.iceberg.connect.channel.Worker;
import io.tabular.iceberg.connect.data.Utilities;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkTask extends SinkTask {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkTask.class);
  private static final String TOPICS_PROP = "topics";

  private Map<String, String> props;
  private Coordinator coordinator;
  private Worker worker;

  private static final String TABLE_PROP = "iceberg.table";

  @Override
  public String version() {
    return "0.0.1";
  }

  @Override
  public void start(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    Catalog catalog = Utilities.loadCatalog(props);
    TableIdentifier tableIdentifier = TableIdentifier.parse(props.get(TABLE_PROP));

    if (isLeader(partitions)) {
      LOG.info("Task elected leader, starting commit coordinator");
      coordinator = new Coordinator(catalog, tableIdentifier, props);
      coordinator.start();
    }
    LOG.info("Starting commit worker");
    worker = new Worker(catalog, tableIdentifier, props, context);
    worker.syncCommitOffsets();
    worker.start();
  }

  private boolean isLeader(Collection<TopicPartition> partitions) {
    // there should only be one worker assigned partition 0 of the first
    // topic, so elect that one the leader
    String firstTopic = getTopics(props).first();
    return partitions.stream()
        .filter(tp -> tp.topic().equals(firstTopic))
        .anyMatch(tp -> tp.partition() == 0);
  }

  public static SortedSet<String> getTopics(Map<String, String> props) {
    return Arrays.stream(props.get(TOPICS_PROP).split(","))
        .map(String::trim)
        .collect(toCollection(TreeSet::new));
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    worker.stop();
    if (coordinator != null) {
      coordinator.stop();
      coordinator = null;
    }
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      worker.save(sinkRecords);
    }
    coordinate();
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    coordinate();
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    return worker.getCommitOffsets();
  }

  private void coordinate() {
    if (worker != null) {
      worker.process();
    }
    if (coordinator != null) {
      coordinator.process();
    }
  }

  @Override
  public void stop() {
    if (worker != null) {
      worker.stop();
    }
    if (coordinator != null) {
      coordinator.stop();
    }
  }
}
