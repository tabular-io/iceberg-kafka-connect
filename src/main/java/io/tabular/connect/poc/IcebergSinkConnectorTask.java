// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import io.tabular.connect.poc.commit.Coordinator;
import io.tabular.connect.poc.commit.Worker;
import java.util.Collection;
import java.util.Map;
import lombok.extern.log4j.Log4j;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

@Log4j
public class IcebergSinkConnectorTask extends SinkTask {

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
    Catalog catalog = IcebergUtil.loadCatalog(props);
    TableIdentifier tableIdentifier = TableIdentifier.parse(props.get(TABLE_PROP));

    // TODO: handle leader election when there are multiple topics
    if (partitions.stream().anyMatch(tp -> tp.partition() == 0)) {
      log.info("Worker elected leader, starting commit coordinator");
      coordinator = new Coordinator(catalog, tableIdentifier, props);
      coordinator.start();
    }
    log.info("Starting commit worker");
    worker = new Worker(catalog, tableIdentifier, props, context);
    worker.syncCommitOffsets();
    worker.start();
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
    worker.process();
    if (coordinator != null) {
      coordinator.process();
    }
  }

  @Override
  public void stop() {
    worker.stop();
    if (coordinator != null) {
      coordinator.stop();
    }
  }
}
