// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect;

import static io.tabular.iceberg.connect.IcebergSinkConnector.COORDINATOR_PROP;

import io.tabular.iceberg.connect.channel.Coordinator;
import io.tabular.iceberg.connect.channel.Worker;
import io.tabular.iceberg.connect.data.Utilities;
import java.util.Collection;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.log4j.Logger;

public class IcebergSinkTask extends SinkTask {

  private static final Logger LOG = Logger.getLogger(IcebergSinkTask.class);

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

    if (PropertyUtil.propertyAsBoolean(props, COORDINATOR_PROP, false)) {
      LOG.info("Task elected leader, starting commit coordinator");
      coordinator = new Coordinator(catalog, tableIdentifier, props);
      coordinator.start();
    }
    LOG.info("Starting commit worker");
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
