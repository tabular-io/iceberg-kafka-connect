// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import io.tabular.connect.poc.commit.Coordinator;
import io.tabular.connect.poc.commit.Worker;
import java.util.Collection;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

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
      coordinator = new Coordinator(catalog, tableIdentifier, props);
      coordinator.start();
    }
    worker = new Worker(catalog, tableIdentifier, props);
    worker.start();
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    if (coordinator != null) {
      coordinator.process();
    }
    worker.process();
    worker.save(sinkRecords);
  }

  @Override
  public void stop() {
    worker.stop();
    if (coordinator != null) {
      coordinator.stop();
    }
  }
}
