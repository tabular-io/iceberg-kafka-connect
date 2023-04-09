// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import java.util.Collection;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class IcebergSinkConnectorTask extends SinkTask {

  private IcebergWriter writer;

  private static final String TABLE_PROP = "iceberg.table";
  private static final String COMMIT_INTERVAL_MS_PROP = "iceberg.table.commitIntervalMs";
  private static final int COMMIT_INTERVAL_MS_DEFAULT = 60_000;

  @Override
  public String version() {
    return "0.0.1";
  }

  @Override
  public void initialize(SinkTaskContext context) {
    super.initialize(context);
  }

  @Override
  public void start(Map<String, String> props) {
    Catalog catalog = IcebergUtil.loadCatalog(props);
    TableIdentifier tableIdentifier = TableIdentifier.parse(props.get(TABLE_PROP));
    this.writer =
        new IcebergWriter(
            catalog,
            tableIdentifier,
            PropertyUtil.propertyAsInt(props, COMMIT_INTERVAL_MS_PROP, COMMIT_INTERVAL_MS_DEFAULT));
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    writer.write(sinkRecords);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    writer.commitIfNeeded();
  }

  @Override
  public void stop() {
    if (writer != null) {
      writer.close();
    }
  }
}
