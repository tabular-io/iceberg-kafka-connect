// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class IcebergSinkConnectorTask extends SinkTask {

  private Catalog catalog;
  private TableIdentifier tableIdentifier;
  private Table table;
  private TaskWriter<Record> writer;

  private static final String TABLE_PROP = "iceberg.table";

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
    this.catalog = IcebergUtil.loadCatalog(props);
    this.tableIdentifier = TableIdentifier.parse(props.get(TABLE_PROP));
  }

  @Override
  @SneakyThrows
  public void put(Collection<SinkRecord> sinkRecords) {
    if (table == null) {
      refreshTable();
    }
    if (writer == null) {
      writer = createWriter(table);
    }
    StructType schemaType = table.schema().asStruct();
    sinkRecords.forEach(
        record -> {
          try {
            Record row = ConvertUtil.convert((byte[]) record.value(), schemaType);
            writer.write(row);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  @Override
  @SneakyThrows
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    WriteResult result = writer.complete();
    writer = null;

    AppendFiles appendOp = table.newAppend();
    Arrays.stream(result.dataFiles()).forEach(appendOp::appendFile);
    appendOp.commit();

    refreshTable();
  }

  private void refreshTable() {
    table = catalog.loadTable(tableIdentifier);
  }

  @Override
  @SneakyThrows
  public void stop() {
    if (writer != null) {
      writer.abort();
    }
  }

  private TaskWriter<Record> createWriter(Table table) {
    String formatStr =
        table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.valueOf(formatStr.toUpperCase());

    long targetFileSize =
        PropertyUtil.propertyAsLong(
            table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    FileAppenderFactory<Record> appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec());

    // (partition ID + task ID + operation ID) must be unique
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(format)
            .build();

    TaskWriter<Record> writer;
    if (table.spec().isUnpartitioned()) {
      writer =
          new UnpartitionedWriter<>(
              table.spec(), format, appenderFactory, fileFactory, table.io(), targetFileSize);
    } else {
      writer =
          new PartitionedFanoutRecordWriter(
              table.spec(),
              format,
              appenderFactory,
              fileFactory,
              table.io(),
              targetFileSize,
              table.schema());
    }
    return writer;
  }
}
