// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import io.tabular.connect.poc.convert.RecordConverter;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import lombok.SneakyThrows;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.connect.sink.SinkRecord;

public class IcebergWriter implements Closeable {
  private final int commitIntervalMs;
  private final Table table;
  private TaskWriter<Record> writer;
  private long startTime;

  public IcebergWriter(Catalog catalog, TableIdentifier tableIdentifier, int commitIntervalMs) {
    this.table = catalog.loadTable(tableIdentifier);
    this.commitIntervalMs = commitIntervalMs;
  }

  public void write(Collection<SinkRecord> sinkRecords) {
    if (writer == null) {
      table.refresh();
      writer = IcebergUtil.createTableWriter(table);
      startTime = System.currentTimeMillis();
    }

    StructType schemaType = table.schema().asStruct();
    sinkRecords.forEach(
        record -> {
          try {
            Record row = RecordConverter.convert(record.value(), schemaType);
            writer.write(row);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });

    commitIfNeeded();
  }

  @SneakyThrows
  public void commitIfNeeded() {
    if (System.currentTimeMillis() - startTime >= commitIntervalMs) {
      WriteResult result = writer.complete();
      writer = null;

      table.refresh();
      AppendFiles appendOp = table.newAppend();
      Arrays.stream(result.dataFiles())
          .filter(f -> f.recordCount() > 0)
          .forEach(appendOp::appendFile);
      appendOp.commit();

      // TODO: offsets
    }
  }

  @Override
  @SneakyThrows
  public void close() {
    if (writer != null) {
      writer.close();
    }
  }
}
