// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import io.tabular.connect.poc.convert.RecordConverter;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

public class IcebergWriter implements Closeable {
  private final Table table;
  private TaskWriter<Record> writer;
  private Map<TopicPartition, Long> offsets;

  public IcebergWriter(Catalog catalog, TableIdentifier tableIdentifier) {
    this.table = catalog.loadTable(tableIdentifier);
  }

  public void write(Collection<SinkRecord> sinkRecords) {
    if (writer == null) {
      table.refresh();
      writer = IcebergUtil.createTableWriter(table);
      offsets = new HashMap<>();
    }

    StructType schemaType = table.schema().asStruct();
    sinkRecords.forEach(
        record -> {
          offsets.put(
              new TopicPartition(record.topic(), record.kafkaPartition()), record.kafkaOffset());
          try {
            Record row = RecordConverter.convert(record.value(), schemaType);
            writer.write(row);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  @SneakyThrows
  public Pair<List<DataFile>, Map<TopicPartition, Long>> commit() {
    if (writer == null) {
      return Pair.of(List.of(), Map.of());
    }

    WriteResult writeResult = writer.complete();
    Pair<List<DataFile>, Map<TopicPartition, Long>> result =
        Pair.of(Arrays.asList(writeResult.dataFiles()), offsets);

    writer = null;
    offsets = null;

    return result;
  }

  @Override
  @SneakyThrows
  public void close() {
    if (writer != null) {
      writer.close();
    }
  }
}
