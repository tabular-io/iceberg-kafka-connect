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
import org.apache.iceberg.util.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

public class IcebergWriter implements Closeable {
  private final Table table;
  private RecordConverter recordConverter;
  private TaskWriter<Record> writer;
  private Map<TopicPartition, Long> offsets;

  public IcebergWriter(Catalog catalog, TableIdentifier tableIdentifier) {
    this.table = catalog.loadTable(tableIdentifier);
    this.recordConverter = new RecordConverter(table);
    this.writer = IcebergUtil.createTableWriter(table);
    this.offsets = new HashMap<>();
  }

  public void write(Collection<SinkRecord> sinkRecords) {
    // TODO: detect schema change

    sinkRecords.forEach(
        record -> {
          offsets.put(
              new TopicPartition(record.topic(), record.kafkaPartition()), record.kafkaOffset());
          try {
            Record row = recordConverter.convert(record.value());
            writer.write(row);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  @SneakyThrows
  public Pair<List<DataFile>, Map<TopicPartition, Long>> commit() {
    WriteResult writeResult = writer.complete();
    Pair<List<DataFile>, Map<TopicPartition, Long>> result =
        Pair.of(Arrays.asList(writeResult.dataFiles()), offsets);

    table.refresh();
    recordConverter = new RecordConverter(table);
    writer = IcebergUtil.createTableWriter(table);
    offsets = new HashMap<>();

    return result;
  }

  @Override
  @SneakyThrows
  public void close() {
    writer.close();
  }
}
