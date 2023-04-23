// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

public class IcebergWriter implements Closeable {
  private final Table table;
  private RecordConverter recordConverter;
  private TaskWriter<Record> writer;
  private Map<TopicPartition, Long> offsets;

  @Builder
  @Getter
  public static class Result {
    private List<DataFile> dataFiles;
    private Map<TopicPartition, Long> offsets;
  }

  public IcebergWriter(Catalog catalog, TableIdentifier tableIdentifier) {
    this.table = catalog.loadTable(tableIdentifier);
    this.recordConverter = new RecordConverter(table);
    this.writer = Utilities.createTableWriter(table);
    this.offsets = new HashMap<>();
  }

  public void write(Collection<SinkRecord> sinkRecords) {
    // TODO: detect schema change

    sinkRecords.forEach(
        record -> {
          // the consumer stores the offsets that corresponds to the next record to consume,
          // so increment the record offset by one
          offsets.put(
              new TopicPartition(record.topic(), record.kafkaPartition()),
              record.kafkaOffset() + 1);
          try {
            Record row = recordConverter.convert(record.value());
            writer.write(row);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  @SneakyThrows
  public Result complete() {
    WriteResult writeResult = writer.complete();
    Result result =
        Result.builder().dataFiles(Arrays.asList(writeResult.dataFiles())).offsets(offsets).build();

    table.refresh();
    recordConverter = new RecordConverter(table);
    writer = Utilities.createTableWriter(table);
    offsets = new HashMap<>();

    return result;
  }

  @Override
  @SneakyThrows
  public void close() {
    writer.close();
  }
}
