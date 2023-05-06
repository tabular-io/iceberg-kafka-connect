// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.data;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

public class IcebergWriter implements Closeable {
  private final Table table;
  private final TableIdentifier tableIdentifier;
  private final IcebergSinkConfig config;
  private RecordConverter recordConverter;
  private TaskWriter<Record> writer;
  private Map<TopicPartition, Long> offsets;

  public IcebergWriter(Catalog catalog, String tableName, IcebergSinkConfig config) {
    this.tableIdentifier = TableIdentifier.parse(tableName);
    this.table = catalog.loadTable(tableIdentifier);
    this.config = config;
    this.recordConverter = new RecordConverter(table);
    this.writer = Utilities.createTableWriter(table, config);
    this.offsets = new HashMap<>();
  }

  public void write(SinkRecord record) {
    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    offsets.put(
        new TopicPartition(record.topic(), record.kafkaPartition()), record.kafkaOffset() + 1);
    try {
      // TODO: config to handle tombstones instead of always ignoring?
      if (record.value() != null) {
        Record row = recordConverter.convert(record.value());
        String cdcField = config.getTablesCdcField();
        if (cdcField == null) {
          writer.write(row);
        } else {
          Operation op = extractCdcOperation(record.value(), cdcField);
          writer.write(new RecordWrapper(row, op));
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Operation extractCdcOperation(Object recordValue, String cdcField) {
    Object opValue;
    if (recordValue instanceof Struct) {
      opValue = ((Struct) recordValue).get(cdcField);
    } else if (recordValue instanceof Map) {
      opValue = ((Map<?, ?>) recordValue).get(cdcField);
    } else {
      throw new UnsupportedOperationException(
          "Cannot extract value from type: " + recordValue.getClass().getName());
    }

    if (opValue == null) {
      return Operation.INSERT;
    }

    // FIXME!! define mapping in config!!

    String opStr = opValue.toString().toUpperCase();
    switch (opStr) {
      case "UPDATE":
      case "U":
        return Operation.UPDATE;
      case "DELETE":
      case "D":
        return Operation.DELETE;
      default:
        return Operation.INSERT;
    }
  }

  public WriterResult complete() {
    WriteResult writeResult;
    try {
      writeResult = writer.complete();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    WriterResult result =
        new WriterResult(
            tableIdentifier,
            Arrays.asList(writeResult.dataFiles()),
            Arrays.asList(writeResult.deleteFiles()),
            table.spec().partitionType(),
            offsets);

    table.refresh();
    recordConverter = new RecordConverter(table);
    writer = Utilities.createTableWriter(table, config);
    offsets = new HashMap<>();

    return result;
  }

  @Override
  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
