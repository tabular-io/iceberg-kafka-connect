// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;

public class PartitionedDeltaWriter extends BaseDeltaTaskWriter {
  private final PartitionKey partitionKey;

  private final Map<PartitionKey, RowDataDeltaWriter> writers = Maps.newHashMap();

  PartitionedDeltaWriter(
      PartitionSpec spec,
      FileFormat format,
      FileAppenderFactory<Record> appenderFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      boolean upsertMode) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema, upsertMode);
    this.partitionKey = new PartitionKey(spec, schema);
  }

  @Override
  RowDataDeltaWriter route(Record row) {
    partitionKey.partition(wrapper().wrap(row));

    RowDataDeltaWriter writer = writers.get(partitionKey);
    if (writer == null) {
      // NOTICE: we need to copy a new partition key here, in case of messing up the keys in
      // writers.
      PartitionKey copiedKey = partitionKey.copy();
      writer = new RowDataDeltaWriter(copiedKey);
      writers.put(copiedKey, writer);
    }

    return writer;
  }

  @Override
  public void close() {
    try {
      Tasks.foreach(writers.values())
          .throwFailureWhenFinished()
          .noRetry()
          .run(RowDataDeltaWriter::close, IOException.class);

      writers.clear();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close equality delta writer", e);
    }
  }
}
