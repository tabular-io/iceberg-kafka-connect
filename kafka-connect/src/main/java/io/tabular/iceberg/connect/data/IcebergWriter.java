/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.data;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.kafka.connect.sink.SinkRecord;

public class IcebergWriter implements Closeable {
  private final Table table;
  private final TableIdentifier tableIdentifier;
  private final IcebergSinkConfig config;
  private final RecordConverter recordConverter;
  private final TaskWriter<Record> writer;

  public IcebergWriter(Catalog catalog, String tableName, IcebergSinkConfig config) {
    this.tableIdentifier = TableIdentifier.parse(tableName);
    this.table = catalog.loadTable(tableIdentifier);
    this.config = config;
    this.recordConverter = new RecordConverter(table, config.getJsonConverter());
    this.writer = Utilities.createTableWriter(table, config);
  }

  public void write(SinkRecord record) {
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
    Object opValue = Utilities.extractFromRecordValue(recordValue, cdcField);

    if (opValue == null) {
      return Operation.INSERT;
    }

    String opStr = opValue.toString().trim().toUpperCase();
    if (opStr.isEmpty()) {
      return Operation.INSERT;
    }

    // TODO: define value mapping in config?

    switch (opStr.charAt(0)) {
      case 'U':
        return Operation.UPDATE;
      case 'D':
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

    return new WriterResult(
        tableIdentifier,
        Arrays.asList(writeResult.dataFiles()),
        Arrays.asList(writeResult.deleteFiles()),
        table.spec().partitionType());
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
