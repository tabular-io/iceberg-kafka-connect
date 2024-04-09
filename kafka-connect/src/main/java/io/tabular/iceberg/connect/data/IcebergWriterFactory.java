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
import io.tabular.iceberg.connect.deadletter.DeadLetterUtils;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergWriterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriterFactory.class);

  private final Catalog catalog;
  private final IcebergSinkConfig config;

  private final Predicate<String> shouldAutoCreate;

  private TableIdentifierSupplier tableIdSupplier;

  private TableCreator tableCreator;

  private static class TableIdentifierSupplier {
    TableIdentifier id(String name) {
      return TableIdentifier.parse(name);
    }
  }

  private static class ErrorHandlingTableIdentifierSupplier extends TableIdentifierSupplier {
    @Override
    TableIdentifier id(String name) {
      TableIdentifier tableId;
      try {
        tableId = super.id(name);
      } catch (Exception error) {
        throw new DeadLetterUtils.DeadLetterException("TABLE_IDENTIFIER", error);
      }
      return tableId;
    }
  }

  private static class TableCreator {

    private final Catalog catalog;
    private final IcebergSinkConfig config;

    TableCreator(Catalog catalog, IcebergSinkConfig config) {
      this.catalog = catalog;
      this.config = config;
    }

    public Table createTable(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec spec,
        Map<String, String> properties) {
      return catalog.createTable(identifier, schema, spec, properties);
    }

    public Schema schema(TableIdentifier identifier, SinkRecord sample) {
      StructType structType;
      if (sample.valueSchema() == null) {
        structType =
            SchemaUtils.inferIcebergType(sample.value(), config)
                .orElseThrow(() -> new DataException("Unable to create table from empty object"))
                .asStructType();
      } else {
        structType = SchemaUtils.toIcebergType(sample.valueSchema(), config).asStructType();
      }

      return new org.apache.iceberg.Schema(structType.fields());
    }
  }

  private class ErrorHandlingTableCreator extends TableCreator {

    private final TableIdentifier deadLetterTableId;

    ErrorHandlingTableCreator(Catalog catalog, IcebergSinkConfig config) {
      super(catalog, config);
      this.deadLetterTableId = TableIdentifier.parse(config.deadLetterTableName());
    }

    @Override
    public Table createTable(
        TableIdentifier identifier,
        Schema schema,
        PartitionSpec spec,
        Map<String, String> properties) {

      Table table;
      if (identifier == deadLetterTableId) {
        table = catalog.createTable(identifier, schema, spec, properties);
      } else {
        try {
          table = catalog.createTable(identifier, schema, spec, properties);
        } catch (IllegalArgumentException | ValidationException error) {

          // CommitStateUnknown exception.
          // Look into NullPointer exceptions
          // look into JsonProcessing exceptions inside of Iceberg

          throw new DeadLetterUtils.DeadLetterException("CREATE_TABLE", error);
        }
      }
      return table;
    }

    @Override
    public Schema schema(TableIdentifier identifier, SinkRecord sample) {
      Schema schema;
      if (identifier == deadLetterTableId) {
        schema = super.schema(identifier, sample);
      } else {
        try {
          schema = super.schema(identifier, sample);
        } catch (IllegalArgumentException | ValidationException error) {
          throw new DeadLetterUtils.DeadLetterException("CREATE_SCHEMA", error);
        }
      }
      return schema;
    }
  }

  public IcebergWriterFactory(Catalog catalog, IcebergSinkConfig config) {
    this.catalog = catalog;
    this.config = config;
    this.tableCreator = new TableCreator(catalog, config);
    this.tableIdSupplier = new TableIdentifierSupplier();

    if (config.autoCreateEnabled()) {
      shouldAutoCreate = (unused) -> true;
    } else if (config.deadLetterTableEnabled()) {
      String deadLetterTableName = config.deadLetterTableName().toLowerCase();
      shouldAutoCreate = (tableName) -> tableName.equals(deadLetterTableName);
      this.tableCreator = new ErrorHandlingTableCreator(catalog, config);
      this.tableIdSupplier = new ErrorHandlingTableIdentifierSupplier();
    } else {
      shouldAutoCreate = (unused) -> false;
    }
  }

  public RecordWriter createWriter(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {

    // this can fail.
    TableIdentifier identifier = tableIdSupplier.id(tableName);
    Table table;
    try {
      table = catalog.loadTable(identifier);
    } catch (NoSuchTableException nst) {
      if (shouldAutoCreate.test(tableName)) {
        table = autoCreateTable(tableName, sample);
      } else if (ignoreMissingTable) {
        return new RecordWriter() {};
      } else {
        throw nst;
      }
    }

    return new IcebergWriter(table, tableName, config);
  }

  @VisibleForTesting
  Table autoCreateTable(String tableName, SinkRecord sample) {

    TableIdentifier identifier = tableIdSupplier.id(tableName);

    Schema schema = tableCreator.schema(identifier, sample);

    List<String> partitionBy = config.tableConfig(tableName).partitionBy();

    PartitionSpec spec;
    try {
      spec = SchemaUtils.createPartitionSpec(schema, partitionBy);
    } catch (Exception e) {
      LOG.error(
          "Unable to create partition spec {}, table {} will be unpartitioned",
          partitionBy,
          identifier,
          e);
      spec = PartitionSpec.unpartitioned();
    }

    PartitionSpec partitionSpec = spec;
    AtomicReference<Table> result = new AtomicReference<>();
    Tasks.range(1)
        .retry(IcebergSinkConfig.CREATE_TABLE_RETRIES)
        .run(
            notUsed -> {
              try {
                result.set(catalog.loadTable(identifier));
              } catch (NoSuchTableException e) {
                result.set(
                    tableCreator.createTable(
                        identifier, schema, partitionSpec, config.autoCreateProps()));
              }
            });
    return result.get();
  }
}
