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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.connect.sink.SinkRecord;

public class IcebergWriterFactory {

  private final IcebergSinkConfig config;

  private final Predicate<String> shouldAutoCreate;

  private final CatalogApi catalogApi;

  public IcebergWriterFactory(Catalog catalog, IcebergSinkConfig config) {
    this(config, getCatalogApi(catalog, config));
  }

  public IcebergWriterFactory(IcebergSinkConfig config, CatalogApi api) {
    this.config = config;
    this.catalogApi = api;

    if (config.autoCreateEnabled()) {
      shouldAutoCreate = (unused) -> true;
    } else if (config.deadLetterTableEnabled()) {
      String deadLetterTableName = config.deadLetterTableName().toLowerCase();
      shouldAutoCreate = (tableName) -> tableName.equals(deadLetterTableName);
    } else {
      shouldAutoCreate = (unused) -> false;
    }
  }

  public RecordWriter createWriter(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {

    TableIdentifier identifier = catalogApi.tableId(tableName);
    Table table;
    try {
      table = catalogApi.loadTable(identifier);
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

    TableIdentifier identifier = catalogApi.tableId(tableName);

    Schema schema = catalogApi.schema(identifier, sample);

    PartitionSpec partitionSpec = catalogApi.partitionSpec(tableName, schema);

    AtomicReference<Table> result = new AtomicReference<>();
    Tasks.range(1)
        .retry(IcebergSinkConfig.CREATE_TABLE_RETRIES)
        .run(
            notUsed -> {
              try {
                result.set(catalogApi.loadTable(identifier));
              } catch (NoSuchTableException e) {
                result.set(
                    catalogApi.createTable(
                        identifier, schema, partitionSpec, config.autoCreateProps()));
              }
            });
    return result.get();
  }

  private static CatalogApi getCatalogApi(Catalog catalog, IcebergSinkConfig config) {
    if (config.deadLetterTableEnabled()) {
      return new CatalogApi.ErrorHandlingCatalogApi(catalog, config);
    } else {
      return new CatalogApi(catalog, config) {};
    }
  }
}
