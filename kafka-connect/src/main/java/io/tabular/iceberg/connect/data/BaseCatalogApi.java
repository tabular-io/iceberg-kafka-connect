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
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import io.tabular.iceberg.connect.exception.WriteException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseCatalogApi implements CatalogApi {
  private static final Logger LOG = LoggerFactory.getLogger(BaseCatalogApi.class);
  private final Catalog catalog;
  private final BiFunction<TableIdentifier, SinkRecord, Schema> schemaFactory;
  private final IcebergSinkConfig config;

  BaseCatalogApi(Catalog catalog, IcebergSinkConfig config) {
    this.config = config;
    this.catalog = catalog;
    this.schemaFactory =
        (tableIdentifier, sample) -> {
          Types.StructType structType;
          if (sample.valueSchema() == null) {
            structType =
                SchemaUtils.inferIcebergType(sample.value(), config)
                    .orElseThrow(
                        () -> new DataException("Unable to create table from empty object"))
                    .asStructType();
          } else {
            structType = SchemaUtils.toIcebergType(sample.valueSchema(), config).asStructType();
          }

          return new org.apache.iceberg.Schema(structType.fields());
        };
  }

  @VisibleForTesting
  BaseCatalogApi(
      Catalog catalog,
      IcebergSinkConfig config,
      BiFunction<TableIdentifier, SinkRecord, Schema> schemaFactory) {
    this.config = config;
    this.catalog = catalog;
    this.schemaFactory = schemaFactory;
  }

  @Override
  public TableIdentifier tableId(String name) {
    TableIdentifier tableId;
    try {
      Preconditions.checkArgument(!name.isEmpty());
      tableId = TableIdentifier.parse(name);
    } catch (Exception error) {
      throw new WriteException.TableIdentifierException(name, error);
    }
    return tableId;
  }

  @Override
  public final Table loadTable(TableIdentifier identifier) {
    try {
      return catalog.loadTable(identifier);
    } catch (NoSuchTableException error) {
      throw error;
    } catch (Exception error) {
      throw new WriteException.LoadTableException(identifier, error);
    }
  }

  @Override
  public final PartitionSpec partitionSpec(String tableName, Schema schema) {
    List<String> partitionBy = config.tableConfig(tableName).partitionBy();

    PartitionSpec spec;
    try {
      spec = SchemaUtils.createPartitionSpec(schema, partitionBy);
    } catch (Exception e) {
      LOG.error(
          "Unable to create partition spec {}, table {} will be unpartitioned",
          partitionBy,
          tableName,
          e);
      spec = PartitionSpec.unpartitioned();
    }
    return spec;
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {

    try {
      return catalog.createTable(identifier, schema, spec, properties);
    } catch (Exception error) {
      throw new WriteException.CreateTableException(identifier, error);
    }
  }

  @Override
  public Schema schema(TableIdentifier identifier, SinkRecord sample) {
    try {
      return schemaFactory.apply(identifier, sample);
    } catch (Exception error) {
      throw new WriteException.CreateSchemaException(identifier, error);
    }
  }
}
