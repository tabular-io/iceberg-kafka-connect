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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.TableSinkConfig;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import io.tabular.iceberg.connect.exception.WriteException;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

public class BaseCatalogApiTest {

  private static final String DEAD_LETTER_TABLE = "dlt.table";
  private static final TableIdentifier DEAD_LETTER_TABLE_ID =
      TableIdentifier.parse(DEAD_LETTER_TABLE);

  private static final org.apache.kafka.connect.data.Schema SCHEMA =
      SchemaBuilder.struct()
          .field("a", Schema.STRING_SCHEMA)
          .field("b", Schema.STRING_SCHEMA)
          .build();

  private SinkRecord sinkRecord() {
    Struct struct = new Struct(SCHEMA);
    struct.put("a", "a");
    struct.put("b", "b");
    return new SinkRecord("some-topic", 0, null, null, SCHEMA, struct, 100L);
  }

  @Test
  @DisplayName("tableId throw exceptions for invalid table names")
  public void tableIdThrows() {
    Catalog catalog = mock(Catalog.class);
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    CatalogApi catalogApi = new BaseCatalogApi(catalog, config) {};
    assertThrows(WriteException.TableIdentifierException.class, () -> catalogApi.tableId(""));
  }

  @Test
  @DisplayName("loadTable should throw LoadTable exceptions wrapping underlying exceptions")
  public void loadTableThrows() {}

  @Test
  @DisplayName("schema should wrap exceptions")
  public void catalogApiSchemaShouldWrap() {
    Catalog catalog = mock(Catalog.class);
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);

    BiFunction<TableIdentifier, SinkRecord, org.apache.iceberg.Schema> illegalArgFn =
        (a, b) -> {
          throw new IllegalArgumentException("test");
        };

    BiFunction<TableIdentifier, SinkRecord, org.apache.iceberg.Schema> validationExceptionFn =
        (a, b) -> {
          throw new ValidationException("test");
        };

    CatalogApi catalogApiIllegalArg = new BaseCatalogApi(catalog, config, illegalArgFn);
    CatalogApi catalogApiValidationExp = new BaseCatalogApi(catalog, config, validationExceptionFn);

    assertThrows(
        WriteException.CreateSchemaException.class,
        () -> catalogApiIllegalArg.schema(DEAD_LETTER_TABLE_ID, sinkRecord()));
    assertThrows(
        WriteException.CreateSchemaException.class,
        () -> catalogApiValidationExp.schema(DEAD_LETTER_TABLE_ID, sinkRecord()));
  }

  @Test
  @DisplayName("partitionSpec should apply the configured PartitionSpec")
  public void catalogApiAppliesPartitionConfig() {
    Catalog catalog = mock(Catalog.class);
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    TableSinkConfig tableConfig =
        new TableSinkConfig(
            Pattern.compile(".*123", Pattern.DOTALL),
            Lists.newArrayList(),
            Lists.newArrayList("a"),
            null);

    when(config.tableConfig(ArgumentMatchers.any())).thenReturn(tableConfig);
    CatalogApi catalogApi = new BaseCatalogApi(catalog, config) {};

    org.apache.iceberg.Schema schema = catalogApi.schema(DEAD_LETTER_TABLE_ID, sinkRecord());
    assertThat(catalogApi.partitionSpec(DEAD_LETTER_TABLE, schema).isPartitioned()).isTrue();
  }

  @Test
  @DisplayName(".partitionSpec should create be unpartitioned if an error occurs")
  public void catalogApiPartitionSpecUnpartitioned() {
    Catalog catalog = mock(Catalog.class);
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    // partition on a field that does not exist
    TableSinkConfig tableConfig =
        new TableSinkConfig(
            Pattern.compile(".*123", Pattern.DOTALL),
            Lists.newArrayList(),
            Lists.newArrayList("does_not_exist"),
            null);

    when(config.tableConfig(ArgumentMatchers.any())).thenReturn(tableConfig);
    CatalogApi catalogApi = new BaseCatalogApi(catalog, config) {};

    org.apache.iceberg.Schema schema = catalogApi.schema(DEAD_LETTER_TABLE_ID, sinkRecord());
    assertThat(catalogApi.partitionSpec(DEAD_LETTER_TABLE, schema).isUnpartitioned()).isTrue();
  }

  @Test
  @DisplayName("createTable should throw CreateTable exceptions for underlying exceptions")
  public void catalogCreateTableShouldThrow() {
    Catalog catalogValidationException = mock(Catalog.class);
    when(catalogValidationException.createTable(
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()))
        .thenThrow(new ValidationException("test"));

    Catalog catalogIllegalArgException = mock(Catalog.class);
    when(catalogIllegalArgException.createTable(
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()))
        .thenThrow(new IllegalArgumentException("test"));

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    TableSinkConfig tableConfig =
        new TableSinkConfig(
            Pattern.compile(".*123", Pattern.DOTALL),
            Lists.newArrayList(),
            Lists.newArrayList("a"),
            null);

    when(config.tableConfig(ArgumentMatchers.any())).thenReturn(tableConfig);
    CatalogApi catalogApiValidation = new BaseCatalogApi(catalogValidationException, config) {};
    CatalogApi catalogApiIllegal = new BaseCatalogApi(catalogIllegalArgException, config) {};

    org.apache.iceberg.Schema schema =
        catalogApiValidation.schema(DEAD_LETTER_TABLE_ID, sinkRecord());

    assertThrows(
        WriteException.CreateTableException.class,
        () ->
            catalogApiValidation.createTable(
                DEAD_LETTER_TABLE_ID,
                schema,
                catalogApiValidation.partitionSpec(DEAD_LETTER_TABLE, schema),
                Maps.newHashMap()));
    assertThrows(
        WriteException.CreateTableException.class,
        () ->
            catalogApiIllegal.createTable(
                DEAD_LETTER_TABLE_ID,
                schema,
                catalogApiValidation.partitionSpec(DEAD_LETTER_TABLE, schema),
                Maps.newHashMap()));
  }
}
