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
import io.tabular.iceberg.connect.deadletter.DeadLetterUtils;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
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

public class CatalogApiTest {

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
  @DisplayName("CatalogApi.tableId throw exceptions for invalid table names")
  public void tableIdThrows() {
    Catalog catalog = mock(Catalog.class);
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    CatalogApi defaultCatalog = new CatalogApi(catalog, config) {};
    assertThrows(IllegalArgumentException.class, () -> defaultCatalog.tableId(""));
  }

  @Test
  @DisplayName(
      "ErrorHandlingCatalogApi.tableId should throw DeadLetterExceptions for invalid table names")
  public void errorTableIdThrows() {
    Catalog catalog = mock(Catalog.class);
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.deadLetterTableName()).thenReturn(DEAD_LETTER_TABLE);
    CatalogApi errorHandlingCatalog = new CatalogApi.ErrorHandlingCatalogApi(catalog, config);
    assertThrows(DeadLetterUtils.DeadLetterException.class, () -> errorHandlingCatalog.tableId(""));
  }

  @Test
  @DisplayName(
      "ErrorHandlingCatalog constructor should throw if dead letter table has invalid name")
  public void errorHandlingCatalogConstructorThrows() {
    Catalog catalog = mock(Catalog.class);
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.deadLetterTableName()).thenReturn("");
    assertThrows(
        IllegalArgumentException.class,
        () -> new CatalogApi.ErrorHandlingCatalogApi(catalog, config));
  }

  @Test
  @DisplayName("ErrorHandlingCatalogAPI.schema should wrap validation/illegal argument exceptions")
  public void errorHandlingCatalogSchemaShouldWrap() {
    Catalog catalog = mock(Catalog.class);
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.deadLetterTableName()).thenReturn(DEAD_LETTER_TABLE);

    BiFunction<TableIdentifier, SinkRecord, org.apache.iceberg.Schema> illegalArgFn =
        (a, b) -> {
          throw new IllegalArgumentException("test");
        };

    BiFunction<TableIdentifier, SinkRecord, org.apache.iceberg.Schema> validationExceptionFn =
        (a, b) -> {
          throw new ValidationException("test");
        };

    CatalogApi catalogApiIllegalArg =
        new CatalogApi.ErrorHandlingCatalogApi(catalog, config, illegalArgFn);
    CatalogApi catalogApiValidationExp =
        new CatalogApi.ErrorHandlingCatalogApi(catalog, config, validationExceptionFn);

    assertThrows(
        DeadLetterUtils.DeadLetterException.class,
        () -> catalogApiIllegalArg.schema(DEAD_LETTER_TABLE_ID, sinkRecord()));
    assertThrows(
        DeadLetterUtils.DeadLetterException.class,
        () -> catalogApiValidationExp.schema(DEAD_LETTER_TABLE_ID, sinkRecord()));
  }

  @Test
  @DisplayName("CatalogAPI/Error.partitionSpec should apply the configured PartitionSpec")
  public void catalogApiAppliesPartitionConfig() {
    Catalog catalog = mock(Catalog.class);
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.deadLetterTableName()).thenReturn(DEAD_LETTER_TABLE);
    TableSinkConfig tableConfig =
        new TableSinkConfig(
            Pattern.compile(".*123", Pattern.DOTALL),
            Lists.newArrayList(),
            Lists.newArrayList("a"),
            null);

    when(config.tableConfig(ArgumentMatchers.any())).thenReturn(tableConfig);
    CatalogApi catalogApi = new CatalogApi(catalog, config) {};
    CatalogApi errorApi = new CatalogApi.ErrorHandlingCatalogApi(catalog, config);

    org.apache.iceberg.Schema schema = catalogApi.schema(DEAD_LETTER_TABLE_ID, sinkRecord());
    assertThat(catalogApi.partitionSpec(DEAD_LETTER_TABLE, schema).isPartitioned()).isTrue();
    assertThat(errorApi.partitionSpec(DEAD_LETTER_TABLE, schema).isPartitioned()).isTrue();
  }

  @Test
  @DisplayName("CatalogAPI/Error.partitionSpec should create be unpartitioned if an error occurs")
  public void catalogApiPartitionSpecUnpartitioned() {
    Catalog catalog = mock(Catalog.class);
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.deadLetterTableName()).thenReturn(DEAD_LETTER_TABLE);
    // partition on a field that does not exist
    TableSinkConfig tableConfig =
        new TableSinkConfig(
            Pattern.compile(".*123", Pattern.DOTALL),
            Lists.newArrayList(),
            Lists.newArrayList("does_not_exist"),
            null);

    when(config.tableConfig(ArgumentMatchers.any())).thenReturn(tableConfig);
    CatalogApi catalogApi = new CatalogApi(catalog, config) {};
    CatalogApi errorApi = new CatalogApi.ErrorHandlingCatalogApi(catalog, config);

    org.apache.iceberg.Schema schema = catalogApi.schema(DEAD_LETTER_TABLE_ID, sinkRecord());
    assertThat(catalogApi.partitionSpec(DEAD_LETTER_TABLE, schema).isUnpartitioned()).isTrue();
    assertThat(errorApi.partitionSpec(DEAD_LETTER_TABLE, schema).isUnpartitioned()).isTrue();
  }

  @Test
  @DisplayName("CatalogAPI.createTable should throw validation/illegal argument exceptions")
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
    when(config.deadLetterTableName()).thenReturn(DEAD_LETTER_TABLE);
    TableSinkConfig tableConfig =
        new TableSinkConfig(
            Pattern.compile(".*123", Pattern.DOTALL),
            Lists.newArrayList(),
            Lists.newArrayList("a"),
            null);

    when(config.tableConfig(ArgumentMatchers.any())).thenReturn(tableConfig);
    CatalogApi catalogApiValidation = new CatalogApi(catalogValidationException, config) {};
    CatalogApi catalogApiIllegal = new CatalogApi(catalogIllegalArgException, config) {};

    org.apache.iceberg.Schema schema =
        catalogApiValidation.schema(DEAD_LETTER_TABLE_ID, sinkRecord());

    assertThrows(
        ValidationException.class,
        () ->
            catalogApiValidation.createTable(
                DEAD_LETTER_TABLE_ID,
                schema,
                catalogApiValidation.partitionSpec(DEAD_LETTER_TABLE, schema),
                Maps.newHashMap()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalogApiIllegal.createTable(
                DEAD_LETTER_TABLE_ID,
                schema,
                catalogApiValidation.partitionSpec(DEAD_LETTER_TABLE, schema),
                Maps.newHashMap()));
  }

  @Test
  @DisplayName(
      "ErrorHandlingCatalogApi.createTable should wrap validation/illegal argument exceptions")
  public void errorHandlingCatalogCreateTableShouldWrap() {
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
    when(config.deadLetterTableName()).thenReturn(DEAD_LETTER_TABLE);
    TableSinkConfig tableConfig =
        new TableSinkConfig(
            Pattern.compile(".*123", Pattern.DOTALL),
            Lists.newArrayList(),
            Lists.newArrayList("a"),
            null);

    when(config.tableConfig(ArgumentMatchers.any())).thenReturn(tableConfig);
    CatalogApi catalogApiValidation =
        new CatalogApi.ErrorHandlingCatalogApi(catalogValidationException, config) {};
    CatalogApi catalogApiIllegal =
        new CatalogApi.ErrorHandlingCatalogApi(catalogIllegalArgException, config) {};

    org.apache.iceberg.Schema schema =
        catalogApiValidation.schema(DEAD_LETTER_TABLE_ID, sinkRecord());

    assertThrows(
        DeadLetterUtils.DeadLetterException.class,
        () ->
            catalogApiValidation.createTable(
                DEAD_LETTER_TABLE_ID,
                schema,
                catalogApiValidation.partitionSpec(DEAD_LETTER_TABLE, schema),
                Maps.newHashMap()));
    assertThrows(
        DeadLetterUtils.DeadLetterException.class,
        () ->
            catalogApiIllegal.createTable(
                DEAD_LETTER_TABLE_ID,
                schema,
                catalogApiValidation.partitionSpec(DEAD_LETTER_TABLE, schema),
                Maps.newHashMap()));
  }
}
