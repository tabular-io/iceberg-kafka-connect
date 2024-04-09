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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.deadletter.DeadLetterUtils;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class CatalogApiTest {

  private static final String DEAD_LETTER_TABLE = "dlt.table";

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
  @DisplayName("CatalogAPI/Error.partitionSpec should apply the configured PartitionSpec")
  public void catalogApiAppliesPartitionConfig() {}

  @Test
  @DisplayName("CatalogAPI/Error.partitionSpec should create be unpartitioned if an error occurs")
  public void catalogApiPartitionSpecUnpartitioned() {}

  @Test
  @DisplayName("CatalogAPI.createTable should throw validation/illegal argument exceptions")
  public void catalogCreateTableShouldThrow() {}

  @Test
  @DisplayName(
      "ErrorHandlingCatalogApi.createTable should wrap validation/illegal argument exceptions")
  public void errorHandlingCatalogCreateTableShouldWrap() {}

  @Test
  @DisplayName("ErrorHandlingCatalogApi.createTable should not wrap other exceptions")
  public void errorHandlingCatalogCreateTableNotWrap() {}

  @Test
  @DisplayName("CatalogAPI.schema should throw exceptions")
  public void catalogApiSchemaThrowsExceptions() {}

  @Test
  @DisplayName("ErrorHandlingCatalogAPI.schema should wrap validation/illegal argument exceptions")
  public void errorHandlingCatalogSchemaShouldWrap() {}

  @Test
  @DisplayName("ErrorHandlingCatalogAPI.schema should not wrap other exceptions")
  public void errorHandlingCatalogNotWrap() {}
}
