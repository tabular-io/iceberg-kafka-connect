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

import org.apache.iceberg.catalog.TableIdentifier;

public class WriteException extends RuntimeException {

  private final String tableIdentifier;

  WriteException(Throwable cause) {
    super(cause);
    tableIdentifier = null;
  }

  WriteException(TableIdentifier tableId, Throwable cause) {
    super(cause);
    this.tableIdentifier = tableId.toString();
  }

  WriteException(String tableId, Throwable cause) {
    super(cause);
    this.tableIdentifier = tableId;
  }

  public String tableId() {
    return tableIdentifier;
  }

  public static class CdcException extends WriteException {
    public CdcException(Throwable cause) {
      super(cause);
    }
  }

  public static class CreateTableException extends WriteException {

    public CreateTableException(TableIdentifier identifier, Throwable cause) {
      super(identifier, cause);
    }
  }

  public static class CreateSchemaException extends WriteException {
    public CreateSchemaException(TableIdentifier identifier, Throwable cause) {
      super(identifier, cause);
    }
  }

  public static class LoadTableException extends WriteException {

    public LoadTableException(TableIdentifier identifier, Throwable cause) {
      super(identifier, cause);
    }
  }

  public static class RecordConversionException extends WriteException {

    RecordConversionException(Throwable cause) {
      super(cause);
    }
  }

  public static class RouteException extends WriteException {
    RouteException(Throwable cause) {
      super(cause);
    }
  }

  public static class RouteRegexException extends WriteException {
    RouteRegexException(Throwable cause) {
      super(cause);
    }
  }

  public static class SchemaEvolutionException extends WriteException {

    SchemaEvolutionException(String name, Throwable cause) {
      super(name, cause);
    }
  }

  public static class TableIdentifierException extends WriteException {
    TableIdentifierException(String name, Throwable cause) {
      super(name, cause);
    }
  }
}
