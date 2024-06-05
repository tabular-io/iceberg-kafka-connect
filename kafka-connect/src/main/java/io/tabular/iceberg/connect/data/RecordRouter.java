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
import io.tabular.iceberg.connect.exception.DeadLetterUtils;

import java.util.List;

import io.tabular.iceberg.connect.exception.WriteException;
import io.tabular.iceberg.connect.exception.WriteExceptionHandler;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

public abstract class RecordRouter {

  public void write(SinkRecord record) {}

  protected final String extractRouteValue(Object recordValue, String routeField) {
    Object routeValue;
    if (recordValue == null) {
      return null;
    }
    try {
      routeValue = Utilities.extractFromRecordValue(recordValue, routeField);
    } catch (Exception error) {
      throw new WriteException.RouteException(error);
    }

    return routeValue == null ? null : routeValue.toString();
  }

  public static RecordRouter from(
          WriterManager writers,
          IcebergSinkConfig config,
          ClassLoader loader,
          SinkTaskContext context) {
    RecordRouter baseRecordRouter;

    if (config.dynamicTablesEnabled()) {
      Preconditions.checkNotNull(
              config.tablesRouteField(), "Route field cannot be null with dynamic routing");
      baseRecordRouter = new DynamicRecordRouter(writers, config.tablesRouteField());
    } else {
      if (config.tables() != null && !config.tables().isEmpty()) {
        config.tables().forEach(TableIdentifier::of);
        if (config.tablesRouteField() != null) {
          if (hasRegexMode(config)) {
            baseRecordRouter = new RegexRecordRouter(writers, config);
          } else {
            baseRecordRouter = new FallbackRecordRouter(new DynamicRecordRouter(writers, config.tablesRouteField()), new ConfigRecordRouter(writers, config.tables()));
          }
        } else {
          baseRecordRouter = new ConfigRecordRouter(writers, config.tables());
        }
      } else {
        baseRecordRouter = new RegexRecordRouter(writers, config);
      }
    }

    if (config.deadLetterTableEnabled()) {
      String handlerClass = config.getWriteExceptionHandler();
      WriteExceptionHandler handler =
              (WriteExceptionHandler) DeadLetterUtils.loadClass(handlerClass, loader);
      handler.initialize(context, config.writeExceptionHandlerProperties());
      baseRecordRouter =
              new RecordRouter.ErrorHandlingRecordRouter(baseRecordRouter, handler);
    }

    return baseRecordRouter;
  }

  private static boolean hasRegexMode(IcebergSinkConfig config) {
    long definedRegexes = config
            .tables()
            .stream()
            .map(
                    tableName -> {
                      try {
return                         config
                                .tableConfig(tableName)
                                .routeRegex().isPresent();
                      } catch (Exception unused) {
                        return false;
                      }
                    }).filter(present -> present).count();
    return definedRegexes > 0;
  }

  public static class ConfigRecordRouter extends RecordRouter {
    private final List<String> tables;
    private final WriterManager writers;

    ConfigRecordRouter(WriterManager writers, List<String> tables) {
      this.tables = tables;
      this.writers = writers;
    }

    @Override
    public void write(SinkRecord record) {
      // route to all tables
      tables.forEach(
              tableName -> {
                writers.write(tableName, record, false);
              });
    }
  }

  public static class RegexRecordRouter extends RecordRouter {
    private final String routeField;
    private final WriterManager writers;
    private final IcebergSinkConfig config;

    RegexRecordRouter(WriterManager writers, IcebergSinkConfig config) {
      this.routeField = config.tablesRouteField();
      this.writers = writers;
      this.config = config;
    }

    @Override
    public void write(SinkRecord record) {
      String routeValue = extractRouteValue(record.value(), routeField);
      if (routeValue != null) {
        config
                .tables()
                .forEach(
                        tableName ->
                                config
                                        .tableConfig(tableName)
                                        .routeRegex()
                                        .ifPresent(
                                                regex -> {
                                                  boolean matches;
                                                  try {
                                                    matches = regex.matcher(routeValue).matches();
                                                  } catch (Exception error) {
                                                    throw new WriteException.RouteRegexException(error);
                                                  }
                                                  if (matches) {
                                                    writers.write(tableName, record, false);
                                                  }
                                                }));
      }
    }
  }

  public static class DynamicRecordRouter extends RecordRouter {
    private final String routeField;
    private final WriterManager writers;

    DynamicRecordRouter(WriterManager writers, String routeField) {
      this.routeField = routeField;
      this.writers = writers;
    }

    @Override
    public void write(SinkRecord record) {
      String routeValue = extractRouteValue(record.value(), routeField);
      if (routeValue != null) {
        String tableName = routeValue.toLowerCase();
        writers.write(tableName, record, true);
      }
    }
  }

  public static class FallbackRecordRouter extends RecordRouter {
    private final RecordRouter primary;
    private final RecordRouter fallback;

    FallbackRecordRouter(RecordRouter primary, RecordRouter fallback) {
      this.primary = primary;
      this.fallback = fallback;
    }

    public void write(SinkRecord record) {
      try {
        primary.write(record);
      } catch (Exception error) {
        fallback.write(record);
      }
    }
  }

  public static class ErrorHandlingRecordRouter extends RecordRouter {
    private final WriteExceptionHandler handler;
    private final RecordRouter router;

    ErrorHandlingRecordRouter(
            RecordRouter baseRouter,
            WriteExceptionHandler handler) {
      this.router = baseRouter;
      this.handler = handler;
    }

    @Override
    public void write(SinkRecord record) {
      try {
        router.write(record);
      } catch (Exception error) {
        SinkRecord result = handler.handle(record, error);
        router.write(result);
      }
    }
  }
}
