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
package io.tabular.iceberg.connect.channel;

import io.tabular.iceberg.connect.WriteExceptionHandler;
import io.tabular.iceberg.connect.data.Utilities;
import io.tabular.iceberg.connect.data.WriterResult;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;

class ExceptionHandlingTableWriter implements TableWriter, AutoCloseable {

  private final TableWriter underlying;
  private final WriteExceptionHandler exceptionHandler;

  ExceptionHandlingTableWriter(TableWriter underlying, WriteExceptionHandler exceptionHandler) {
    this.underlying = underlying;
    this.exceptionHandler = exceptionHandler;
  }

  @Override
  public void write(SinkRecord sinkRecord, String tableName, boolean ignoreMissingTable) {
    try {
      underlying.write(sinkRecord, tableName, ignoreMissingTable);
    } catch (Exception exception) {
      final WriteExceptionHandler.Result result;
      try {
        result = exceptionHandler.handle(sinkRecord, tableName, exception);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      if (result != null) {
        // TODO: ignoreMissingTables=false make sense here? I _think_ so but I could also expose
        // it
        write(result.sinkRecord(), result.tableName(), false);
      }
    }
  }

  @Override
  public List<WriterResult> committable() {
    exceptionHandler.preCommit();
    return underlying.committable();
  }

  @Override
  public void close() {
    Utilities.close(exceptionHandler);
    Utilities.close(underlying);
  }
}
