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

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.WriteExceptionHandler;
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.Utilities;
import io.tabular.iceberg.connect.data.WriterResult;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

class TableWriterImpl implements TableWriter, AutoCloseable {

  private final TableWriter underlying;

  private WriteExceptionHandler loadHandler(String name) {
    ClassLoader loader = this.getClass().getClassLoader();
    final Object obj;
    try {
      Class<?> clazz = Class.forName(name, true, loader);
      obj = clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(String.format("Could not initialize class %s", name), e);
    }

    final WriteExceptionHandler writeExceptionHandler = (WriteExceptionHandler) obj;

    return writeExceptionHandler;
  }

  TableWriterImpl(
      SinkTaskContext context, IcebergSinkConfig config, IcebergWriterFactory writerFactory) {
    MultiTableWriter baseTableWriter = new MultiTableWriter(writerFactory);
    if (config.writeExceptionHandlerClassName() == null) {
      this.underlying = baseTableWriter;
    } else {
      WriteExceptionHandler handler = loadHandler(config.writeExceptionHandlerClassName());
      handler.initialize(context, config);
      this.underlying = new ExceptionHandlingTableWriter(baseTableWriter, handler);
    }
  }

  @Override
  public void write(SinkRecord sinkRecord, String tableName, boolean ignoreMissingTable) {
    underlying.write(sinkRecord, tableName, ignoreMissingTable);
  }

  @Override
  public List<WriterResult> committable() {
    return underlying.committable();
  }

  @Override
  public void close() {
    Utilities.close(underlying);
  }
}
