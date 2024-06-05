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
package io.tabular.iceberg.connect.exception;

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Map;

public class DeadLetterTableWriteExceptionHandler implements WriteExceptionHandler {
  private FailedRecordFactory factory;

  @Override
  public void initialize(
      SinkTaskContext context, Map<String, String> props) {
    String failedRecordFactoryClass = props.get("failed_record_factory");
    factory = (FailedRecordFactory) DeadLetterUtils.loadClass(failedRecordFactoryClass, this.getClass().getClassLoader());
    factory.configure(props);
  }

  @Override
  public SinkRecord handle(SinkRecord record, Exception exception) {
    if (exception instanceof WriteException) {
      return handleWriteException(record, (WriteException) exception);
    }
    Throwable cause = exception.getCause();
    if (cause instanceof WriteException) {
      return handleWriteException(record, (WriteException) cause);
    }
    throw new RuntimeException(exception);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private SinkRecord handleWriteException(SinkRecord record, WriteException exception) {
    if (exception instanceof WriteException.CreateTableException) {
      Throwable cause = exception.getCause();
      if (cause instanceof IllegalArgumentException || cause instanceof ValidationException) {
        return failedRecord(record, exception);
      }
    } else if (exception instanceof WriteException.CreateSchemaException) {
      return failedRecord(record, exception);
    } else if (exception instanceof WriteException.LoadTableException) {
      Throwable cause = exception.getCause();
      if (cause instanceof IllegalArgumentException || cause instanceof ValidationException) {
        return failedRecord(record, exception);
      }
    } else if (exception instanceof WriteException.RecordConversionException) {
      return failedRecord(record, exception);

    } else if (exception instanceof WriteException.RouteException) {
      return failedRecord(record, exception);

    } else if (exception instanceof WriteException.RouteRegexException) {
      return failedRecord(record, exception);

    } else if (exception instanceof WriteException.SchemaEvolutionException) {
      Throwable cause = exception.getCause();
      if (cause instanceof IllegalArgumentException
          || cause instanceof ValidationException
          || cause instanceof UnsupportedOperationException) {
        return failedRecord(record, exception);
      }
    } else if (exception instanceof WriteException.TableIdentifierException) {
      return failedRecord(record, exception);
    }
    throw exception;
  }

  private SinkRecord failedRecord(SinkRecord record, WriteException exception) {
    return factory.recordFromConnector(record, exception);
  }
}
