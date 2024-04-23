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
package io.tabular.iceberg.connect.handler;

import io.tabular.iceberg.connect.DeadLetterTable;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.WriteExceptionHandler;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/** Sample DLQ implementation that sends records to a fixed dead-letter table. */
public class IcebergDLQ implements WriteExceptionHandler {
  @Override
  public void initialize(SinkTaskContext context, IcebergSinkConfig config) {}

  @Override
  public Result handle(SinkRecord sinkRecord, String tableName, Exception exception) {
    return new WriteExceptionHandler.Result(
        // Users could customize here and create a more sophisticated SinkRecord that includes:
        // - key/value bytes
        // - connector name
        // - connector version
        // etc.
        DeadLetterTable.sinkRecord(sinkRecord, exception, DeadLetterTable.NAME),
        // Users could customize here and make a dead-letter table per topic or whatever they want
        DeadLetterTable.NAME);
  }

  @Override
  public void preCommit() {}
}
