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

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.WriteExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * Sends any record that can't be handled to Kafka.
 *
 * <p>Provides only at-least-once guarantees which is a limitation of the Kafka Connect {@link
 * ErrantRecordReporter} API.
 */
class KafkaDLQ implements WriteExceptionHandler {
  private SinkTaskContext context;

  private List<Future<Void>> pendingFutures;

  @SuppressWarnings("RegexpSingleline")
  @Override
  public void initialize(SinkTaskContext sinkTaskContext, IcebergSinkConfig config) {
    this.context = sinkTaskContext;
    this.pendingFutures = new ArrayList<>();
  }

  private boolean isDone(Future<Void> voidFuture) {
    if (voidFuture.isDone()) {
      try {
        voidFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Result handle(SinkRecord sinkRecord, String tableName, Exception exception) {
    // TODO: I would much rather do something smarter with callbacks
    //  but I only get the old java.util.concurrent.Future interface here >_<
    pendingFutures.add(context.errantRecordReporter().report(sinkRecord, exception));
    pendingFutures.removeIf(this::isDone);
    return null;
  }

  @Override
  public void preCommit() {
    pendingFutures.forEach(
        voidFuture -> {
          try {
            voidFuture.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });
  }
}
