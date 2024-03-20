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
package io.tabular.iceberg.connect.internal.worker;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.internal.data.IcebergWriterFactoryImpl;
import io.tabular.iceberg.connect.internal.kafka.AdminFactory;
import io.tabular.iceberg.connect.internal.kafka.ConsumerFactory;
import io.tabular.iceberg.connect.internal.kafka.TransactionalProducerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * A {@link WorkerManager} is responsible for managing the saving of records and responding to
 * commit requests.
 */
public class WorkerManager implements Closeable {

  private final Worker worker;
  private final CommitRequestListener commitRequestListener;

  public WorkerManager(
      SinkTaskContext context,
      IcebergSinkConfig config,
      Collection<TopicPartition> partitions,
      Catalog catalog,
      ConsumerFactory consumerFactory,
      TransactionalProducerFactory producerFactory,
      AdminFactory adminFactory) {
    this(
        new MultiPartitionWorker(
            context,
            config,
            ImmutableSet.copyOf(partitions),
            new PartitionWorkerFactory(
                config, new IcebergWriterFactoryImpl(catalog, config), producerFactory),
            adminFactory),
        new ControlTopicCommitRequestListener(config, consumerFactory));
  }

  @VisibleForTesting
  WorkerManager(Worker worker, CommitRequestListener commitRequestListener) {
    this.worker = worker;
    this.commitRequestListener = commitRequestListener;
  }

  public void save(Collection<SinkRecord> sinkRecords) {
    worker.save(sinkRecords);
    commit();
  }

  public void commit() {
    commitRequestListener.getCommitId().ifPresent(worker::commit);
  }

  @Override
  public void close() throws IOException {
    worker.close();
    commitRequestListener.close();
  }
}
