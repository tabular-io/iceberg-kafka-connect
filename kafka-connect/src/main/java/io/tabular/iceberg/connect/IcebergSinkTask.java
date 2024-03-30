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
package io.tabular.iceberg.connect;

import io.tabular.iceberg.connect.api.Committer;
import io.tabular.iceberg.connect.api.CommitterFactory;
import io.tabular.iceberg.connect.api.Writer;
import io.tabular.iceberg.connect.writer.WriterImpl;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkTask extends SinkTask {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkTask.class);

  private IcebergSinkConfig config;
  private Writer writer;

  private Committer committer;

  @Override
  public String version() {
    return IcebergSinkConfig.version();
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new IcebergSinkConfig(props);
  }

  private void clearState() {
    if (writer != null) {
      try {
        writer.close();
      } catch (IOException e) {
        LOG.warn("An error occurred closing worker instance, ignoring...", e);
      }
      writer = null;
    }

    if (committer != null) {
      try {
        committer.close();
      } catch (IOException e) {
        LOG.warn("An error occurred closing worker instance, ignoring...", e);
      }
      committer = null;
    }
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    // destroy any state if KC re-uses object
    clearState();

    ImmutableSet<TopicPartition> topicPartitions = ImmutableSet.copyOf(partitions);

    this.writer = new WriterImpl(config, topicPartitions);

    final CommitterFactory committerFactory;
    try {
      Class<CommitterFactory> committerFactoryClass =
          (Class<CommitterFactory>) Class.forName(config.committerFactoryClass());
      committerFactory = committerFactoryClass.newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.committer = committerFactory.create(context, config, topicPartitions);
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    writer.put(sinkRecords);
    committer.commit(writer);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {}

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    return ImmutableMap.of();
  }

  @Override
  public void stop() {
    clearState();
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    clearState();
  }
}
