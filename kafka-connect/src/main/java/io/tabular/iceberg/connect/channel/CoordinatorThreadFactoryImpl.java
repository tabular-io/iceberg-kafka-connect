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
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CoordinatorThreadFactoryImpl implements CoordinatorThreadFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorThreadFactoryImpl.class);
  private final KafkaClientFactory kafkaClientFactory;
  private final Catalog catalog;

  CoordinatorThreadFactoryImpl(Catalog catalog, KafkaClientFactory kafkaClientFactory) {
    this.kafkaClientFactory = kafkaClientFactory;
    this.catalog = catalog;
  }

  private static class TopicPartitionComparator implements Comparator<TopicPartition> {
    @Override
    public int compare(TopicPartition o1, TopicPartition o2) {
      int result = o1.topic().compareTo(o2.topic());
      if (result == 0) {
        result = Integer.compare(o1.partition(), o2.partition());
      }
      return result;
    }
  }

  private boolean isLeader(
      Collection<MemberDescription> members, Collection<TopicPartition> partitions) {
    // there should only be one task assigned partition 0 of the first topic,
    // so elect that one the leader
    TopicPartition firstTopicPartition =
        members.stream()
            .flatMap(member -> member.assignment().topicPartitions().stream())
            .min(new TopicPartitionComparator())
            .orElseThrow(
                () -> new ConnectException("No partitions assigned, cannot determine leader"));

    return partitions.contains(firstTopicPartition);
  }

  @Override
  public Optional<CoordinatorThread> create(SinkTaskContext context, IcebergSinkConfig config) {
    CoordinatorThread thread = null;

    ConsumerGroupDescription groupDesc;
    try (Admin admin = kafkaClientFactory.createAdmin()) {
      groupDesc = KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin);
    }

    if (groupDesc.state() == ConsumerGroupState.STABLE) {
      Collection<MemberDescription> members = groupDesc.members();
      if (isLeader(members, context.assignment())) {
        LOG.info("Task elected leader, starting commit coordinator");
        Coordinator coordinator = new Coordinator(catalog, config, members, kafkaClientFactory);
        thread = new CoordinatorThread(coordinator);
        thread.start();
      }
    }

    return Optional.ofNullable(thread);
  }
}
