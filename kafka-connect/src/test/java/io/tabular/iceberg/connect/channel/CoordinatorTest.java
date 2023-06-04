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

import static io.tabular.iceberg.connect.channel.EventTestUtil.createDataFile;
import static io.tabular.iceberg.connect.channel.EventTestUtil.createDeleteFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.channel.events.CommitReadyPayload;
import io.tabular.iceberg.connect.channel.events.CommitRequestPayload;
import io.tabular.iceberg.connect.channel.events.CommitResponsePayload;
import io.tabular.iceberg.connect.channel.events.Event;
import io.tabular.iceberg.connect.channel.events.EventType;
import io.tabular.iceberg.connect.channel.events.TableName;
import io.tabular.iceberg.connect.channel.events.TopicPartitionOffset;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

public class CoordinatorTest extends ChannelTestBase {
  @Test
  public void testCommitAppend() throws IOException {
    coordinatorTest(ImmutableList.of(createDataFile()), ImmutableList.of());
    verify(appendOp).commit();
    verify(deltaOp, times(0)).commit();
  }

  @Test
  public void testCommitDelta() throws IOException {
    coordinatorTest(ImmutableList.of(createDataFile()), ImmutableList.of(createDeleteFile()));
    verify(appendOp, times(0)).commit();
    verify(deltaOp).commit();
  }

  private void coordinatorTest(List<DataFile> dataFiles, List<DeleteFile> deleteFiles)
      throws IOException {
    when(config.getCommitIntervalMs()).thenReturn(0);
    when(config.getCommitTimeoutMs()).thenReturn(5000);

    Coordinator coordinator = new Coordinator(catalog, config, clientFactory);
    coordinator.start();

    // init consumer after subscribe()
    initConsumer();

    coordinator.process();

    assertTrue(producer.transactionCommitted());
    assertEquals(1, producer.history().size());
    verify(appendOp, times(0)).commit();
    verify(deltaOp, times(0)).commit();

    byte[] bytes = producer.history().get(0).value();
    Event commitRequest = AvroEncoderUtil.decode(bytes);
    assertEquals(EventType.COMMIT_REQUEST, commitRequest.getType());

    UUID commitId = ((CommitRequestPayload) commitRequest.getPayload()).getCommitId();
    Event commitResponse =
        new Event(
            EventType.COMMIT_RESPONSE,
            new CommitResponsePayload(
                StructType.of(),
                commitId,
                new TableName(ImmutableList.of("db"), "tbl"),
                dataFiles,
                deleteFiles));
    bytes = AvroEncoderUtil.encode(commitResponse, commitResponse.getSchema());
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    Event commitReady =
        new Event(
            EventType.COMMIT_READY,
            new CommitReadyPayload(
                commitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, 1L))));
    bytes = AvroEncoderUtil.encode(commitReady, commitReady.getSchema());
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", bytes));

    when(config.getCommitIntervalMs()).thenReturn(0);

    coordinator.process();
  }
}
