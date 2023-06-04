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
package io.tabular.iceberg.connect.channel.events;

import static io.tabular.iceberg.connect.channel.EventTestUtil.createDataFile;
import static io.tabular.iceberg.connect.channel.EventTestUtil.createDeleteFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class EventTest {

  @Test
  public void testCommitRequestSerialization() throws Exception {
    UUID commitId = UUID.randomUUID();
    Event event = new Event(EventType.COMMIT_REQUEST, new CommitRequestPayload(commitId));

    byte[] data = AvroEncoderUtil.encode(event, event.getSchema());
    Event result = AvroEncoderUtil.decode(data);

    assertEquals(event.getType(), result.getType());
    CommitRequestPayload payload = (CommitRequestPayload) result.getPayload();
    assertEquals(commitId, payload.getCommitId());
  }

  @Test
  public void testCommitResponseSerialization() throws Exception {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            EventType.COMMIT_RESPONSE,
            new CommitResponsePayload(
                StructType.of(),
                commitId,
                new TableName(ImmutableList.of("db"), "tbl"),
                ImmutableList.of(createDataFile(), createDataFile()),
                ImmutableList.of(createDeleteFile(), createDeleteFile())));

    byte[] data = AvroEncoderUtil.encode(event, event.getSchema());
    Event result = AvroEncoderUtil.decode(data);

    assertEquals(event.getType(), result.getType());
    CommitResponsePayload payload = (CommitResponsePayload) result.getPayload();
    assertEquals(commitId, payload.getCommitId());
    assertThat(payload.getDataFiles()).hasSize(2);
    assertThat(payload.getDataFiles()).allMatch(f -> f.specId() == 1);
    assertThat(payload.getDeleteFiles()).hasSize(2);
    assertThat(payload.getDeleteFiles()).allMatch(f -> f.specId() == 1);
  }

  @Test
  public void testCommitReadySerialization() throws Exception {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            EventType.COMMIT_READY,
            new CommitReadyPayload(
                commitId,
                ImmutableList.of(
                    new TopicPartitionOffset("topic", 1, 1L, 1L),
                    new TopicPartitionOffset("topic", 2, null, null))));

    byte[] data = AvroEncoderUtil.encode(event, event.getSchema());
    Event result = AvroEncoderUtil.decode(data);

    assertEquals(event.getType(), result.getType());
    CommitReadyPayload payload = (CommitReadyPayload) result.getPayload();
    assertEquals(commitId, payload.getCommitId());
    assertThat(payload.getAssignments()).hasSize(2);
    assertThat(payload.getAssignments()).allMatch(tp -> tp.getTopic().equals("topic"));
  }
}
