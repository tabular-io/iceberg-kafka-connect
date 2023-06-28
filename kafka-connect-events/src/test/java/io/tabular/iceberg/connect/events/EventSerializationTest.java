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
package io.tabular.iceberg.connect.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class EventSerializationTest {

  @Test
  public void testCommitRequestSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event("connector", EventType.COMMIT_REQUEST, new CommitRequestPayload(commitId));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertEquals(event.getType(), result.getType());
    CommitRequestPayload payload = (CommitRequestPayload) result.getPayload();
    assertEquals(commitId, payload.getCommitId());
  }

  @Test
  public void testCommitResponseSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "connector",
            EventType.COMMIT_RESPONSE,
            new CommitResponsePayload(
                StructType.of(),
                commitId,
                new TableName(Collections.singletonList("db"), "tbl"),
                Arrays.asList(EventTestUtil.createDataFile(), EventTestUtil.createDataFile()),
                Arrays.asList(EventTestUtil.createDeleteFile(), EventTestUtil.createDeleteFile())));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertEquals(event.getType(), result.getType());
    CommitResponsePayload payload = (CommitResponsePayload) result.getPayload();
    assertEquals(commitId, payload.getCommitId());
    assertEquals(TableIdentifier.parse("db.tbl"), payload.getTableName().toIdentifier());
    assertThat(payload.getDataFiles()).hasSize(2);
    assertThat(payload.getDataFiles()).allMatch(f -> f.specId() == 1);
    assertThat(payload.getDeleteFiles()).hasSize(2);
    assertThat(payload.getDeleteFiles()).allMatch(f -> f.specId() == 1);
  }

  @Test
  public void testCommitReadySerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "connector",
            EventType.COMMIT_READY,
            new CommitReadyPayload(
                commitId,
                Arrays.asList(
                    new TopicPartitionOffset("topic", 1, 1L, 1L),
                    new TopicPartitionOffset("topic", 2, null, null))));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertEquals(event.getType(), result.getType());
    CommitReadyPayload payload = (CommitReadyPayload) result.getPayload();
    assertEquals(commitId, payload.getCommitId());
    assertThat(payload.getAssignments()).hasSize(2);
    assertThat(payload.getAssignments()).allMatch(tp -> tp.getTopic().equals("topic"));
  }

  @Test
  public void testCommitTableSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "connector",
            EventType.COMMIT_TABLE,
            new CommitTablePayload(
                commitId, new TableName(Collections.singletonList("db"), "tbl"), 1L, 2L));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertEquals(event.getType(), result.getType());
    CommitTablePayload payload = (CommitTablePayload) result.getPayload();
    assertEquals(commitId, payload.getCommitId());
    assertEquals(TableIdentifier.parse("db.tbl"), payload.getTableName().toIdentifier());
    assertEquals(1L, payload.getSnapshotId());
    assertEquals(2L, payload.getVtts());
  }

  @Test
  public void testCommitCompleteSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event("connector", EventType.COMMIT_COMPLETE, new CommitCompletePayload(commitId, 2L));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertEquals(event.getType(), result.getType());
    CommitCompletePayload payload = (CommitCompletePayload) result.getPayload();
    assertEquals(commitId, payload.getCommitId());
    assertEquals(2L, payload.getVtts());
  }
}
