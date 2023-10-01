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
        new Event("cg-connector", EventType.COMMIT_REQUEST, new CommitRequestPayload(commitId));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertThat(result.getType()).isEqualTo(event.getType());
    CommitRequestPayload payload = (CommitRequestPayload) result.getPayload();
    assertThat(payload.getCommitId()).isEqualTo(commitId);
  }

  @Test
  public void testCommitResponseSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            EventType.COMMIT_RESPONSE,
            new CommitResponsePayload(
                StructType.of(),
                commitId,
                new TableName(Collections.singletonList("db"), "tbl"),
                Arrays.asList(EventTestUtil.createDataFile(), EventTestUtil.createDataFile()),
                Arrays.asList(EventTestUtil.createDeleteFile(), EventTestUtil.createDeleteFile())));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertThat(result.getType()).isEqualTo(event.getType());
    CommitResponsePayload payload = (CommitResponsePayload) result.getPayload();
    assertThat(payload.getCommitId()).isEqualTo(commitId);
    assertThat(payload.getTableName().toIdentifier()).isEqualTo(TableIdentifier.parse("db.tbl"));
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
            "cg-connector",
            EventType.COMMIT_READY,
            new CommitReadyPayload(
                commitId,
                Arrays.asList(
                    new TopicPartitionOffset("topic", 1, 1L, 1L),
                    new TopicPartitionOffset("topic", 2, null, null))));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertThat(result.getType()).isEqualTo(event.getType());
    CommitReadyPayload payload = (CommitReadyPayload) result.getPayload();
    assertThat(payload.getCommitId()).isEqualTo(commitId);
    assertThat(payload.getAssignments()).hasSize(2);
    assertThat(payload.getAssignments()).allMatch(tp -> tp.getTopic().equals("topic"));
  }

  @Test
  public void testCommitTableSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector",
            EventType.COMMIT_TABLE,
            new CommitTablePayload(
                commitId, new TableName(Collections.singletonList("db"), "tbl"), 1L, 2L));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertThat(result.getType()).isEqualTo(event.getType());
    CommitTablePayload payload = (CommitTablePayload) result.getPayload();
    assertThat(payload.getCommitId()).isEqualTo(commitId);
    assertThat(payload.getTableName().toIdentifier()).isEqualTo(TableIdentifier.parse("db.tbl"));
    assertThat(payload.getSnapshotId()).isEqualTo(1L);
    assertThat(payload.getVtts()).isEqualTo(2L);
  }

  @Test
  public void testCommitCompleteSerialization() {
    UUID commitId = UUID.randomUUID();
    Event event =
        new Event(
            "cg-connector", EventType.COMMIT_COMPLETE, new CommitCompletePayload(commitId, 2L));

    byte[] data = Event.encode(event);
    Event result = Event.decode(data);

    assertThat(result.getType()).isEqualTo(event.getType());
    CommitCompletePayload payload = (CommitCompletePayload) result.getPayload();
    assertThat(payload.getCommitId()).isEqualTo(commitId);
    assertThat(payload.getVtts()).isEqualTo(2L);
  }
}
