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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.events.CommitReadyPayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.Payload;
import io.tabular.iceberg.connect.events.TopicPartitionOffset;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class CommitStateTest {
  @Test
  public void testIsCommitReady() {
    TopicPartitionOffset tp = mock(TopicPartitionOffset.class);

    CommitState commitState = new CommitState();
    commitState.startNewCommit();

    CommitReadyPayload payload1 = mock(CommitReadyPayload.class);
    when(payload1.getCommitId()).thenReturn(commitState.getCurrentCommitId());
    when(payload1.getAssignments()).thenReturn(ImmutableList.of(tp, tp));

    CommitReadyPayload payload2 = mock(CommitReadyPayload.class);
    when(payload2.getCommitId()).thenReturn(commitState.getCurrentCommitId());
    when(payload2.getAssignments()).thenReturn(ImmutableList.of(tp));

    CommitReadyPayload payload3 = mock(CommitReadyPayload.class);
    when(payload3.getCommitId()).thenReturn(UUID.randomUUID());
    when(payload3.getAssignments()).thenReturn(ImmutableList.of(tp));

    commitState.addReady(wrapInEnvelope(payload1));
    commitState.addReady(wrapInEnvelope(payload2));
    commitState.addReady(wrapInEnvelope(payload3));

    assertTrue(commitState.isCommitReady(3));
    assertFalse(commitState.isCommitReady(4));
  }

  @Test
  public void testGetVtts() {
    CommitReadyPayload payload1 = mock(CommitReadyPayload.class);
    TopicPartitionOffset tp1 = mock(TopicPartitionOffset.class);
    when(tp1.getTimestamp()).thenReturn(3L);
    TopicPartitionOffset tp2 = mock(TopicPartitionOffset.class);
    when(tp2.getTimestamp()).thenReturn(2L);
    when(payload1.getAssignments()).thenReturn(ImmutableList.of(tp1, tp2));

    CommitReadyPayload payload2 = mock(CommitReadyPayload.class);
    TopicPartitionOffset tp3 = mock(TopicPartitionOffset.class);
    when(tp3.getTimestamp()).thenReturn(1L);
    when(payload2.getAssignments()).thenReturn(ImmutableList.of(tp3));

    List<CommitReadyPayload> buffer = ImmutableList.of(payload1, payload2);

    CommitState commitState = new CommitState();
    commitState.startNewCommit();

    commitState.addReady(wrapInEnvelope(payload1));
    commitState.addReady(wrapInEnvelope(payload2));

    assertEquals(1L, commitState.getVtts(false));
    assertNull(commitState.getVtts(true));

    // null timestamp for one, so should not set a vtts
    CommitReadyPayload payload3 = mock(CommitReadyPayload.class);
    TopicPartitionOffset tp4 = mock(TopicPartitionOffset.class);
    when(tp4.getTimestamp()).thenReturn(null);
    when(payload3.getAssignments()).thenReturn(ImmutableList.of(tp4));

    commitState.addReady(wrapInEnvelope(payload3));

    buffer = ImmutableList.of(payload1, payload2, payload3);
    assertNull(commitState.getVtts(false));
    assertNull(commitState.getVtts(true));
  }

  private Envelope wrapInEnvelope(Payload payload) {
    Event event = mock(Event.class);
    when(event.getPayload()).thenReturn(payload);
    return new Envelope(event, 0, 0);
  }
}
