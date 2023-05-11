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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynConstructors.Ctor;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.StringType;
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
                    new TopicPartitionOffset("topic", 1, 1L),
                    new TopicPartitionOffset("topic", 2, null))));

    byte[] data = AvroEncoderUtil.encode(event, event.getSchema());
    Event result = AvroEncoderUtil.decode(data);

    assertEquals(event.getType(), result.getType());
    CommitReadyPayload payload = (CommitReadyPayload) result.getPayload();
    assertEquals(commitId, payload.getCommitId());
    assertThat(payload.getAssignments()).hasSize(2);
    assertThat(payload.getAssignments()).allMatch(tp -> tp.getTopic().equals("topic"));
  }

  private DataFile createDataFile() {
    Ctor<DataFile> ctor =
        DynConstructors.builder(DataFile.class)
            .hiddenImpl(
                "org.apache.iceberg.GenericDataFile",
                int.class,
                String.class,
                FileFormat.class,
                PartitionData.class,
                long.class,
                Metrics.class,
                ByteBuffer.class,
                List.class,
                Integer.class)
            .build();

    PartitionData partitionData =
        new PartitionData(StructType.of(required(999, "type", StringType.get())));
    Metrics metrics =
        new Metrics(1L, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());

    return ctor.newInstance(
        1,
        "path",
        FileFormat.PARQUET,
        partitionData,
        1L,
        metrics,
        ByteBuffer.wrap(new byte[] {0}),
        null,
        1);
  }

  private DeleteFile createDeleteFile() {
    Ctor<DeleteFile> ctor =
        DynConstructors.builder(DeleteFile.class)
            .hiddenImpl(
                "org.apache.iceberg.GenericDeleteFile",
                int.class,
                FileContent.class,
                String.class,
                FileFormat.class,
                PartitionData.class,
                long.class,
                Metrics.class,
                int[].class,
                Integer.class,
                ByteBuffer.class)
            .build();

    PartitionData partitionData =
        new PartitionData(StructType.of(required(999, "type", StringType.get())));
    Metrics metrics =
        new Metrics(1L, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());

    return ctor.newInstance(
        1,
        FileContent.EQUALITY_DELETES,
        "path",
        FileFormat.PARQUET,
        partitionData,
        1L,
        metrics,
        new int[] {1},
        1,
        ByteBuffer.wrap(new byte[] {0}));
  }
}
