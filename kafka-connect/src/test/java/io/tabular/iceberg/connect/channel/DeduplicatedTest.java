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

import static io.tabular.iceberg.connect.events.EventType.COMMIT_RESPONSE;
import static org.assertj.core.api.Assertions.assertThat;

import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.TableName;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class DeduplicatedTest {

  protected MemoryAppender deduplicatedMemoryAppender;

  private static final UUID CURRENT_COMMIT_ID =
      UUID.fromString("cf602430-0f4d-41d8-a3e9-171848d89832");
  private static final UUID PAYLOAD_COMMIT_ID =
      UUID.fromString("4142add7-7c92-4bbe-b864-21ce8ac4bf53");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("db", "tbl");
  private static final TableName TABLE_NAME = TableName.of(TABLE_IDENTIFIER);
  private static final String GROUP_ID = "some-group";
  private static final DataFile DATA_FILE_1 = createDataFile("1");
  private static final DataFile DATA_FILE_2 = createDataFile("2");
  private static final DeleteFile DELETE_FILE_1 = createDeleteFile("1");
  private static final DeleteFile DELETE_FILE_2 = createDeleteFile("2");

  @BeforeEach
  public void before() {
    deduplicatedMemoryAppender = new MemoryAppender();
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Deduplicated.class))
        .addAppender(deduplicatedMemoryAppender);
    deduplicatedMemoryAppender.start();
  }

  @AfterEach
  public void after() {
    deduplicatedMemoryAppender.stop();
  }

  public static DataFile createDataFile(String fileSuffix) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("data-" + fileSuffix + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100L)
        .withRecordCount(5)
        .build();
  }

  public static DeleteFile createDeleteFile(String fileSuffix) {
    return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
        .ofEqualityDeletes(1)
        .withPath("delete-" + fileSuffix + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private void assertExpectedFiles(
      List<Envelope> batch, Set<DataFile> expectedDatafiles, Set<DeleteFile> expectedDeleteFiles) {
    List<DataFile> actualDataFiles =
        Deduplicated.dataFiles(CURRENT_COMMIT_ID, TABLE_IDENTIFIER, batch);
    List<DeleteFile> actualDeleteFiles =
        Deduplicated.deleteFiles(CURRENT_COMMIT_ID, TABLE_IDENTIFIER, batch);

    Assertions.assertEquals(expectedDatafiles, Sets.newHashSet(actualDataFiles));
    Assertions.assertEquals(expectedDeleteFiles, Sets.newHashSet(actualDeleteFiles));
  }

  private void assertNoWarnOrHigherLogs() {
    assertThat(deduplicatedMemoryAppender.getWarnOrHigher())
        .as("Expected 0 log messages")
        .hasSize(0);
  }

  private void assertWarnOrHigherLogsSize(int expectedSize) {
    assertThat(deduplicatedMemoryAppender.getWarnOrHigher()).hasSize(expectedSize);
  }

  private void assertWarnOrHigherLogsContainsEntryMatching(String expectedMessagesFmt) {
    Assertions.assertTrue(
        deduplicatedMemoryAppender.getWarnOrHigher().stream()
            .anyMatch(x -> x.matches(expectedMessagesFmt)));
  }

  private Event commitResponseEvent(List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
    return new Event(
        GROUP_ID,
        COMMIT_RESPONSE,
        new CommitResponsePayload(
            Types.StructType.of(), PAYLOAD_COMMIT_ID, TABLE_NAME, dataFiles, deleteFiles));
  }

  private <F> String detectedDuplicateFileAcrossMultipleEvents(
      int numFiles, String fileType, ContentFile<F> contentFile) {
    List<String> envelopes = Lists.newArrayList();
    IntStream.range(1, numFiles).forEach(ignored -> envelopes.add("Envelope\\(.*\\)"));
    return String.format(
        "^Detected %d %s files with the same path=%s for table=%s during commit-id=%s in the following \\(deduplicated\\) events=\\[%s]$",
        numFiles,
        fileType,
        contentFile.path(),
        TABLE_IDENTIFIER,
        CURRENT_COMMIT_ID,
        String.join(", ", envelopes));
  }

  private <F> String detectedDuplicateFileInSameEvent(
      int numFiles, String fileType, ContentFile<F> contentFile, int partition, long offset) {
    return String.format(
        "^Detected %d %s files with the same path=%s in the same event=Envelope\\(partition=%d, offset=%d, event=Event\\(id=.*, group-id=%s, timestamp=.*, commit-id=%s\\)\\) for table=%s during commit-id=%s$",
        numFiles,
        fileType,
        contentFile.path(),
        partition,
        offset,
        GROUP_ID,
        PAYLOAD_COMMIT_ID,
        TABLE_IDENTIFIER,
        CURRENT_COMMIT_ID);
  }

  @Test
  public void testShouldReturnEmptyFiles() {
    Event event = commitResponseEvent(ImmutableList.of(), ImmutableList.of());
    Envelope envelope = new Envelope(event, 0, 100);

    List<Envelope> batch = ImmutableList.of(envelope);

    assertExpectedFiles(batch, ImmutableSet.of(), ImmutableSet.of());
    assertNoWarnOrHigherLogs();
  }

  @Test
  public void testShouldReturnNonDuplicatedFile() {
    Event event =
        commitResponseEvent(ImmutableList.of(DATA_FILE_1), ImmutableList.of(DELETE_FILE_1));
    Envelope envelope = new Envelope(event, 0, 100);

    List<Envelope> batch = ImmutableList.of(envelope);

    assertExpectedFiles(batch, ImmutableSet.of(DATA_FILE_1), ImmutableSet.of(DELETE_FILE_1));
    assertNoWarnOrHigherLogs();
  }

  @Test
  public void testShouldReturnNonDuplicatedFiles() {
    Event event =
        commitResponseEvent(
            ImmutableList.of(DATA_FILE_1, DATA_FILE_2),
            ImmutableList.of(DELETE_FILE_1, DELETE_FILE_2));
    Envelope envelope = new Envelope(event, 0, 100);

    List<Envelope> batch = ImmutableList.of(envelope);

    assertExpectedFiles(
        batch,
        ImmutableSet.of(DATA_FILE_1, DATA_FILE_2),
        ImmutableSet.of(DELETE_FILE_1, DELETE_FILE_2));
    assertNoWarnOrHigherLogs();
  }

  @Test
  public void testShouldReturnNonDuplicatedFilesFromMultipleEvents() {
    Event event1 =
        commitResponseEvent(ImmutableList.of(DATA_FILE_1), ImmutableList.of(DELETE_FILE_1));
    Event event2 =
        commitResponseEvent(ImmutableList.of(DATA_FILE_2), ImmutableList.of(DELETE_FILE_2));

    List<Envelope> batch =
        ImmutableList.of(new Envelope(event1, 0, 100), new Envelope(event2, 0, 101));

    assertExpectedFiles(
        batch,
        ImmutableSet.of(DATA_FILE_1, DATA_FILE_2),
        ImmutableSet.of(DELETE_FILE_1, DELETE_FILE_2));
    assertNoWarnOrHigherLogs();
  }

  @Test
  public void testShouldDeduplicateEnvelopes() {
    Event event =
        commitResponseEvent(
            ImmutableList.of(DATA_FILE_1, DATA_FILE_2),
            ImmutableList.of(DELETE_FILE_1, DELETE_FILE_2));
    Envelope duplicatedEnvelope = new Envelope(event, 0, 100);

    List<Envelope> batch = ImmutableList.of(duplicatedEnvelope, duplicatedEnvelope);

    assertExpectedFiles(
        batch,
        ImmutableSet.of(DATA_FILE_1, DATA_FILE_2),
        ImmutableSet.of(DELETE_FILE_1, DELETE_FILE_2));

    assertWarnOrHigherLogsSize(4);
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileAcrossMultipleEvents(2, "data", DATA_FILE_1));
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileAcrossMultipleEvents(2, "data", DATA_FILE_2));
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileAcrossMultipleEvents(2, "delete", DELETE_FILE_1));
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileAcrossMultipleEvents(2, "delete", DELETE_FILE_2));
  }

  @Test
  public void testShouldDeduplicateFilesInsidePayloads() {
    Event event =
        commitResponseEvent(
            ImmutableList.of(DATA_FILE_1, DATA_FILE_2, DATA_FILE_1),
            ImmutableList.of(DELETE_FILE_1, DELETE_FILE_2, DELETE_FILE_1));
    Envelope envelope = new Envelope(event, 0, 100);

    List<Envelope> batch = ImmutableList.of(envelope);

    assertExpectedFiles(
        batch,
        ImmutableSet.of(DATA_FILE_1, DATA_FILE_2),
        ImmutableSet.of(DELETE_FILE_1, DELETE_FILE_2));

    assertWarnOrHigherLogsSize(2);
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileInSameEvent(2, "data", DATA_FILE_1, 0, 100));
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileInSameEvent(2, "delete", DELETE_FILE_1, 0, 100));
  }

  @Test
  public void testShouldDeduplicateFilesAcrossPayloads() {
    Event event1 =
        commitResponseEvent(ImmutableList.of(DATA_FILE_1), ImmutableList.of(DELETE_FILE_1));
    Event event2 =
        commitResponseEvent(
            ImmutableList.of(DATA_FILE_1, DATA_FILE_2),
            ImmutableList.of(DELETE_FILE_1, DELETE_FILE_2));

    List<Envelope> batch =
        ImmutableList.of(new Envelope(event1, 0, 100), new Envelope(event2, 0, 101));

    assertExpectedFiles(
        batch,
        ImmutableSet.of(DATA_FILE_1, DATA_FILE_2),
        ImmutableSet.of(DELETE_FILE_1, DELETE_FILE_2));

    assertWarnOrHigherLogsSize(2);
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileAcrossMultipleEvents(2, "data", DATA_FILE_1));
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileAcrossMultipleEvents(2, "delete", DELETE_FILE_1));
  }

  @Test
  public void testShouldHandleComplexCase() {
    Event event1 =
        commitResponseEvent(ImmutableList.of(DATA_FILE_1), ImmutableList.of(DELETE_FILE_1));
    Event event2 =
        commitResponseEvent(
            ImmutableList.of(DATA_FILE_1, DATA_FILE_2),
            ImmutableList.of(DELETE_FILE_1, DELETE_FILE_2));
    Event event3 =
        commitResponseEvent(
            ImmutableList.of(DATA_FILE_1, DATA_FILE_2, DATA_FILE_2),
            ImmutableList.of(DELETE_FILE_1, DELETE_FILE_2, DELETE_FILE_2));

    List<Envelope> batch =
        ImmutableList.of(
            new Envelope(event1, 0, 100),
            new Envelope(event2, 0, 101),
            new Envelope(event1, 0, 100),
            new Envelope(event3, 0, 102));

    assertExpectedFiles(
        batch,
        ImmutableSet.of(DATA_FILE_1, DATA_FILE_2),
        ImmutableSet.of(DELETE_FILE_1, DELETE_FILE_2));

    assertWarnOrHigherLogsSize(6);
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileAcrossMultipleEvents(4, "data", DATA_FILE_1));
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileAcrossMultipleEvents(2, "data", DATA_FILE_2));
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileAcrossMultipleEvents(4, "delete", DELETE_FILE_1));
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileAcrossMultipleEvents(2, "delete", DELETE_FILE_2));
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileInSameEvent(2, "data", DATA_FILE_2, 0, 102));
    assertWarnOrHigherLogsContainsEntryMatching(
        detectedDuplicateFileInSameEvent(2, "data", DATA_FILE_2, 0, 102));

    // call a second time to make sure there are no mutability bugs
    assertExpectedFiles(
        batch,
        ImmutableSet.of(DATA_FILE_1, DATA_FILE_2),
        ImmutableSet.of(DELETE_FILE_1, DELETE_FILE_2));
  }
}
