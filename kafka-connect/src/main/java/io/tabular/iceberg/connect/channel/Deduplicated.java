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

import static java.util.stream.Collectors.toList;

import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventType;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Deduplicated {
  private static final Logger LOG = LoggerFactory.getLogger(Deduplicated.class);
  private final UUID currentCommitId;
  private final TableIdentifier tableIdentifier;
  private final List<Envelope> deduplicatedEnvelopes;

  private static final String DATA_FILE_TYPE = "data";
  private static final String DELETE_FILE_TYPE = "delete";

  Deduplicated(UUID currentCommitId, TableIdentifier tableIdentifier, List<Envelope> batch) {
    this.currentCommitId = currentCommitId;
    this.tableIdentifier = tableIdentifier;
    this.deduplicatedEnvelopes = deduplicateEnvelopes(ImmutableList.copyOf(batch));
  }

  /**
   * Returns the deduplicated data files from the batch of envelopes. Does not guarantee anything
   * about the ordering of the files that are returned.
   */
  public List<DataFile> dataFiles() {
    List<DataFile> dedupedInsidePayloads =
        deduplicatedEnvelopes.stream()
            .flatMap(
                envelope -> {
                  Event event = envelope.event();
                  CommitResponsePayload payload = (CommitResponsePayload) event.payload();
                  List<DataFile> dataFiles = payload.dataFiles();
                  if (dataFiles == null) {
                    return Stream.empty();
                  } else {
                    return deduplicateFilesInPayload(
                        dataFiles,
                        Deduplicated::dataFilePath,
                        DATA_FILE_TYPE,
                        payload.commitId(),
                        envelope.partition(),
                        envelope.offset(),
                        event.id(),
                        event.groupId(),
                        event.type(),
                        event.timestamp())
                        .stream();
                  }
                })
            .collect(toList());

    List<DataFile> dedupedInsideAndAcrossPayloads =
        deduplicateFiles(dedupedInsidePayloads, Deduplicated::dataFilePath, DATA_FILE_TYPE);

    return ImmutableList.copyOf(dedupedInsideAndAcrossPayloads);
  }

  /**
   * Returns the deduplicated delete files from the batch of envelopes. Does not guarantee
   * anything about the ordering of the files that are returned.
   */
  public List<DeleteFile> deleteFiles() {
    List<DeleteFile> dedupedInsidePayloads =
        deduplicatedEnvelopes.stream()
            .flatMap(
                envelope -> {
                  Event event = envelope.event();
                  CommitResponsePayload payload =
                      (CommitResponsePayload) envelope.event().payload();
                  List<DeleteFile> deleteFiles = payload.deleteFiles();
                  if (deleteFiles == null) {
                    return Stream.empty();
                  } else {
                    return deduplicateFilesInPayload(
                        deleteFiles,
                        Deduplicated::deleteFilePath,
                        DELETE_FILE_TYPE,
                        payload.commitId(),
                        envelope.partition(),
                        envelope.offset(),
                        event.id(),
                        event.groupId(),
                        event.type(),
                        event.timestamp())
                        .stream();
                  }
                })
            .collect(toList());

    List<DeleteFile> dedupedInsideAndAcrossPayloads =
        deduplicateFiles(dedupedInsidePayloads, Deduplicated::deleteFilePath, DELETE_FILE_TYPE);

    return ImmutableList.copyOf(dedupedInsideAndAcrossPayloads);
  }

  private static class PartitionOffset {

    private final int partition;
    private final long offset;

    PartitionOffset(int partition, long offset) {
      this.partition = partition;
      this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PartitionOffset that = (PartitionOffset) o;
      return partition == that.partition && offset == that.offset;
    }

    @Override
    public int hashCode() {
      return Objects.hash(partition, offset);
    }

    @Override
    public String toString() {
      return "PartitionOffset{" + "partition=" + partition + ", offset=" + offset + '}';
    }
  }

  private List<Envelope> deduplicateEnvelopes(List<Envelope> envelopes) {
    return deduplicate(
        envelopes,
        envelope -> new PartitionOffset(envelope.partition(), envelope.offset()),
        (numDuplicatedFiles, duplicatedPartitionOffset) ->
            String.format(
                "Detected %d envelopes with the same partition-offset=%d-%d during commitId=%s for table=%s",
                numDuplicatedFiles,
                duplicatedPartitionOffset.partition,
                duplicatedPartitionOffset.offset,
                currentCommitId.toString(),
                tableIdentifier.toString()));
  }

  private static String dataFilePath(DataFile dataFile) {
    return dataFile.path().toString();
  }

  private static String deleteFilePath(DeleteFile deleteFile) {
    return deleteFile.path().toString();
  }

  private <T> List<T> deduplicateFilesInPayload(
      List<T> files,
      Function<T, String> getPathFn,
      String fileType,
      UUID payloadCommitId,
      int partition,
      long offset,
      UUID eventId,
      String groupId,
      EventType eventType,
      Long eventTimestamp) {
    return deduplicate(
        files,
        getPathFn,
        (numDuplicatedFiles, duplicatedPath) ->
            String.format(
                "Detected %d %s files with the same path=%s in payload with payload-commit-id=%s for table=%s at partition=%s and offset=%s with event-id=%s and group-id=%s and event-type=%s and event-timestamp=%s",
                numDuplicatedFiles,
                fileType,
                duplicatedPath,
                payloadCommitId.toString(),
                tableIdentifier.toString(),
                partition,
                offset,
                eventId,
                groupId,
                eventType,
                eventTimestamp));
  }

  private <T> List<T> deduplicateFiles(
      List<T> files, Function<T, String> getPathFn, String fileType) {
    return deduplicate(
        files,
        getPathFn,
        (numDuplicatedFiles, duplicatedPath) ->
            String.format(
                "Detected %d %s files with the same path=%s across payloads during commitId=%s for table=%s",
                numDuplicatedFiles,
                fileType,
                duplicatedPath,
                currentCommitId.toString(),
                tableIdentifier.toString()));
  }

  private static <K, T> List<T> deduplicate(
      List<T> files, Function<T, K> keyExtractor, BiFunction<Integer, K, String> logMessageFn) {
    Map<K, List<T>> pathToFilesMapping = Maps.newHashMap();
    files.forEach(
        f ->
            pathToFilesMapping
                .computeIfAbsent(keyExtractor.apply(f), ignored -> Lists.newArrayList())
                .add(f));

    return pathToFilesMapping.entrySet().stream()
        .flatMap(
            entry -> {
              K maybeDuplicatedKey = entry.getKey();
              List<T> maybeDuplicatedFiles = entry.getValue();
              int numDuplicatedFiles = maybeDuplicatedFiles.size();
              if (numDuplicatedFiles > 1) {
                LOG.warn(logMessageFn.apply(numDuplicatedFiles, maybeDuplicatedKey));
              }
              return Stream.of(maybeDuplicatedFiles.get(0));
            })
        .collect(toList());
  }
}
