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
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class both de-duplicates a batch of envelopes and adds logging to help disambiguate between
 * different ways that duplicates could manifest. Duplicates could occur in the following three
 * general ways:
 *
 * <ul>
 *   <li>same file appears in 2 equivalent envelopes e.g. if the Coordinator read the same message
 *       twice from Kafka
 *       <ul>
 *         <li>In this case, you should see a log message similar to "Deduplicated 2 data files with
 *             the same path=data.parquet for table=db.tbl during
 *             commit-id=cf602430-0f4d-41d8-a3e9-171848d89832 from the following
 *             events=[2x(SimpleEnvelope{...})]",
 *       </ul>
 *   <li>same file appears in 2 different envelopes e.g. if a Worker sent the same message twice to
 *       Kafka
 *       <ul>
 *         <li>In this case, you should see a log message similar to "Deduplicated 2 data files with
 *             the same path=data.parquet for table=db.tbl during
 *             commit-id=cf602430-0f4d-41d8-a3e9-171848d89832 from the following
 *             events=[1x(SimpleEnvelope{...}), 1x(SimpleEnvelope{...})]",
 *       </ul>
 *   <li>same file appears in a single envelope twice e.g. if a Worker included the same file twice
 *       in a single message in Kafka
 *       <ul>
 *         <li>In this case, you should see a log message similar to "Deduplicated 2 data files with
 *             the same path=data.parquet in the same event=SimpleEnvelope{...} for table=db.tbl
 *             during commit-id=cf602430-0f4d-41d8-a3e9-171848d89832"
 *       </ul>
 * </ul>
 */
class Deduplicated {
  private static final Logger LOG = LoggerFactory.getLogger(Deduplicated.class);

  private Deduplicated() {}

  /**
   * Returns the deduplicated data files from the batch of envelopes. Does not guarantee anything
   * about the ordering of the files that are returned.
   */
  public static List<DataFile> dataFiles(
      UUID currentCommitId, TableIdentifier tableIdentifier, List<Envelope> envelopes) {
    return deduplicatedFiles(
        currentCommitId,
        tableIdentifier,
        envelopes,
        "data",
        CommitResponsePayload::dataFiles,
        dataFile -> dataFile.path().toString());
  }

  /**
   * Returns the deduplicated delete files from the batch of envelopes. Does not guarantee anything
   * about the ordering of the files that are returned.
   */
  public static List<DeleteFile> deleteFiles(
      UUID currentCommitId, TableIdentifier tableIdentifier, List<Envelope> envelopes) {
    return deduplicatedFiles(
        currentCommitId,
        tableIdentifier,
        envelopes,
        "delete",
        CommitResponsePayload::deleteFiles,
        deleteFile -> deleteFile.path().toString());
  }

  private static <F> List<F> deduplicatedFiles(
      UUID currentCommitId,
      TableIdentifier tableIdentifier,
      List<Envelope> envelopes,
      String fileType,
      Function<CommitResponsePayload, List<F>> extractFilesFromPayload,
      Function<F, String> extractPathFromFile) {
    List<Pair<F, SimpleEnvelope>> filesAndEnvelopes =
        envelopes.stream()
            .flatMap(
                envelope -> {
                  CommitResponsePayload payload =
                      (CommitResponsePayload) envelope.event().payload();
                  List<F> files = extractFilesFromPayload.apply(payload);
                  if (files == null) {
                    return Stream.empty();
                  } else {
                    SimpleEnvelope simpleEnvelope = new SimpleEnvelope(envelope);
                    return deduplicate(
                            files,
                            extractPathFromFile,
                            (path, duplicateFiles) ->
                                duplicateFilesInSameEventMessage(
                                    path,
                                    duplicateFiles,
                                    fileType,
                                    simpleEnvelope,
                                    tableIdentifier,
                                    currentCommitId))
                        .stream()
                        .map(file -> Pair.of(file, simpleEnvelope));
                  }
                })
            .collect(toList());

    List<Pair<F, SimpleEnvelope>> result =
        deduplicate(
            filesAndEnvelopes,
            fileAndEnvelope -> extractPathFromFile.apply(fileAndEnvelope.first()),
            (path, duplicateFilesAndEnvelopes) ->
                duplicateFilesAcrossMultipleEventsMessage(
                    path, duplicateFilesAndEnvelopes, fileType, tableIdentifier, currentCommitId));

    return result.stream().map(Pair::first).collect(toList());
  }

  private static <T> List<T> deduplicate(
      List<T> elements,
      Function<T, String> keyExtractor,
      BiFunction<String, List<T>, String> logMessageFn) {
    return elements.stream()
        .collect(Collectors.groupingBy(keyExtractor, Collectors.toList()))
        .entrySet()
        .stream()
        .flatMap(
            entry -> {
              String key = entry.getKey();
              List<T> values = entry.getValue();
              if (values.size() > 1) {
                LOG.warn(logMessageFn.apply(key, values));
              }
              return Stream.of(values.get(0));
            })
        .collect(toList());
  }

  private static <F> String duplicateFilesInSameEventMessage(
      String path,
      List<F> duplicateFiles,
      String fileType,
      SimpleEnvelope envelope,
      TableIdentifier tableIdentifier,
      UUID currentCommitId) {
    return String.format(
        "Deduplicated %d %s files with the same path=%s in the same event=%s for table=%s during commit-id=%s",
        duplicateFiles.size(), fileType, path, envelope, tableIdentifier, currentCommitId);
  }

  private static <F> String duplicateFilesAcrossMultipleEventsMessage(
      String path,
      List<Pair<F, SimpleEnvelope>> duplicateFilesAndEnvelopes,
      String fileType,
      TableIdentifier tableIdentifier,
      UUID currentCommitId) {
    return String.format(
        "Deduplicated %d %s files with the same path=%s for table=%s during commit-id=%s from the following events=%s",
        duplicateFilesAndEnvelopes.size(),
        fileType,
        path,
        tableIdentifier,
        currentCommitId,
        duplicateFilesAndEnvelopes.stream()
            .map(Pair::second)
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
            .entrySet()
            .stream()
            .map(e -> String.format("%dx(%s)", e.getValue(), e.getKey()))
            .collect(toList()));
  }

  private static class SimpleEnvelope {

    private final int partition;
    private final long offset;
    private final UUID eventId;
    private final String eventGroupId;
    private final Long eventTimestamp;
    private final UUID payloadCommitId;

    SimpleEnvelope(Envelope envelope) {
      partition = envelope.partition();
      offset = envelope.offset();
      eventId = envelope.event().id();
      eventGroupId = envelope.event().groupId();
      eventTimestamp = envelope.event().timestamp();
      payloadCommitId = ((CommitResponsePayload) envelope.event().payload()).commitId();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SimpleEnvelope that = (SimpleEnvelope) o;
      return partition == that.partition
          && offset == that.offset
          && Objects.equals(eventId, that.eventId)
          && Objects.equals(eventGroupId, that.eventGroupId)
          && Objects.equals(eventTimestamp, that.eventTimestamp)
          && Objects.equals(payloadCommitId, that.payloadCommitId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          partition, offset, eventId, eventGroupId, eventTimestamp, payloadCommitId);
    }

    @Override
    public String toString() {
      return "SimpleEnvelope{"
          + "partition="
          + partition
          + ", offset="
          + offset
          + ", eventId="
          + eventId
          + ", eventGroupId='"
          + eventGroupId
          + '\''
          + ", eventTimestamp="
          + eventTimestamp
          + ", payloadCommitId="
          + payloadCommitId
          + '}';
    }
  }
}
