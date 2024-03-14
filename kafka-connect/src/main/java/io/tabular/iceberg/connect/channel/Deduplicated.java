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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class both de-duplicates a batch of envelopes and adds logging to help disambiguate between
 * different ways that duplicates could manifest. Duplicates could occur in one of three general
 * ways:
 *
 * <ul>
 *   <li>same file appears in 2 envelopes e.g. if the Coordinator read the same message twice from
 *       Kafka
 *       <ul>
 *         <li>In this case, you should see a log message similar to "Detected 2 data files with the
 *             same path=data.parquet for table=.* during commit-id=.* in the following
 *             (deduplicated) events=[Envelope(offset=1)]",
 *       </ul>
 *   <li>same file in 2 different envelopes e.g. if a Worker sent the same message twice to Kafka
 *       <ul>
 *         <li>In this case, you should see a log message similar to "Detected 2 data files with the
 *             same path=data.parquet for table=.* during commit-id=.* in the following
 *             (deduplicated) events=[Envelope(offset=1), Envelope(offset=2)]",
 *       </ul>
 *   <li>same file in a single envelope twice e.g. if a Worker included the same file twice in a
 *       single message in Kafka
 *       <ul>
 *         <li>In this case, you should see a log message similar to "Detected 2 data files with the
 *             same path=data.parquet in the same event=Envelope(offset=1)"
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
    List<FileAndEnvelope<F>> filesAndEnvelopes =
        envelopes.stream()
            .flatMap(
                envelope -> {
                  CommitResponsePayload payload =
                      (CommitResponsePayload) envelope.event().payload();
                  List<F> files = extractFilesFromPayload.apply(payload);
                  if (files == null) {
                    return Stream.empty();
                  } else {
                    return deduplicate(
                            files,
                            extractPathFromFile,
                            (path, duplicateFiles) ->
                                detectedDuplicateFilesInSameEventMessage(
                                    path,
                                    duplicateFiles,
                                    fileType,
                                    envelope,
                                    tableIdentifier,
                                    currentCommitId))
                        .stream()
                        .map(file -> new FileAndEnvelope<>(file, envelope));
                  }
                })
            .collect(toList());

    List<FileAndEnvelope<F>> result =
        deduplicate(
            filesAndEnvelopes,
            fileAndEnvelope -> extractPathFromFile.apply(fileAndEnvelope.getFile()),
            (path, duplicateFilesAndEnvelopes) ->
                detectedDuplicateFilesAcrossMultipleEventsMessage(
                    path, duplicateFilesAndEnvelopes, fileType, tableIdentifier, currentCommitId));

    return result.stream().map(FileAndEnvelope::getFile).collect(toList());
  }

  private static class FileAndEnvelope<T> {
    private final T file;
    private final Envelope envelope;

    FileAndEnvelope(T file, Envelope envelope) {
      this.file = file;
      this.envelope = envelope;
    }

    public T getFile() {
      return file;
    }

    public Envelope getEnvelope() {
      return envelope;
    }
  }

  private static <T> List<T> deduplicate(
      List<T> elements,
      Function<T, String> keyExtractor,
      BiFunction<String, List<T>, String> logMessageFn) {
    Map<String, List<T>> keyToDuplicatesMapping = Maps.newHashMap();
    elements.forEach(
        element ->
            keyToDuplicatesMapping
                .computeIfAbsent(keyExtractor.apply(element), ignored -> Lists.newArrayList())
                .add(element));

    return keyToDuplicatesMapping.entrySet().stream()
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

  private static <F> String detectedDuplicateFilesInSameEventMessage(
      String path,
      List<F> duplicateFiles,
      String fileType,
      Envelope envelope,
      TableIdentifier tableIdentifier,
      UUID currentCommitId) {
    return String.format(
        "Detected %d %s files with the same path=%s in the same event=%s for table=%s during commit-id=%s",
        duplicateFiles.size(),
        fileType,
        path,
        envelopeToString(envelope),
        tableIdentifier,
        currentCommitId);
  }

  private static <F> String detectedDuplicateFilesAcrossMultipleEventsMessage(
      String path,
      List<FileAndEnvelope<F>> duplicateFilesAndEnvelopes,
      String fileType,
      TableIdentifier tableIdentifier,
      UUID currentCommitId) {
    return String.format(
        "Detected %d %s files with the same path=%s for table=%s during commit-id=%s in the following (deduplicated) events=%s",
        duplicateFilesAndEnvelopes.size(),
        fileType,
        path,
        tableIdentifier,
        currentCommitId,
        duplicateFilesAndEnvelopes.stream()
            .map(fileAndEnvelope -> envelopeToString(fileAndEnvelope.getEnvelope()))
            .distinct()
            .collect(toList()));
  }

  private static String envelopeToString(Envelope envelope) {
    Event event = envelope.event();
    CommitResponsePayload payload = ((CommitResponsePayload) event.payload());
    return String.format(
        "Envelope(partition=%d, offset=%d, event=Event(id=%s, group-id=%s, timestamp=%s, commit-id=%s))",
        envelope.partition(),
        envelope.offset(),
        event.id(),
        event.groupId(),
        event.timestamp(),
        payload.commitId());
  }
}
