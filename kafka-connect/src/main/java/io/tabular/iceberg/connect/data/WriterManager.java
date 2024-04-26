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
package io.tabular.iceberg.connect.data;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.sink.SinkRecord;

public class WriterManager {
  private final IcebergWriterFactory writerFactory;

  private final Map<String, RecordWriter> writers;

  public WriterManager(IcebergWriterFactory writerFactory) {
    this.writerFactory = writerFactory;
    this.writers = Maps.newHashMap();
  }

  public void write(String tableName, SinkRecord record, boolean ignoreMissingTable) {
    writers
        .computeIfAbsent(
            tableName, notUsed -> writerFactory.createWriter(tableName, record, ignoreMissingTable))
        .write(record);
  }

  public List<WriterResult> writeResults() {
    return writers.values().stream()
        .flatMap(writer -> writer.complete().stream())
        .collect(toList());
  }

  public void clear() {
    this.writers.clear();
  }

  public void stop() {
    writers.values().forEach(RecordWriter::close);
  }
}
