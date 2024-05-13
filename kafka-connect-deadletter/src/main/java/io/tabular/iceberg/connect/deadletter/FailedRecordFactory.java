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
package io.tabular.iceberg.connect.deadletter;

import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

public interface FailedRecordFactory {
  Schema schema(String context);

  // how to take SMT record (which FYI is all ByteArrays) and turn it into some form of FailedRecord
  SinkRecord recordFromSmt(SinkRecord original, Throwable error, String context);

  // here is where it starts getting awkward
  // where in the original are the byte arrays.
  SinkRecord recordFromConnector(SinkRecord record, Throwable error, String context);

  void configure(Map<String, ?> props);
}
