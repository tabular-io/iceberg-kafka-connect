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
package io.tabular.iceberg.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

public class DeadLetterTable {

  private DeadLetterTable() {}

  public static final String NAME = "dead-letter";

  public static final Schema SCHEMA =
      // This is just a simple dead letter table schema just for demonstration purposes
      // Users can get as complicated as they wish
      SchemaBuilder.struct()
          .field("topic", Schema.STRING_SCHEMA)
          .field("partition", Schema.INT32_SCHEMA)
          .field("offset", Schema.INT64_SCHEMA)
          .field("exception", Schema.STRING_SCHEMA)
          .field("target_table", Schema.STRING_SCHEMA)
          .schema();

  public static SinkRecord sinkRecord(
      SinkRecord original, Exception exception, String deadLetterTableName) {
    Struct struct = new Struct(SCHEMA);
    struct.put("topic", original.topic());
    struct.put("partition", original.kafkaPartition());
    struct.put("offset", original.kafkaOffset());
    struct.put("exception", exception.toString());
    struct.put("target_table", deadLetterTableName);
    return original.newRecord(
        original.topic(),
        original.kafkaPartition(),
        null,
        null,
        SCHEMA,
        struct,
        original.timestamp());
  }
}
