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

import io.tabular.iceberg.connect.events.CommitRequestPayload;
import java.util.UUID;

import io.tabular.iceberg.connect.events.CommitResponsePayload;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.iceberg.connect.events.*;

public class EventDecoder {

  private EventDecoder() {}

  public static Event decode(byte[] value) {
    try {
      return AvroUtil.decode(value);
    } catch (SchemaParseException exception) {
      io.tabular.iceberg.connect.events.Event event =
          io.tabular.iceberg.connect.events.Event.decode(value);
      return convertLegacy(event);
    }
  }

  private static Event convertLegacy(io.tabular.iceberg.connect.events.Event event) {
    Payload payload = convertPayload(event.payload());
    return new Event(event.groupId(), payload);
  }

  private static Payload convertPayload(
      io.tabular.iceberg.connect.events.Payload payload) {
    if (payload instanceof CommitRequestPayload) {
      CommitRequestPayload pay = (CommitRequestPayload) payload;
      return new StartCommit((UUID) pay.get(0));
    }  else if (payload instanceof CommitResponsePayload) {
      CommitResponsePayload pay = (CommitResponsePayload) payload;
      Schema schema = pay.getSchema();
      // need to get a PartitionType somehow .
      throw new RuntimeException("stuck here");
    }

    else {
      throw new RuntimeException("borp");
    }
  }
}
