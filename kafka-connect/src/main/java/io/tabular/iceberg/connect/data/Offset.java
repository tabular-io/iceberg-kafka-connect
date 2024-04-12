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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

public class Offset implements Comparable<Offset> {

  public static final Offset NULL_OFFSET = new Offset(null, null);

  private final Long offset;
  private final Long timestamp;

  public Offset(Long offset, Long timestamp) {
    this.offset = offset;
    this.timestamp = timestamp;
  }

  public Long offset() {
    return offset;
  }

  public OffsetDateTime timestamp() {
    if (timestamp == null) {
      return null;
    }
    return OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
  }

  @Override
  public int compareTo(Offset other) {
    if (Objects.equals(this.offset, other.offset)) {
      return 0;
    }
    if (this.offset == null || (other.offset != null && other.offset > this.offset)) {
      return -1;
    }
    return 1;
  }
}
