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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

public class SchemaUtilsTest {

  @Test
  public void testToIcebergType() {
    assertThat(SchemaUtils.toIcebergType(Schema.BOOLEAN_SCHEMA)).isInstanceOf(BooleanType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.BYTES_SCHEMA)).isInstanceOf(BinaryType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT8_SCHEMA)).isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT16_SCHEMA)).isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT32_SCHEMA)).isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT64_SCHEMA)).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.FLOAT32_SCHEMA)).isInstanceOf(FloatType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.FLOAT64_SCHEMA)).isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.STRING_SCHEMA)).isInstanceOf(StringType.class);

    Schema arraySchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
    assertThat(SchemaUtils.toIcebergType(arraySchema)).isInstanceOf(ListType.class);

    Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
    assertThat(SchemaUtils.toIcebergType(mapSchema)).isInstanceOf(MapType.class);

    Schema structSchema = SchemaBuilder.struct().field("i", Schema.INT32_SCHEMA).build();
    assertThat(SchemaUtils.toIcebergType(structSchema)).isInstanceOf(StructType.class);
  }

  @Test
  public void testInferIcebergType() {
    assertThat(SchemaUtils.inferIcebergType(1)).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.inferIcebergType(1L)).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.inferIcebergType(1.1f)).isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.inferIcebergType(1.1d)).isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.inferIcebergType("foobar")).isInstanceOf(StringType.class);
    assertThat(SchemaUtils.inferIcebergType(null)).isInstanceOf(StringType.class);
    assertThat(SchemaUtils.inferIcebergType(true)).isInstanceOf(BooleanType.class);
    assertThat(SchemaUtils.inferIcebergType(ImmutableList.of("foobar")))
        .isInstanceOf(ListType.class);
    assertThat(SchemaUtils.inferIcebergType(ImmutableMap.of("foo", "bar")))
        .isInstanceOf(StructType.class);
  }
}
