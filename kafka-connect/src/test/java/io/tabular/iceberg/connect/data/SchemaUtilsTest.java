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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.SchemaUpdate.AddColumn;
import io.tabular.iceberg.connect.data.SchemaUpdate.MakeOptional;
import io.tabular.iceberg.connect.data.SchemaUpdate.UpdateType;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;

public class SchemaUtilsTest {

  private static final org.apache.iceberg.Schema SIMPLE_SCHEMA =
      new org.apache.iceberg.Schema(
          NestedField.required(1, "i", IntegerType.get()),
          NestedField.required(2, "f", FloatType.get()));

  private static final org.apache.iceberg.Schema SCHEMA_FOR_SPEC =
      new org.apache.iceberg.Schema(
          NestedField.required(1, "i", IntegerType.get()),
          NestedField.required(2, "s", StringType.get()),
          NestedField.required(3, "ts1", TimestampType.withZone()),
          NestedField.required(4, "ts2", TimestampType.withZone()),
          NestedField.required(5, "ts3", TimestampType.withZone()),
          NestedField.required(6, "ts4", TimestampType.withZone()));

  private static final IcebergSinkConfig CONFIG = mock(IcebergSinkConfig.class);

  @Test
  public void testApplySchemaUpdates() {
    UpdateSchema updateSchema = mock(UpdateSchema.class);
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);
    when(table.updateSchema()).thenReturn(updateSchema);

    // the updates to "i" should be ignored as it already exists and is the same type
    List<SchemaUpdate> updates =
        ImmutableList.of(
            new AddColumn(null, "i", IntegerType.get()),
            new UpdateType("i", IntegerType.get()),
            new MakeOptional("i"),
            new UpdateType("f", DoubleType.get()),
            new AddColumn(null, "s", StringType.get()));

    SchemaUtils.applySchemaUpdates(table, updates);
    verify(table).refresh();
    verify(table).updateSchema();
    verify(updateSchema).addColumn(isNull(), matches("s"), isA(StringType.class));
    verify(updateSchema).updateColumn(matches("f"), isA(DoubleType.class));
    verify(updateSchema).makeColumnOptional(matches("i"));
    verify(updateSchema).commit();
  }

  @Test
  public void testApplySchemaUpdatesNoUpdates() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);

    SchemaUtils.applySchemaUpdates(table, null);
    verify(table, times(0)).refresh();
    verify(table, times(0)).updateSchema();

    SchemaUtils.applySchemaUpdates(table, ImmutableList.of());
    verify(table, times(0)).refresh();
    verify(table, times(0)).updateSchema();
  }

  @Test
  public void testNeedsDataTypeUpdate() {
    // valid updates
    assertThat(SchemaUtils.needsDataTypeUpdate(FloatType.get(), Schema.FLOAT64_SCHEMA))
        .isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.needsDataTypeUpdate(IntegerType.get(), Schema.INT64_SCHEMA))
        .isInstanceOf(LongType.class);

    // other updates will be skipped
    assertThat(SchemaUtils.needsDataTypeUpdate(IntegerType.get(), Schema.STRING_SCHEMA)).isNull();
    assertThat(SchemaUtils.needsDataTypeUpdate(FloatType.get(), Schema.STRING_SCHEMA)).isNull();
    assertThat(SchemaUtils.needsDataTypeUpdate(StringType.get(), Schema.INT64_SCHEMA)).isNull();
  }

  @Test
  public void testCreatePartitionSpecUnpartitioned() {
    PartitionSpec spec = SchemaUtils.createPartitionSpec(SCHEMA_FOR_SPEC, ImmutableList.of());
    assertThat(spec.isPartitioned()).isFalse();
  }

  @Test
  public void testCreatePartitionSpec() {
    List<String> partitionFields =
        ImmutableList.of(
            "year(ts1)",
            "month(ts2)",
            "day(ts3)",
            "hour(ts4)",
            "bucket(i, 4)",
            "truncate(s, 10)",
            "s");
    PartitionSpec spec = SchemaUtils.createPartitionSpec(SCHEMA_FOR_SPEC, partitionFields);
    assertThat(spec.isPartitioned()).isTrue();
    assertThat(spec.fields()).anyMatch(val -> val.transform().toString().startsWith("year"));
    assertThat(spec.fields()).anyMatch(val -> val.transform().toString().startsWith("month"));
    assertThat(spec.fields()).anyMatch(val -> val.transform().toString().startsWith("day"));
    assertThat(spec.fields()).anyMatch(val -> val.transform().toString().startsWith("hour"));
    assertThat(spec.fields()).anyMatch(val -> val.transform().toString().startsWith("bucket"));
    assertThat(spec.fields()).anyMatch(val -> val.transform().toString().startsWith("truncate"));
    assertThat(spec.fields()).anyMatch(val -> val.transform().toString().startsWith("identity"));
  }

  @Test
  public void testToIcebergType() {
    assertThat(SchemaUtils.toIcebergType(Schema.BOOLEAN_SCHEMA, CONFIG))
        .isInstanceOf(BooleanType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.BYTES_SCHEMA, CONFIG))
        .isInstanceOf(BinaryType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT8_SCHEMA, CONFIG))
        .isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT16_SCHEMA, CONFIG))
        .isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT32_SCHEMA, CONFIG))
        .isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT64_SCHEMA, CONFIG)).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.FLOAT32_SCHEMA, CONFIG))
        .isInstanceOf(FloatType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.FLOAT64_SCHEMA, CONFIG))
        .isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.STRING_SCHEMA, CONFIG))
        .isInstanceOf(StringType.class);
    assertThat(SchemaUtils.toIcebergType(Date.SCHEMA, CONFIG)).isInstanceOf(DateType.class);
    assertThat(SchemaUtils.toIcebergType(Time.SCHEMA, CONFIG)).isInstanceOf(TimeType.class);

    Type timestampType = SchemaUtils.toIcebergType(Timestamp.SCHEMA, CONFIG);
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isTrue();

    Type decimalType = SchemaUtils.toIcebergType(Decimal.schema(4), CONFIG);
    assertThat(decimalType).isInstanceOf(DecimalType.class);
    assertThat(((DecimalType) decimalType).scale()).isEqualTo(4);

    Type listType =
        SchemaUtils.toIcebergType(SchemaBuilder.array(Schema.STRING_SCHEMA).build(), CONFIG);
    assertThat(listType).isInstanceOf(ListType.class);
    assertThat(listType.asListType().elementType()).isInstanceOf(StringType.class);

    Type mapType =
        SchemaUtils.toIcebergType(
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build(), CONFIG);
    assertThat(mapType).isInstanceOf(MapType.class);
    assertThat(mapType.asMapType().keyType()).isInstanceOf(StringType.class);
    assertThat(mapType.asMapType().valueType()).isInstanceOf(StringType.class);

    Type structType =
        SchemaUtils.toIcebergType(
            SchemaBuilder.struct().field("i", Schema.INT32_SCHEMA).build(), CONFIG);
    assertThat(structType).isInstanceOf(StructType.class);
    assertThat(structType.asStructType().fieldType("i")).isInstanceOf(IntegerType.class);
  }

  @Test
  public void testInferIcebergType() {
    assertThatThrownBy(() -> SchemaUtils.inferIcebergType(null, CONFIG))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot infer type from null value");

    assertThat(SchemaUtils.inferIcebergType(1, CONFIG)).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.inferIcebergType(1L, CONFIG)).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.inferIcebergType(1.1f, CONFIG)).isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.inferIcebergType(1.1d, CONFIG)).isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.inferIcebergType("foobar", CONFIG)).isInstanceOf(StringType.class);
    assertThat(SchemaUtils.inferIcebergType(true, CONFIG)).isInstanceOf(BooleanType.class);
    assertThat(SchemaUtils.inferIcebergType(LocalDate.now(), CONFIG)).isInstanceOf(DateType.class);
    assertThat(SchemaUtils.inferIcebergType(LocalTime.now(), CONFIG)).isInstanceOf(TimeType.class);

    Type timestampType = SchemaUtils.inferIcebergType(new java.util.Date(), CONFIG);
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isTrue();

    timestampType = SchemaUtils.inferIcebergType(OffsetDateTime.now(), CONFIG);
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isTrue();

    timestampType = SchemaUtils.inferIcebergType(LocalDateTime.now(), CONFIG);
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isFalse();

    Type decimalType = SchemaUtils.inferIcebergType(new BigDecimal("12.345"), CONFIG);
    assertThat(decimalType).isInstanceOf(DecimalType.class);
    assertThat(((DecimalType) decimalType).scale()).isEqualTo(3);

    assertThat(SchemaUtils.inferIcebergType(ImmutableList.of("foobar"), CONFIG))
        .isInstanceOf(ListType.class);
    assertThat(SchemaUtils.inferIcebergType(ImmutableMap.of("foo", "bar"), CONFIG))
        .isInstanceOf(StructType.class);
  }
}
