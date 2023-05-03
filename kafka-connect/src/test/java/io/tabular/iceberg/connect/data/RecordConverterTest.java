// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

public class RecordConverterTest {

  private static final org.apache.iceberg.Schema SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(21, "i", Types.IntegerType.get()),
          Types.NestedField.required(22, "l", Types.LongType.get()),
          Types.NestedField.required(23, "d", Types.DateType.get()),
          Types.NestedField.required(24, "t", Types.TimeType.get()),
          Types.NestedField.required(25, "ts", Types.TimestampType.withoutZone()),
          Types.NestedField.required(26, "tsz", Types.TimestampType.withZone()),
          Types.NestedField.required(27, "fl", Types.FloatType.get()),
          Types.NestedField.required(28, "do", Types.DoubleType.get()),
          Types.NestedField.required(29, "dec", Types.DecimalType.of(9, 2)),
          Types.NestedField.required(30, "s", Types.StringType.get()),
          Types.NestedField.required(31, "u", Types.UUIDType.get()),
          Types.NestedField.required(32, "f", Types.FixedType.ofLength(3)),
          Types.NestedField.required(33, "b", Types.BinaryType.get()),
          Types.NestedField.required(
              34, "li", Types.ListType.ofRequired(35, Types.StringType.get())),
          Types.NestedField.required(
              36,
              "ma",
              Types.MapType.ofRequired(37, 38, Types.StringType.get(), Types.StringType.get())));

  private static final org.apache.iceberg.Schema NESTED_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "ii", Types.IntegerType.get()),
          Types.NestedField.required(2, "st", SCHEMA.asStruct()));

  private static final Schema CONNECT_SCHEMA =
      SchemaBuilder.struct()
          .field("i", Schema.INT32_SCHEMA)
          .field("l", Schema.INT64_SCHEMA)
          .field("d", Schema.STRING_SCHEMA)
          .field("t", Schema.STRING_SCHEMA)
          .field("ts", Schema.STRING_SCHEMA)
          .field("tsz", Schema.STRING_SCHEMA)
          .field("fl", Schema.FLOAT32_SCHEMA)
          .field("do", Schema.FLOAT64_SCHEMA)
          .field("dec", Schema.STRING_SCHEMA)
          .field("s", Schema.STRING_SCHEMA)
          .field("u", Schema.STRING_SCHEMA)
          .field("f", Schema.BYTES_SCHEMA)
          .field("b", Schema.BYTES_SCHEMA)
          .field("li", SchemaBuilder.array(Schema.STRING_SCHEMA))
          .field("ma", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA));

  private static final Schema CONNECT_NESTED_SCHEMA =
      SchemaBuilder.struct().field("ii", Schema.INT32_SCHEMA).field("st", CONNECT_SCHEMA);

  private static final LocalDate DATE_VAL = LocalDate.parse("2023-05-18");
  private static final LocalTime TIME_VAL = LocalTime.parse("07:14:21");
  private static final LocalDateTime TS_VAL = LocalDateTime.parse("2023-05-18T07:14:21");
  private static final OffsetDateTime TSZ_VAL = OffsetDateTime.parse("2023-05-18T07:14:21Z");
  private static final BigDecimal DEC_VAL = new BigDecimal("12.34");
  private static final String STR_VAL = "foobar";
  private static final UUID UUID_VAL = UUID.randomUUID();
  private static final ByteBuffer BYTES_VAL = ByteBuffer.wrap(new byte[] {1, 2, 3});
  private static final List<String> LIST_VAL = ImmutableList.of("hello", "world");
  private static final Map<String, String> MAP_VAL = ImmutableMap.of("one", "1", "two", "2");

  @Test
  public void testMapConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SCHEMA);
    RecordConverter converter = new RecordConverter(table);

    Map<String, Object> data = createMapData();
    Record record = converter.convert(data);
    assertRecordValues(record);

    when(table.schema()).thenReturn(NESTED_SCHEMA);
    converter = new RecordConverter(table);

    Map<String, Object> nestedData = createNestedMapData();
    record = converter.convert(nestedData);
    assertNestedRecordValues(record);
  }

  @Test
  public void testStructConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SCHEMA);
    RecordConverter converter = new RecordConverter(table);

    Struct data = createStructData();
    Record record = converter.convert(data);
    assertRecordValues(record);

    when(table.schema()).thenReturn(NESTED_SCHEMA);
    converter = new RecordConverter(table);

    Struct nestedData = createNestedStructData();
    record = converter.convert(nestedData);
    assertNestedRecordValues(record);
  }

  private Map<String, Object> createMapData() {
    return ImmutableMap.<String, Object>builder()
        .put("i", 1)
        .put("l", 2L)
        .put("d", DATE_VAL.toString())
        .put("t", TIME_VAL.toString())
        .put("ts", TS_VAL.toString())
        .put("tsz", TSZ_VAL.toString())
        .put("fl", 1.1f)
        .put("do", 2.2d)
        .put("dec", DEC_VAL.toString())
        .put("s", STR_VAL)
        .put("u", UUID_VAL.toString())
        .put("f", Base64.getEncoder().encodeToString(BYTES_VAL.array()))
        .put("b", Base64.getEncoder().encodeToString(BYTES_VAL.array()))
        .put("li", LIST_VAL)
        .put("ma", MAP_VAL)
        .build();
  }

  private Map<String, Object> createNestedMapData() {
    return ImmutableMap.<String, Object>builder().put("ii", 11).put("st", createMapData()).build();
  }

  private Struct createStructData() {
    return new Struct(CONNECT_SCHEMA)
        .put("i", 1)
        .put("l", 2L)
        .put("d", DATE_VAL.toString())
        .put("t", TIME_VAL.toString())
        .put("ts", TS_VAL.toString())
        .put("tsz", TSZ_VAL.toString())
        .put("fl", 1.1f)
        .put("do", 2.2d)
        .put("dec", DEC_VAL.toString())
        .put("s", STR_VAL)
        .put("u", UUID_VAL.toString())
        .put("f", BYTES_VAL.array())
        .put("b", BYTES_VAL.array())
        .put("li", LIST_VAL)
        .put("ma", MAP_VAL);
  }

  private Struct createNestedStructData() {
    return new Struct(CONNECT_NESTED_SCHEMA).put("ii", 11).put("st", createStructData());
  }

  private void assertRecordValues(Record record) {
    GenericRecord rec = (GenericRecord) record;
    assertEquals(1, rec.getField("i"));
    assertEquals(2L, rec.getField("l"));
    assertEquals(DATE_VAL, rec.getField("d"));
    assertEquals(TIME_VAL, rec.getField("t"));
    assertEquals(TS_VAL, rec.getField("ts"));
    assertEquals(TSZ_VAL, rec.getField("tsz"));
    assertEquals(1.1f, rec.getField("fl"));
    assertEquals(2.2d, rec.getField("do"));
    assertEquals(DEC_VAL, rec.getField("dec"));
    assertEquals(STR_VAL, rec.getField("s"));
    assertEquals(UUID_VAL, rec.getField("u"));
    assertEquals(BYTES_VAL, rec.getField("f"));
    assertEquals(BYTES_VAL, rec.getField("b"));
    assertEquals(LIST_VAL, rec.getField("li"));
    assertEquals(MAP_VAL, rec.getField("ma"));
  }

  private void assertNestedRecordValues(Record record) {
    GenericRecord rec = (GenericRecord) record;
    assertEquals(11, rec.getField("ii"));
    assertRecordValues((GenericRecord) rec.getField("st"));
  }
}
