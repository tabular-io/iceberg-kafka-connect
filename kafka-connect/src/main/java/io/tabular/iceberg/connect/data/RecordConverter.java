// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.data;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

public class RecordConverter {

  // support ' ' separator
  private static final DateTimeFormatter LOCAL_DATE_TIME_FORMAT =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(ISO_LOCAL_DATE)
          .appendLiteral(' ')
          .append(ISO_LOCAL_TIME)
          .toFormatter();

  // support ' ' separator
  private static final DateTimeFormatter OFFSET_DATE_TIME_FORMAT =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(LOCAL_DATE_TIME_FORMAT)
          .appendOffsetId()
          .toFormatter();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final StructType tableSchema;
  private final NameMapping nameMapping;
  private final JsonConverter jsonConverter;

  public RecordConverter(Table table) {
    this.tableSchema = table.schema().asStruct();
    this.nameMapping = getNameMapping(table);
    this.jsonConverter = new JsonConverter();
    jsonConverter.configure(
        ImmutableMap.of(
            JsonConverterConfig.SCHEMAS_ENABLE_CONFIG,
            false,
            ConverterConfig.TYPE_CONFIG,
            ConverterType.VALUE.getName()));
  }

  public Record convert(Object data) {
    if (data instanceof Struct || data instanceof Map) {
      return convertStructValue(data, tableSchema);
    }
    throw new UnsupportedOperationException("Cannot convert type: " + data.getClass().getName());
  }

  private NameMapping getNameMapping(Table table) {
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    return nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
  }

  private Object convertValue(Object value, Type type) {
    if (value == null) {
      return null;
    }
    switch (type.typeId()) {
      case STRUCT:
        return convertStructValue(value, type.asStructType());
      case LIST:
        return convertListValue(value, type.asListType());
      case MAP:
        return convertMapValue(value, type.asMapType());
      case INTEGER:
        return convertInt(value);
      case LONG:
        return convertLong(value);
      case FLOAT:
        return convertFloat(value);
      case DOUBLE:
        return convertDouble(value);
      case DECIMAL:
        return convertDecimal(value);
      case BOOLEAN:
        return convertBoolean(value);
      case STRING:
        return convertString(value);
      case UUID:
        return convertUUID(value);
      case BINARY:
      case FIXED:
        return convertBase64Binary(value);
      case DATE:
        return convertDateValue(value);
      case TIME:
        return convertTimeValue(value);
      case TIMESTAMP:
        return convertTimestampValue(value, (TimestampType) type);
    }
    throw new UnsupportedOperationException("Unsupported type: " + type.typeId());
  }

  protected GenericRecord convertStructValue(Object value, StructType schema) {
    GenericRecord record = GenericRecord.create(schema);
    schema
        .fields()
        .forEach(
            field -> {
              Object fieldVal = getValueFromStruct(value, field);
              if (fieldVal != null) {
                record.setField(field.name(), convertValue(fieldVal, field.type()));
              }
            });
    return record;
  }

  private Object getValueFromStruct(Object value, NestedField field) {
    MappedField mappedField = nameMapping == null ? null : nameMapping.find(field.fieldId());

    if (mappedField == null || mappedField.names().isEmpty()) {
      // if no name mappings then use the field name in the schema
      return getFieldValue(value, field.name());
    }

    for (String fieldName : mappedField.names()) {
      Object result = getFieldValue(value, fieldName);
      if (result != null) {
        return result;
      }
    }

    return null; // no field matches in the source
  }

  private Object getFieldValue(Object value, String fieldName) {
    if (value instanceof Map) {
      return ((Map<?, ?>) value).get(fieldName);
    } else if (value instanceof Struct) {
      return ((Struct) value).get(fieldName);
    }
    throw new IllegalArgumentException("Cannot convert to struct: " + value.getClass().getName());
  }

  protected List<Object> convertListValue(Object value, ListType type) {
    Preconditions.checkArgument(value instanceof List);
    List<?> list = (List<?>) value;
    return list.stream()
        .map(element -> convertValue(element, type.elementType()))
        .collect(toList());
  }

  protected Map<Object, Object> convertMapValue(Object value, MapType type) {
    Preconditions.checkArgument(value instanceof Map);
    Map<?, ?> map = (Map<?, ?>) value;
    Map<Object, Object> result = new HashMap<>();
    map.forEach(
        (k, v) -> result.put(convertValue(k, type.keyType()), convertValue(v, type.valueType())));
    return result;
  }

  protected int convertInt(Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof String) {
      return Integer.parseInt((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to int: " + value.getClass().getName());
  }

  protected long convertLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      return Long.parseLong((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to long: " + value.getClass().getName());
  }

  protected float convertFloat(Object value) {
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      return Float.parseFloat((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to float: " + value.getClass().getName());
  }

  protected double convertDouble(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else if (value instanceof String) {
      return Double.parseDouble((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to double: " + value.getClass().getName());
  }

  protected BigDecimal convertDecimal(Object value) {
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    } else if (value instanceof Number) {
      Number num = (Number) value;
      long l = num.longValue();
      if (num.equals(l)) {
        return BigDecimal.valueOf(num.longValue());
      }
      return BigDecimal.valueOf(num.doubleValue());
    } else if (value instanceof String) {
      return new BigDecimal((String) value);
    }
    throw new IllegalArgumentException(
        "Cannot convert to BigDecimal: " + value.getClass().getName());
  }

  protected boolean convertBoolean(Object value) {
    if (value instanceof Boolean) {
      return (boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to boolean: " + value.getClass().getName());
  }

  protected String convertString(Object value) {
    try {
      if (value instanceof String) {
        return (String) value;
      } else if (value instanceof Number || value instanceof Boolean) {
        return value.toString();
      } else if (value instanceof Map || value instanceof List) {
        return MAPPER.writeValueAsString(value);
      } else if (value instanceof Struct) {
        Struct struct = (Struct) value;
        byte[] data = jsonConverter.fromConnectData(null, struct.schema(), struct);
        return new String(data, StandardCharsets.UTF_8);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    throw new IllegalArgumentException("Cannot convert to string: " + value.getClass().getName());
  }

  protected UUID convertUUID(Object value) {
    if (value instanceof String) {
      return UUID.fromString((String) value);
    } else if (value instanceof UUID) {
      return (UUID) value;
    }
    throw new IllegalArgumentException("Cannot convert to UUID: " + value.getClass().getName());
  }

  protected ByteBuffer convertBase64Binary(Object value) {
    if (value instanceof String) {
      return ByteBuffer.wrap(Base64.getDecoder().decode((String) value));
    } else if (value instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) value);
    } else if (value instanceof ByteBuffer) {
      return (ByteBuffer) value;
    }
    throw new IllegalArgumentException("Cannot convert to binary: " + value.getClass().getName());
  }

  protected LocalDate convertDateValue(Object value) {
    if (value instanceof Number) {
      int i = ((Number) value).intValue();
      return DateTimeUtil.dateFromDays(i);
    } else if (value instanceof String) {
      return LocalDate.parse((String) value);
    } else if (value instanceof LocalDate) {
      return (LocalDate) value;
    }
    throw new RuntimeException("Cannot convert date: " + value);
  }

  protected LocalTime convertTimeValue(Object value) {
    if (value instanceof Number) {
      long l = ((Number) value).longValue();
      return DateTimeUtil.timeFromMicros(l * 1000);
    } else if (value instanceof String) {
      return LocalTime.parse((String) value);
    } else if (value instanceof LocalTime) {
      return (LocalTime) value;
    }
    throw new RuntimeException("Cannot convert time: " + value);
  }

  protected Temporal convertTimestampValue(Object value, TimestampType type) {
    // TODO: support non-ISO format, e.g. ' ' instead of 'T'
    if (type.shouldAdjustToUTC()) {
      if (value instanceof Number) {
        long l = ((Number) value).longValue();
        return DateTimeUtil.timestamptzFromMicros(l * 1000);
      } else if (value instanceof String) {
        return parseOffsetDateTime((String) value);
      } else if (value instanceof OffsetDateTime) {
        return (OffsetDateTime) value;
      }
    } else {
      if (value instanceof Number) {
        long l = ((Number) value).longValue();
        return DateTimeUtil.timestampFromMicros(l * 1000);
      } else if (value instanceof String) {
        return parseLocalDateTime((String) value);
      } else if (value instanceof LocalDateTime) {
        return (LocalDateTime) value;
      }
    }
    throw new RuntimeException("Cannot convert timestamp: " + value);
  }

  private OffsetDateTime parseOffsetDateTime(String str) {
    try {
      return OffsetDateTime.parse(str, ISO_OFFSET_DATE_TIME);
    } catch (DateTimeParseException e) {
      return OffsetDateTime.parse(str, OFFSET_DATE_TIME_FORMAT);
    }
  }

  private LocalDateTime parseLocalDateTime(String str) {
    try {
      return LocalDateTime.parse(str, ISO_LOCAL_DATE_TIME);
    } catch (DateTimeParseException e) {
      return LocalDateTime.parse(str, LOCAL_DATE_TIME_FORMAT);
    }
  }
}
