// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc.convert;

import static io.tabular.connect.poc.convert.ConvertUtil.DEFAULT_TZ;
import static io.tabular.connect.poc.convert.ConvertUtil.NANOS_PER_MILLI;
import static java.util.stream.Collectors.toList;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;

public class MapRecordConverter implements RecordConverter {

  @Override
  public Record convert(Object data, StructType tableSchema) {
    return convertStructValue(data, tableSchema);
  }

  private static Object convertValue(Object value, Type type) {
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
    throw new RuntimeException("Unsupported type: " + type.typeId());
  }

  private static GenericRecord convertStructValue(Object value, StructType schema) {
    Preconditions.checkArgument(value instanceof Map);
    Map<?, ?> map = (Map<?, ?>) value;
    GenericRecord record = GenericRecord.create(schema);
    map.forEach(
        (k, v) -> {
          NestedField field = schema.field((String) k);
          // NOTE: this ignores fields that aren't present in the schema
          if (field != null) {
            record.setField(field.name(), convertValue(v, field.type()));
          }
        });
    return record;
  }

  private static List<Object> convertListValue(Object value, ListType type) {
    Preconditions.checkArgument(value instanceof Map);
    List<?> list = (List<?>) value;
    return list.stream()
        .map(element -> convertValue(element, type.elementType()))
        .collect(toList());
  }

  private static Map<Object, Object> convertMapValue(Object value, MapType type) {
    Preconditions.checkArgument(value instanceof Map);
    Map<?, ?> map = (Map<?, ?>) value;
    Map<Object, Object> result = new HashMap<>();
    map.forEach(
        (k, v) -> result.put(convertValue(k, type.keyType()), convertValue(v, type.valueType())));
    return result;
  }

  private static int convertInt(Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof String) {
      return Integer.parseInt((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to int: " + value.getClass().getName());
  }

  private static long convertLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      return Long.parseLong((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to long: " + value.getClass().getName());
  }

  private static float convertFloat(Object value) {
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      return Float.parseFloat((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to float: " + value.getClass().getName());
  }

  private static double convertDouble(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else if (value instanceof String) {
      return Double.parseDouble((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to double: " + value.getClass().getName());
  }

  private static BigDecimal convertDecimal(Object value) {
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

  private static boolean convertBoolean(Object value) {
    if (value instanceof Boolean) {
      return (boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to boolean: " + value.getClass().getName());
  }

  @SneakyThrows
  private static String convertString(Object value) {
    if (value instanceof String) {
      return (String) value;
    } else if (value instanceof Number || value instanceof Boolean) {
      return value.toString();
    } else if (value instanceof Map || value instanceof List) {
      return ConvertUtil.MAPPER.writeValueAsString(value);
    }
    throw new IllegalArgumentException("Cannot convert to string: " + value.getClass().getName());
  }

  private static UUID convertUUID(Object value) {
    if (value instanceof String) {
      return UUID.fromString((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to UUID: " + value.getClass().getName());
  }

  private static ByteBuffer convertBase64Binary(Object value) {
    if (value instanceof String) {
      return ByteBuffer.wrap(Base64.getDecoder().decode((String) value));
    }
    throw new IllegalArgumentException("Cannot convert to binary: " + value.getClass().getName());
  }

  private static LocalDate convertDateValue(Object value) {
    if (value instanceof Number) {
      long i = ((Number) value).intValue();
      return LocalDate.ofEpochDay(i);
    } else if (value instanceof String) {
      return LocalDate.parse((String) value);
    }
    throw new RuntimeException("Cannot convert date: " + value);
  }

  private static LocalTime convertTimeValue(Object value) {
    if (value instanceof Number) {
      long l = ((Number) value).longValue();
      return LocalTime.ofNanoOfDay(l * NANOS_PER_MILLI);
    } else if (value instanceof String) {
      return LocalTime.parse((String) value);
    }
    throw new RuntimeException("Cannot convert time: " + value);
  }

  private static Temporal convertTimestampValue(Object value, TimestampType type) {
    // TODO: support non-ISO format, e.g. ' ' instead of 'T'
    if (type.shouldAdjustToUTC()) {
      if (value instanceof Number) {
        long l = ((Number) value).longValue();
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneOffset.UTC);
      } else if (value instanceof String) {
        return OffsetDateTime.parse((String) value);
      }
    } else {
      if (value instanceof Number) {
        long l = ((Number) value).longValue();
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(l), DEFAULT_TZ);
      } else if (value instanceof String) {
        return LocalDateTime.parse((String) value);
      }
    }
    throw new RuntimeException("Cannot convert timestamp: " + value);
  }
}
