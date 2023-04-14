// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc.convert;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.common.DynMethods.BoundMethod;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

public class RecordConverter {

  private static final ZoneId DEFAULT_TZ = ZoneId.systemDefault();
  private static final long NANOS_PER_MILLI = 1_000_000L;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final RecordConverter INSTANCE = new RecordConverter();

  private final BoundMethod convertToJson;

  private RecordConverter() {
    JsonConverter jsonConverter = new JsonConverter();
    convertToJson =
        DynMethods.builder("convertToJsonWithoutEnvelope")
            .hiddenImpl(JsonConverter.class, Schema.class, Object.class)
            .build(jsonConverter);
  }

  @SneakyThrows
  public static Record convert(Object data, StructType tableSchema) {
    if (data instanceof org.apache.kafka.common.protocol.types.Struct || data instanceof Map) {
      return INSTANCE.convertStructValue(data, tableSchema);
    } else if (data instanceof String) {
      Map<?, ?> map = MAPPER.readValue((String) data, Map.class);
      return INSTANCE.convertStructValue(map, tableSchema);
    } else if (data instanceof byte[]) {
      Map<?, ?> map = MAPPER.readValue((byte[]) data, Map.class);
      return INSTANCE.convertStructValue(map, tableSchema);
    }
    throw new IllegalArgumentException("Cannot convert type: " + data.getClass().getName());
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
              Object fieldVal = getValueFromStruct(value, field.name());
              if (fieldVal != null) {
                record.setField(field.name(), convertValue(fieldVal, field.type()));
              }
            });
    return record;
  }

  protected Object getValueFromStruct(Object value, String fieldName) {
    if (value instanceof Map) {
      return ((Map<?, ?>) value).get(fieldName);
    } else if (value instanceof Struct) {
      return ((Struct) value).get(fieldName);
    }
    throw new IllegalArgumentException("Cannot convert to struct: " + value.getClass().getName());
  }

  protected List<Object> convertListValue(Object value, ListType type) {
    Preconditions.checkArgument(value instanceof Map);
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

  @SneakyThrows
  protected String convertString(Object value) {
    if (value instanceof String) {
      return (String) value;
    } else if (value instanceof Number || value instanceof Boolean) {
      return value.toString();
    } else if (value instanceof Map || value instanceof List) {
      return MAPPER.writeValueAsString(value);
    } else if (value instanceof Struct) {
      Struct struct = (Struct) value;
      return convertToJson.invoke(struct.schema(), struct);
    }
    throw new IllegalArgumentException("Cannot convert to string: " + value.getClass().getName());
  }

  protected UUID convertUUID(Object value) {
    if (value instanceof String) {
      return UUID.fromString((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to UUID: " + value.getClass().getName());
  }

  protected ByteBuffer convertBase64Binary(Object value) {
    if (value instanceof String) {
      return ByteBuffer.wrap(Base64.getDecoder().decode((String) value));
    }
    throw new IllegalArgumentException("Cannot convert to binary: " + value.getClass().getName());
  }

  protected LocalDate convertDateValue(Object value) {
    if (value instanceof Number) {
      long i = ((Number) value).intValue();
      return LocalDate.ofEpochDay(i);
    } else if (value instanceof String) {
      return LocalDate.parse((String) value);
    }
    throw new RuntimeException("Cannot convert date: " + value);
  }

  protected LocalTime convertTimeValue(Object value) {
    if (value instanceof Number) {
      long l = ((Number) value).longValue();
      return LocalTime.ofNanoOfDay(l * NANOS_PER_MILLI);
    } else if (value instanceof String) {
      return LocalTime.parse((String) value);
    }
    throw new RuntimeException("Cannot convert time: " + value);
  }

  protected Temporal convertTimestampValue(Object value, TimestampType type) {
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
