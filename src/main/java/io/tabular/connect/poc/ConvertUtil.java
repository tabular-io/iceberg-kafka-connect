// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Streams;
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
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.kafka.connect.data.Struct;

public class ConvertUtil {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ZoneId DEFAULT_TZ = ZoneId.systemDefault();
  private static final long NANOS_PER_MILLI = 1_000_000L;

  @SneakyThrows
  public static GenericRecord convert(Object value, StructType schema) {
    if (value instanceof byte[]) {
      JsonNode jsonNode = MAPPER.readTree((byte[]) value);
      return JsonConverter.convertStructValue(jsonNode, schema);
    } else if (value instanceof String) {
      JsonNode jsonNode = MAPPER.readTree((String) value);
      return JsonConverter.convertStructValue(jsonNode, schema);
    } else if (value instanceof Struct) {
      return StructConverter.convertRecordValue((Struct) value, schema);
    }
    throw new UnsupportedOperationException(
        "Cannot convert values of type: " + value.getClass().getName());
  }

  private static class JsonConverter {
    private static Object convertValue(JsonNode value, Type type) {
      Object result;
      switch (type.typeId()) {
        case STRUCT:
          result = convertStructValue(value, type.asStructType());
          break;
        case LIST:
          result = convertListValue(value, type.asListType());
          break;
        case MAP:
          result = convertMapValue(value, type.asMapType());
          break;
        case INTEGER:
          result = value.asInt();
          break;
        case LONG:
          result = value.asLong();
          break;
        case FLOAT:
          result = (float) value.asDouble();
          break;
        case DOUBLE:
          result = value.asDouble();
          break;
        case DECIMAL:
          result = convertDecimalValue(value);
          break;
        case BOOLEAN:
          result = value.asBoolean();
          break;
        case STRING:
          // if the value is a JSON object then convert it to a string
          // using toString() instead of asText()
          result = value.toString();
          break;
        case UUID:
          result = UUID.fromString(value.asText());
          break;
        case BINARY:
        case FIXED:
          result = convertBase64Value(value.asText());
          break;
        case DATE:
          result = convertDateValue(value);
          break;
        case TIME:
          result = convertTimeValue(value);
          break;
        case TIMESTAMP:
          result = convertTimestampValue(value, (TimestampType) type);
          break;
        default:
          throw new RuntimeException("Unsupported type: " + type.typeId());
      }
      return result;
    }

    private static GenericRecord convertStructValue(JsonNode value, StructType schema) {
      GenericRecord record = GenericRecord.create(schema);
      Streams.stream(value.fields())
          .forEach(
              child -> {
                NestedField field = schema.field(child.getKey());
                if (field != null) {
                  record.setField(field.name(), convertValue(child.getValue(), field.type()));
                } else {
                  // TODO: warn or throw exception if missing?
                }
              });
      return record;
    }

    private static List<Object> convertListValue(JsonNode value, ListType type) {
      return Streams.stream(value.elements())
          .map(element -> convertValue(element, type.elementType()))
          .collect(toList());
    }

    private static Map<String, Object> convertMapValue(JsonNode value, MapType type) {
      // NOTE: only String key types are supported
      Map<String, Object> result = new HashMap<>();
      Streams.stream(value.fields())
          .forEach(kv -> result.put(kv.getKey(), convertValue(kv.getValue(), type.valueType())));
      return result;
    }

    private static BigDecimal convertDecimalValue(JsonNode value) {
      if (value.isIntegralNumber()) {
        return BigDecimal.valueOf(value.asLong());
      } else if (value.isFloatingPointNumber()) {
        return BigDecimal.valueOf(value.asDouble());
      } else if (value.isTextual()) {
        return new BigDecimal(value.asText());
      }
      throw new RuntimeException("Cannot convert big decimal: " + value);
    }

    private static ByteBuffer convertBase64Value(String string) {
      return ByteBuffer.wrap(Base64.getDecoder().decode(string));
    }

    private static LocalDate convertDateValue(JsonNode value) {
      if (value.isIntegralNumber()) {
        return LocalDate.ofEpochDay(value.asInt());
      } else if (value.isTextual()) {
        return LocalDate.parse(value.asText());
      }
      throw new RuntimeException("Cannot convert date: " + value);
    }

    private static LocalTime convertTimeValue(JsonNode value) {
      if (value.isIntegralNumber()) {
        return LocalTime.ofNanoOfDay(value.asLong() * NANOS_PER_MILLI);
      } else if (value.isTextual()) {
        return LocalTime.parse(value.asText());
      }
      throw new RuntimeException("Cannot convert time: " + value);
    }

    private static Temporal convertTimestampValue(JsonNode value, TimestampType type) {
      // TODO: support non-ISO format, e.g. ' ' instead of 'T'
      if (type.shouldAdjustToUTC()) {
        if (value.isIntegralNumber()) {
          return OffsetDateTime.ofInstant(Instant.ofEpochMilli(value.asLong()), ZoneOffset.UTC);
        } else if (value.isTextual()) {
          return OffsetDateTime.parse(value.asText());
        }
      } else {
        if (value.isIntegralNumber()) {
          return LocalDateTime.ofInstant(Instant.ofEpochMilli(value.asLong()), DEFAULT_TZ);
        } else if (value.isTextual()) {
          return LocalDateTime.parse(value.asText());
        }
      }
      throw new RuntimeException("Cannot convert timestamp: " + value);
    }
  }

  private static class StructConverter {

    private static GenericRecord convertRecordValue(Struct struct, StructType schema) {
      // TODO
      throw new UnsupportedOperationException("TODO");
    }
  }
}
